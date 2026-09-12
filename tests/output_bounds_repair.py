"""Boundary repair regressions; exact owned fixtures, no cleanup discovery.

The original matrix's barrier/pidfd/reaping fixture owns real brokers. Synthetic
peers own only their direct frontend. --baseline records the reviewed defects;
it is used only against the preserved original candidate, not in the Rust gate.
"""
import copy
import ctypes
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

import output_bounds as bounds
import receipts as wire


def record(**fields):
    print(json.dumps(fields), flush=True)


def fact():
    stream = {'retained': 0, 'reserved': 0, 'discarded': 0, 'storage_fault': None,
              'collection': {'state': 'eof'}, 'prefix_complete': True}
    return {'limit': 64, 'first_stop': None, 'stdout': stream,
            'stderr': copy.deepcopy(stream), 'io_inflight': 0, 'sealed': True,
            'tail_read': 0, 'leader_exit': {'code': 0},
            'original_group_absent': True, 'group_control': {'state': 'settled'}}


def encoded(kind, value):
    payload = json.dumps(value).encode()
    return bytes([kind]) + struct.pack('>I', len(payload)) + payload


def decode_frames(data):
    frames = []
    while data:
        assert len(data) >= 5, data
        kind, size = data[0], struct.unpack('>I', data[1:5])[0]
        assert len(data) >= 5 + size, data
        frames.append((kind, data[5:5+size]))
        data = data[5+size:]
    return frames


def frontend(baseline=False):
    cases = ['valid-empty', 'valid-zero-replay', 'valid-resume-replay',
             'valid-resume-at-end', 'progress-ahead',
             'missing-stdout', 'missing-stderr', 'missing-resumed-stdout',
             'missing-resumed-stderr', 'end-behind-delivery',
             'end-limit-lower', 'end-limit-higher',
             'progress-limit-lower', 'progress-limit-higher']
    cases += ['hello-' + key + suffix for key in ('exit', 'stdout_eof', 'stderr_eof')
              for suffix in ('', '-null')]
    for case in cases:
        bad_hello = case.startswith('hello-')
        bad_progress = case.startswith('progress-limit-')
        invalid = bad_hello or bad_progress or case.startswith(('missing-', 'end-'))
        offsets = (5, 7) if 'resume' in case else (0, 0)
        initial = fact()
        initial['stdout']['retained'], initial['stderr']['retained'] = offsets
        final = copy.deepcopy(initial)
        payloads = [b'', b'']
        if case in ('valid-zero-replay', 'valid-resume-replay', 'progress-ahead'):
            payloads = [b'abc', b'XY']
        if case == 'end-behind-delivery':
            payloads = [b'x', b'']
        else:
            for name, payload in zip(('stdout', 'stderr'), payloads):
                final[name]['retained'] += len(payload)
        if case.startswith('missing-'):
            final['stderr' if 'stderr' in case else 'stdout']['retained'] += 1
        if '-limit-' in case:
            final['limit'] += -1 if case.endswith('lower') else 1
        hello = {'attachment': bounds.ATTACH,
                 'offsets': dict(stdin=0, stdout=offsets[0], stderr=offsets[1]),
                 'output': initial,
                 'session': {'capabilities': ['group-pidfd-cancel-v1', bounds.CAP],
                             'workload_started': True, 'cancel_state': 'settled'}}
        if bad_hello:
            key = case.removeprefix('hello-').removesuffix('-null')
            hello[key] = None if case.endswith('-null') else ({'code': 0} if key == 'exit' else 0)
        with tempfile.TemporaryDirectory(prefix='pk-repair-wire-') as root:
            path = Path(root) / hashlib.sha256(b'owned').hexdigest()[:32]
            path.mkdir()
            with socket.socket(socket.AF_UNIX) as listener:
                listener.bind(str(path/'broker.sock'))
                listener.listen()
                listener.settimeout(5)
                p = subprocess.Popen([bounds.BINARY, '--id', 'owned', '--output-bounded', '--', '/bin/false'],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     env=dict(os.environ, PIPEKEEP_RUNTIME_DIR=root), bufsize=0)
                handle = os.pidfd_open(p.pid)
                signals = []
                try:
                    wire.write_bytes(p.stdin.fileno(), json.dumps({'offsets': dict(zip(('stdout', 'stderr'), offsets))}).encode()+b'\n')
                    conn, _ = listener.accept()
                    with conn:
                        assert wire.read_json(conn.fileno())['action'] == 'attach-output-v1'
                        if bad_hello:
                            # Queued input must stay behind affirmative hello validation.
                            wire.write_bytes(p.stdin.fileno(), wire.pack(1, 0, b'probe'))
                        conn.sendall(json.dumps(hello).encode()+b'\n')
                        forwarded_input = b''
                        if not bad_hello or baseline:
                            opening = wire.read_json(p.stdout.fileno())
                            assert opening['output']['limit'] == initial['limit']
                            if bad_hello:
                                forwarded_input = wire.read_exact(conn.fileno(), 18)
                                assert forwarded_input == wire.pack(1, 0, b'probe')
                            if case == 'progress-ahead' or bad_progress:
                                conn.sendall(encoded(7, final))
                            if bad_progress:
                                # No terminal follows: a changed progress policy must
                                # itself be rejected, and must not be forwarded.
                                if baseline:
                                    conn.shutdown(socket.SHUT_WR)
                            else:
                                for i, data in enumerate(payloads):
                                    if data:
                                        conn.sendall(wire.pack(3+i, offsets[i], data))
                                conn.sendall(encoded(8, final))
                        else:
                            opening = None
                        code = p.wait(timeout=5)
                        output = p.stdout.read()
                        error = p.stderr.read().decode('utf-8', 'replace')
                        if opening is None:
                            # Pre-handshake diagnostics may be a JSON error; they
                            # must not contain the authoritative attachment hello.
                            assert b'framed-output-v1' not in output, output
                            frames = []
                        else:
                            frames = decode_frames(output)
                        conn.settimeout(1)
                        try:
                            remaining_input = conn.recv(4096)
                        except ConnectionResetError:
                            remaining_input = b''
                        assert remaining_input == b'', remaining_input
                        expected_code = 1 if bad_progress or (invalid and not baseline) else 0
                        assert code == expected_code, (case, code, frames, error)
                        kinds = [k for k, _ in frames]
                        if invalid and not baseline:
                            assert 8 not in kinds and 7 not in kinds, (case, kinds)
                            assert not forwarded_input
                        elif not bad_progress:
                            assert kinds[-1] == 8, (case, kinds)
                        if case == 'progress-ahead':
                            assert kinds == [7, 3, 4, 8], kinds
                        if bad_progress and baseline:
                            assert kinds == [7], kinds
                        record(case=case, baseline=baseline, resume_offsets=offsets,
                               opening_limit=initial['limit'], subsequent_limit=final['limit'],
                               claimed_end=[final[n]['retained'] for n in ('stdout', 'stderr')],
                               delivered_end=[n+len(d) for n, d in zip(offsets, payloads)],
                               forwarded_hello=opening is not None, forwarded_frames=kinds,
                               forwarded_stdin_bytes=len(forwarded_input), actual_frontend_exit=code,
                               launches=0, stderr=error)
                finally:
                    if p.poll() is None:
                        try:
                            signal.pidfd_send_signal(handle, signal.SIGKILL)
                            signals.append('exact_frontend_SIGKILL')
                        except ProcessLookupError:
                            signals.append('exact_frontend_ESRCH')
                    code = p.wait(timeout=5)
                    os.close(handle)
                    for stream in (p.stdin, p.stdout, p.stderr):
                        stream.close()
                    record(case=case, exact_frontend_pid=p.pid, actual_wait_returncode=code, signals=signals)
    record(frontend_boundary_cases=len(cases), baseline=baseline, result='passed')


def direct(s, acquisition=False):
    s.path.mkdir(exist_ok=True)
    if acquisition:
        # Lazy import: group_pidfd has a fixture entry point at import time.
        import group_pidfd
        assert os.uname().machine in ('x86_64', 'aarch64'), 'required seccomp ABI unavailable'
        fault = lambda: group_pidfd.seccomp('acquire')
    else:
        fault = None
    b = subprocess.Popen([bounds.BINARY, '__broker', '--id', 'owned', '--session-dir', str(s.path),
                          '--group-pidfd', '--output-limit', str(s.limit), '--', sys.executable,
                          bounds.SCRIPT, '--fixture', str(s.root), s.mode, str(s.count)],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=s.env, start_new_session=True, preexec_fn=fault)
    s.proxies.append(b)
    s.pin()
    assert s.broker == b.pid
    return b


def acquisition(baseline=False):
    cases = ('within-budget',) if baseline else ('within-budget', 'overflow', 'storage-fault')
    for case in cases:
        with bounds.Session(2 if case == 'overflow' else 1024, count=50, grace=1) as s:
            s.path.mkdir()
            if case == 'storage-fault':
                (s.path/'stdout.buffer').symlink_to('/dev/full')
            direct(s, acquisition=True)
            before = s.query()
            assert before['session']['capabilities'] == [], before
            assert before['output']['group_control'] == {'state': 'unconfirmed', 'cause': 'unavailable'}
            # Let the old collector reach its first iteration before releasing
            # the workload. No operation or policy stop has occurred.
            time.sleep(.08)
            before = s.query()['output']
            assert before['first_stop'] is None
            assert all(before[n]['collection']['state'] == ('unconfirmed' if baseline else 'reading')
                       for n in ('stdout', 'stderr')), before
            q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}})
            out, err = q.communicate(timeout=5)
            assert q.returncode == 1 and b'finite output control unverified' in out, (out, err)
            s.go()
            final = bounds.until(lambda: (f if (f := s.query()['output'])['leader_exit'] is not None
                                         and f['sealed'] else None), 'acquisition fixture actual result and collection')
            s.actual_leader_result = final['leader_exit']
            assert final['group_control'] == {'state': 'unconfirmed', 'cause': 'unavailable'}
            assert not final['original_group_absent']
            assert s.query()['session']['capabilities'] == []
            bounds.accounting(final)
            if case == 'within-budget':
                data = [(s.path/(n+'.buffer')).read_bytes() for n in ('stdout', 'stderr')]
                if baseline:
                    assert data == [b'', b''] and final['leader_exit'] == {'code': 120}, final
                else:
                    assert data == [b'A'*50, b'B'*50], data
                    assert final['leader_exit'] == {'code': 23}, final
                    assert all(final[n]['collection']['state'] == 'eof' and final[n]['prefix_complete']
                               for n in ('stdout', 'stderr')), final
                assert final['first_stop'] is None
                record(case='acquire-'+case, baseline=baseline, before=before, fact=final,
                       spool_bytes=list(map(len, data)), spool_sha256=[hashlib.sha256(d).hexdigest() for d in data])
            else:
                assert final['first_stop'] == ('output_limit' if case == 'overflow' else 'storage_fault'), final
                assert any(final[n]['collection'] == {'state': 'unconfirmed', 'cause': 'group_unconfirmed'}
                           for n in ('stdout', 'stderr')), final
                assert sum(final[n]['retained'] for n in ('stdout', 'stderr')) <= s.limit
                if case == 'storage-fault':
                    assert final['stdout']['storage_fault'] == 'write' and final['stdout']['retained'] == 0
                record(case='acquire-'+case, baseline=False, before=before, fact=final)
            assert (s.root/'launches').read_bytes() == b'launch\n'
    record(acquisition_cases=len(cases), baseline=baseline, result='passed')


def manual_deadline():
    # The existing escaped-idle fixture writes count+1 bytes, all within budget.
    with bounds.Session(128, mode='escape-idle', count=15, grace=1, settle=2) as s:
        p, _ = s.ready()
        s.go()
        (s.root/'children-go').touch()
        bounds.until(lambda: s.query()['leader_exit'] == {'code': 23}, 'natural leader wait')
        start = time.monotonic()
        with socket.socket(socket.AF_UNIX) as cancel:
            cancel.connect(str(s.path/'broker.sock'))
            cancel.sendall(b'{"action":"cancel-output-v1"}\n')
            _, final, _ = s.collect(p)
            result = wire.read_json(cancel.fileno())
        elapsed = time.monotonic()-start
        assert 2.8 <= elapsed < 3.6, elapsed
        assert 'error' in result and final['first_stop'] is None, result
        assert final['leader_exit'] == {'code': 23} and final['original_group_absent'], final
        assert all(final[n]['collection'] == {'state': 'unconfirmed', 'cause': 'operation_deadline'}
                   for n in ('stdout', 'stderr')), final
        assert not select.select([s.handles[s.descendants[0]]], [], [], 0)[0]
        assert result['output']['original_group_absent']
        record(case='manual-no-policy-stop', elapsed_seconds=elapsed, fact=final,
               cancellation=result, escaped_survivor_after_collection=True)


def blocked_storage():
    with bounds.Session(4*1024*1024, count=1024*1024, grace=1, settle=2) as s:
        s.env['PIPEKEEP_SESSION_TTL_SECS'] = '1'
        s.path.mkdir()
        fifo = s.path/'stdout.buffer'
        os.mkfifo(fifo)
        reader = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
        try:
            direct(s)
            s.go()
            blocked = bounds.until(lambda: (f if (f := s.query()['output'])['stdout']['retained'] > 0
                                            and f['stdout']['reserved'] == 4096 and f['io_inflight'] == 1
                                            else None), 'blocked FIFO write')
            time.sleep(.1)
            assert s.query()['output']['stdout']['retained'] == blocked['stdout']['retained']
            assert blocked['first_stop'] is None
            start = time.monotonic()
            with socket.socket(socket.AF_UNIX) as cancel:
                cancel.connect(str(s.path/'broker.sock'))
                cancel.sendall(b'{"action":"cancel-output-v1"}\n')
                absent = bounds.until(lambda: (f if (f := s.query()['output'])['original_group_absent'] else None),
                                      'group settles independently of blocked storage', 2.5)
                absence_elapsed = time.monotonic()-start
                reply = wire.read_json(cancel.fileno())
            deadline_elapsed = time.monotonic()-start
            assert 'error' in reply and 2.8 <= deadline_elapsed < 3.6, reply
            final = reply['output']
            s.actual_leader_result = final['leader_exit']
            assert final['leader_exit'] == {'signal': 9} and final['original_group_absent'], final
            assert final['stdout']['reserved'] == 4096 and final['io_inflight'] == 1 and not final['sealed']
            # The cancel reply and collector deadline wakeup run in separate
            # tasks. Observe closure after the reply without moving its deadline
            # or releasing the blocked write/reservation to make it happen.
            closed = bounds.until(lambda: (f if (f := s.query()['output'])['stdout']['collection']
                                           == {'state': 'unconfirmed', 'cause': 'storage_pending'} else None),
                                  'collector closure after cancellation deadline', 1)
            closure_elapsed = time.monotonic()-start
            assert closed['leader_exit'] == {'signal': 9} and closed['original_group_absent'], closed
            assert closed['stdout']['reserved'] == 4096 and closed['io_inflight'] == 1 and not closed['sealed']
            time.sleep(1.2)
            assert s.query()['output']['io_inflight'] == 1
            assert not select.select([s.handles[s.broker]], [], [], 0)[0]
            data = bytearray()
            end = time.monotonic()+4
            while True:
                try:
                    chunk = os.read(reader, 65536)
                except BlockingIOError:
                    chunk = b''
                data.extend(chunk)
                resolved = s.query()['output']
                if resolved['io_inflight'] == 0 and len(data) == resolved['stdout']['retained']:
                    break
                assert time.monotonic() < end, 'blocked write resolution'
                time.sleep(.005)
            assert data == b'A'*len(data)
            assert resolved['stdout']['reserved'] == 0 and resolved['sealed']
            assert resolved['stdout']['collection'] == closed['stdout']['collection']
            assert resolved['original_group_absent']
            offsets = tuple(resolved[n]['retained'] for n in ('stdout', 'stderr'))
            q = s.spawn({'offsets': dict(zip(('stdout', 'stderr'), offsets))})
            wire.read_json(q.stdout.fileno())
            replay, terminal, _ = s.collect(q, offsets)
            assert replay == [b'', b'']
            record(case='blocked-storage', blocked=blocked, absence_elapsed=absence_elapsed,
                   deadline_elapsed=deadline_elapsed, deadline_fact=final,
                   closure_elapsed=closure_elapsed, closed=closed, resolved=resolved,
                   drained_bytes=len(data), drained_sha256=hashlib.sha256(data).hexdigest(),
                   terminal=terminal, pinned_past_ttl=True)
        finally:
            os.close(reader)


def blocked_replay():
    with bounds.Session(128, count=16, grace=1) as s:
        s.env['PIPEKEEP_SESSION_TTL_SECS'] = '1'
        p, _ = s.ready()
        s.go()
        s.collect(p)
        fifo = s.path/'stdout.buffer'
        fifo.unlink()
        os.mkfifo(fifo)
        previous = None
        for _ in range(3):
            q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
            wire.read_json(q.stdout.fileno())
            bounds.until(lambda: s.query()['output']['io_inflight'] == 1, 'blocked replay open')
            if previous:
                assert previous.wait(timeout=5) == 1
                assert all(k != 8 for k, _ in decode_frames(previous.stdout.read()))
            previous = q
        time.sleep(1.2)
        assert s.query()['output']['io_inflight'] == 1
        assert not select.select([s.handles[s.broker]], [], [], 0)[0]
        # Release only this exact private FIFO open; no helper process or signal.
        writer = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
        try:
            # The superseded worker returns first. Keep the writer present so
            # the current attachment's serialized open can also return.
            assert q.wait(timeout=5) == 1
            frames = decode_frames(q.stdout.read())
            assert all(k != 8 for k, _ in frames), frames
            final = bounds.until(lambda: (f if (f := s.query()['output'])['io_inflight'] == 0
                                          and f['original_group_absent'] else None), 'replay released and group latch')
        finally:
            os.close(writer)
        assert final['stdout']['retained'] == 16 and final['leader_exit'] == {'code': 23}
        assert final['first_stop'] == 'storage_fault' and final['stdout']['storage_fault'] == 'replay_read'
        record(case='blocked-replay', replacement_attempts=3, peak_io_inflight=1,
               pinned_past_ttl=True, fact=final, final_frames=[k for k, _ in frames])


def main():
    bounds.BINARY = str(Path(sys.argv[1]).resolve())
    assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0
    baseline = '--baseline' in sys.argv[2:]
    record(binary=bounds.BINARY, binary_sha256=hashlib.sha256(Path(bounds.BINARY).read_bytes()).hexdigest(),
           baseline=baseline, kernel=os.uname().release)
    frontend(baseline)
    acquisition(baseline)
    if not baseline:
        manual_deadline()
        blocked_storage()
        blocked_replay()
    try:
        os.waitpid(-1, os.WNOHANG)
    except ChildProcessError:
        pass
    else:
        raise AssertionError('owned fixture remains; no cleanup guessing or duplicate run')
    record(result='BASELINE DEFECTS REPRODUCED' if baseline else 'OUTPUT BOUNDS REPAIR PASSED', cleanup='ECHILD')


if __name__ == '__main__':
    main()

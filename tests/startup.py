"""Public proxy compatibility/startup regressions, with exact owned cleanup.

Called by group_pidfd.py inside its disposable subreaper. Fixtures stay at an
ownership barrier until their broker and workload have individual pidfds.
"""
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time


def until(predicate, message, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if result := predicate():
            return result
        time.sleep(.01)
    raise AssertionError(message)


def fixture(root):
    signal.alarm(20)
    root = Path(root)
    # Atomic readiness, with both identities supplied by the owned workload.
    (root / 'ready.tmp').write_text(json.dumps([os.getppid(), os.getpid()]))
    (root / 'ready.tmp').rename(root / 'ready')
    until(lambda: (root / 'finish').exists(), 'workload finish barrier', 15)
    print('ordinary output', flush=True)
    sys.exit(5)


class PublicSession:
    def __init__(self, binary, opted=False, fault=None, ttl=1):
        self.temp = tempfile.TemporaryDirectory(prefix='pk-startup-')
        self.root = Path(self.temp.name)
        self.path = self.root / hashlib.sha256(b'owned').hexdigest()[:32]
        self.env = dict(os.environ, PIPEKEEP_RUNTIME_DIR=str(self.root),
                        PIPEKEEP_SESSION_TTL_SECS=str(ttl))
        self.binary = binary
        self.command = [binary, '--id', 'owned'] + (['--group-pidfd'] if opted else [])
        self.command += ['--', sys.executable, str(Path(__file__).resolve()),
                         '--fixture', str(self.root)]
        self.proxy = subprocess.Popen(self.command, stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      env=self.env, preexec_fn=fault)
        self.handles = [os.pidfd_open(self.proxy.pid)]
        # This public proxy deliberately is not its own group leader.
        assert os.getpgid(self.proxy.pid) != self.proxy.pid
        self.proxy.stdin.write(b'{"stdin_eof":0}\n')
        self.proxy.stdin.flush()

    def pin_workload(self):
        until(lambda: (self.root / 'ready').exists(), 'public workload ready')
        broker, child = json.loads((self.root / 'ready').read_text())
        self.handles += [os.pidfd_open(broker), os.pidfd_open(child)]

    def finish(self):
        (self.root / 'finish').touch()

    def query(self, action='session'):
        with socket.socket(socket.AF_UNIX) as sock:
            sock.settimeout(4)
            sock.connect(str(self.path / 'broker.sock'))
            sock.sendall(json.dumps({'action': action}).encode() + b'\n')
            with sock.makefile('rb') as stream:
                return json.loads(stream.readline())

    def expired(self):
        until(lambda: not self.path.exists(), 'public session TTL cleanup', 4)
        until(lambda: select.select([self.handles[1]], [], [], 0)[0],
              'broker exit after TTL', 3)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        for fd in reversed(self.handles):
            try:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.close(fd)
        self.proxy.wait(timeout=5)
        for stream in (self.proxy.stdin, self.proxy.stdout, self.proxy.stderr):
            stream.close()
        # Only this harness's adopted children; never enumerate host processes.
        end = time.monotonic() + 5
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            assert time.monotonic() < end, 'public fixture survived exact cleanup'
            if not pid:
                time.sleep(.01)
        self.temp.cleanup()


def compatibility(binary):
    binaries = [binary]
    if legacy := os.environ.get('PIPEKEEP_LEGACY_BINARY'):
        binaries.append(legacy)
    for current in binaries:
        for ignore, expected in ((True, 1), (False, 5)):
            fault = (lambda: signal.signal(signal.SIGCHLD, signal.SIG_IGN)) if ignore else None
            with PublicSession(current, fault=fault) as s:
                s.pin_workload()
                s.finish()
                out, err = s.proxy.communicate(timeout=4)
                assert s.proxy.returncode == expected, (out, err)
                header, output = out.split(b'\n', 1)
                assert 'session' not in json.loads(header), header
                assert output == b'ordinary output\n' and not err, (out, err)
                # Terminal replay must expose the shipped fallback/actual exit.
                replay = subprocess.run([current, '--id', 'owned', '--', '/bin/false'],
                                        input=b'{"offsets":{"stdout":0,"stderr":0},"stdin_eof":0}\n',
                                        capture_output=True, env=s.env, timeout=3)
                assert replay.returncode == expected, (replay.stdout, replay.stderr)
                retained = json.loads(replay.stdout.splitlines()[0])
                assert retained['exit'] == {'code': expected}, retained
                s.expired()
                print(json.dumps({'ordinary_binary': current, 'ignored_sigchld': ignore,
                                  'proxy_status': expected, 'retained': retained,
                                  'TTL_directory_removed': True}), flush=True)
    print('PASS public ordinary failed-wait fallback/attachment completion/TTL/actual nonzero', flush=True)

    # No group-pidfd kernel support is needed to reject the inherited policy.
    with PublicSession(binary, opted=True,
                       fault=lambda: signal.signal(signal.SIGCHLD, signal.SIG_IGN)) as s:
        out, err = s.proxy.communicate(timeout=3)
        assert s.proxy.returncode == 1, (out, err)
        reason = json.loads(out)['error']
        assert 'requires default SIGCHLD' in reason and 'workload not started' in reason, reason
        assert not list(s.root.iterdir()), 'rejection allocated a session or launched a workload'
        for _ in range(3):
            retry = subprocess.run(s.command, input=b'{"stdin_eof":0}\n', capture_output=True,
                                   env=s.env, timeout=3,
                                   preexec_fn=lambda: signal.signal(signal.SIGCHLD, signal.SIG_IGN))
            assert retry.returncode == 1 and json.loads(retry.stdout)['error'] == reason
            assert not list(s.root.iterdir()), 'retry left stale session state'
        print(json.dumps({'public_sigchld_rejection': reason, 'attempts': 4,
                          'session_allocated': False, 'workload_launched': False}), flush=True)
    print('PASS public opted-in SIGCHLD-ignore rejects before allocation/dispatch/retry', flush=True)


def ambiguous_startup(binary, seccomp):
    # Deny only proxy connect and nonblocking wait4. The broker's blocking
    # exact-child wait remains functional. Error strings and errno never confer
    # permission to kill/remove/relaunch a possibly dispatched workload.
    for fault in ('startup-wait-echild', 'startup-wait-eio', 'startup-timeout'):
        with PublicSession(binary, opted=True, fault=lambda: seccomp(fault)) as s:
            s.pin_workload()
            out, err = s.proxy.communicate(timeout=7)
            assert s.proxy.returncode == 1, (out, err)
            reason = json.loads(out)['error']
            assert 'session retained' in reason and 'workload may have started' in reason, reason
            assert 'do not retry creation' in reason and 'workload not started' not in reason, reason
            if fault != 'startup-timeout':
                assert 'wait failed after dispatch' in reason, reason
            assert s.path.is_dir()
            assert not select.select(s.handles[1:], [], [], 0)[0], 'owner or workload killed'
            before = s.query()
            assert before['session']['capabilities'] == ['group-pidfd-cancel-v1'], before
            assert before['leader_exit'] is None and before['failure'] is None, before
            retry = subprocess.run(s.command, input=b'{"stdin_eof":0}\n', capture_output=True,
                                   env=s.env, timeout=3)
            assert retry.returncode == 1 and b'already exists' in retry.stdout, retry.stdout
            assert s.query()['pid'] == before['pid'], 'creation replaced the original workload'
            s.finish()
            until(lambda: s.query()['leader_exit'] == {'code': 5}, 'retained actual wait')
            result = s.query('cancel-group-pidfd')
            assert result['exit'] == {'code': 5} and result['outcome'] == 'already_exited', result
            assert result['session']['cancel_state'] == 'settled', result
            s.expired()
            print(json.dumps({'public_startup_fault': fault, 'error': reason,
                              'retained_actual_result': result}), flush=True)
    print('PASS public ambiguous wait/readiness errors retain authority/no cleanup/no relaunch', flush=True)


if __name__ == '__main__':
    assert sys.argv[1] == '--fixture'
    fixture(sys.argv[2])

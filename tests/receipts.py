"""Real public CLI protocol tests. Every workload waits at a pidfd barrier.

No process-name/path ownership, broad signals, PID churn or output budget claim.
The disposable harness alone is a subreaper. Data fixtures cap input at 8 MiB,
output at twice that, and have an independent 45 second failure watchdog.
Set PIPEKEEP_RECEIPTS_OLD_BINARY to the accepted 2f1329f8 private build to run
actual old peers (required when PIPEKEEP_REQUIRE_RECEIPTS_OLD=1).
"""
import ctypes
import hashlib
import fcntl
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

WINDOW = 64 * 1024
CHUNK = 4096
LIMIT = 8 * 1024 * 1024
XOR = bytes(i ^ 0xa5 for i in range(256))
SCRIPT = str(Path(__file__).resolve())


def until(test, message, timeout=8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = test()
        if result:
            return result
        time.sleep(.005)
    raise AssertionError(message)


def pattern(offset, count):
    result = bytearray()
    while count:
        block, start = divmod(offset, CHUNK)
        take = min(count, CHUNK - start)
        result.extend(hashlib.shake_256(struct.pack('>Q', block)).digest(CHUNK)[start:start+take])
        offset += take
        count -= take
    return bytes(result)


def workload(root, mode):
    root = Path(root)
    signal.alarm(45)
    with (root / 'launches').open('ab') as f:
        f.write(b'launch\n')
    if mode == 'partial':
        assert fcntl.fcntl(0, fcntl.F_SETPIPE_SZ, 4096) == 4096
    (root / 'ready.tmp').write_text(json.dumps([os.getppid(), os.getpid()]))
    (root / 'ready.tmp').rename(root / 'ready')
    until(lambda: (root / 'owned').exists(), 'ownership barrier')
    if mode == 'partial':
        # Receipt must advance while application consumption is exactly zero.
        until(lambda: (root / 'read-go').exists(), 'partial write barrier', 20)
    if mode == 'signal':
        os.write(1, b'out\x00\xff')
        os.write(2, b'err\xff\x00')
        os.kill(os.getpid(), signal.SIGTERM)
    count = 0
    digest = hashlib.sha256()
    while True:
        data = os.read(0, CHUNK)
        if not data:
            break
        count += len(data)
        assert count <= LIMIT, 'fixture input/output bound'
        digest.update(data)
        os.write(1, data)
        os.write(2, data.translate(XOR))
    (root / 'result').write_text(json.dumps({'count': count, 'sha256': digest.hexdigest()}))
    # Hold actual exit after EOF for lost-ack recovery, when requested.
    if mode == 'eof':
        until(lambda: (root / 'exit-go').exists(), 'exit barrier', 20)
    sys.exit(23)


def pack(kind, offset=0, data=b''):
    payload = struct.pack('>Q', offset) + data
    return bytes([kind]) + struct.pack('>I', len(payload)) + payload


def read_exact(fd, count, timeout=8):
    out = bytearray()
    end = time.monotonic() + timeout
    while len(out) < count:
        assert select.select([fd], [], [], max(0, end-time.monotonic()))[0], 'read deadline'
        data = os.read(fd, count-len(out))
        if not data:
            raise EOFError(bytes(out))
        out.extend(data)
    return bytes(out)


def read_json(fd):
    data = bytearray()
    while not data.endswith(b'\n'):
        data.extend(read_exact(fd, 1))
        assert len(data) <= 65536
    return json.loads(data)


def read_frame(fd):
    header = read_exact(fd, 5)
    kind, size = header[0], struct.unpack('>I', header[1:])[0]
    assert size <= 16 * 1024 * 1024
    data = read_exact(fd, size)
    if kind == 6:
        return kind, json.loads(data), b''
    assert kind in (3, 4, 5) and len(data) >= 8
    return kind, struct.unpack('>Q', data[:8])[0], data[8:]


def write_bytes(fd, data):
    end = time.monotonic() + 8
    while data:
        assert select.select([], [fd], [], max(0, end-time.monotonic()))[1], 'write deadline'
        count = os.write(fd, data[:4096])
        data = data[count:]


class Session:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix='pk-receipts-')
        self.root = Path(self.temp.name)
        self.path = self.root / hashlib.sha256(b'owned').hexdigest()[:32]
        self.env = dict(os.environ, PIPEKEEP_RUNTIME_DIR=str(self.root),
                        PIPEKEEP_SESSION_TTL_SECS='30')
        self.children = []
        self.handles = []
        self.pinned = False

    def spawn(self, hello, framed=True, force=False, binary=None, nobuffer=False, mode='stream'):
        args = [binary or BINARY, '--id', 'owned']
        if framed: args += ['--framed']
        if force: args += ['--force']
        if nobuffer: args += ['--nobuffer']
        args += ['--', sys.executable, SCRIPT, '--workload', str(self.root), mode]
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=self.env, bufsize=0)
        self.children.append(p)
        # A rejected CLI may close stdin immediately; do not treat broken pipe
        # as launch success or failure, inspect its owned process and response.
        try: write_bytes(p.stdin.fileno(), json.dumps(hello).encode()+b'\n')
        except BrokenPipeError: pass
        return p

    def pin(self):
        until(lambda: (self.root/'ready').exists(), 'workload ready')
        broker, child = json.loads((self.root/'ready').read_text())
        self.handles += [os.pidfd_open(broker), os.pidfd_open(child)]
        self.pinned = True
        (self.root/'owned').touch()

    def query(self, request):
        with socket.socket(socket.AF_UNIX) as sock:
            sock.settimeout(8)
            sock.connect(str(self.path/'broker.sock'))
            sock.sendall(json.dumps(request).encode()+b'\n')
            return read_json(sock.fileno())

    def socket(self, request):
        sock = socket.socket(socket.AF_UNIX)
        sock.settimeout(8)
        sock.connect(str(self.path/'broker.sock'))
        sock.sendall(json.dumps(request).encode()+b'\n')
        hello = read_json(sock.fileno())
        return sock, hello

    def __enter__(self): return self

    def __exit__(self, *_):
        # Popen handles own frontends. Workload readiness barrier pins both
        # detached identities before any test releases or signals them.
        if not self.pinned and (self.root/'ready').exists(): self.pin()
        for fd in reversed(self.handles):
            try: signal.pidfd_send_signal(fd, signal.SIGKILL)
            except ProcessLookupError: pass
        for p in self.children:
            if p.poll() is None: p.kill()
            p.wait(timeout=8)
            for stream in (p.stdin, p.stdout, p.stderr):
                if stream and not stream.closed: stream.close()
        for fd in self.handles:
            assert select.select([fd], [], [], 8)[0], 'pidfd cleanup not observed'
            os.close(fd)
        # Only adopted children of THIS isolated harness, after exact kills.
        end = time.monotonic()+8
        reaped = 0
        while True:
            try: pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError: break
            assert time.monotonic() < end, 'owned fixture did not clean up'
            if pid: reaped += 1
            else: time.sleep(.005)
        print(json.dumps({'cleanup': 'ECHILD', 'frontends_waited': len(self.children),
                          'pidfds_observed': len(self.handles), 'adopted_reaped': reaped}), flush=True)
        self.temp.cleanup()


class Flow:
    """Only bytes [ack,sent) are retained; at most one extra encoded frame."""
    def __init__(self):
        self.ack = self.sent = 0
        self.pending = bytearray()
        self.out = self.err = 0
        self.receipts = 0
        self.receipts_before_eof = None
        self.max_pending = 0
        self.digest = hashlib.sha256()
        self.exit = None

    def attach(self, p):
        hello = read_json(p.stdout.fileno())
        assert hello['attachment'] == 'framed-v1' and not hello.get('nobuffer'), hello
        assert hello['offsets']['stdout'] == self.out and hello['offsets']['stderr'] == self.err
        self.receipt(hello['offsets']['stdin'])
        return hello

    def receipt(self, pos):
        assert self.ack <= pos <= self.sent, (self.ack, pos, self.sent)
        del self.pending[:pos-self.ack]
        self.ack = pos
        assert len(self.pending) == self.sent-self.ack <= WINDOW

    def consume(self, frame):
        kind, value, data = frame
        if kind == 5:
            self.receipt(value)
            self.receipts += 1
        elif kind in (3, 4):
            pos = self.out if kind == 3 else self.err
            assert value == pos
            expected = pattern(pos, len(data))
            if kind == 4: expected = expected.translate(XOR)
            assert data == expected, ('output bytes', kind, pos, len(data))
            if kind == 3: self.out += len(data)
            else: self.err += len(data)
        else:
            assert value == {'code': 23}, value
            self.exit = value

    def send_to(self, p, total, replay=False, tiny=False, slow=False):
        cursor = self.ack if replay else self.sent
        while cursor < total or self.ack < total:
            # Space is granted ONLY by actual broker receipts.
            if cursor < total and (cursor < self.sent or len(self.pending) < WINDOW):
                if cursor < self.sent:
                    data = bytes(self.pending[cursor-self.ack:cursor-self.ack+CHUNK])
                else:
                    count = min(97 if tiny else CHUNK, total-cursor, WINDOW-len(self.pending))
                    data = pattern(cursor, count)
                    self.pending.extend(data)
                    self.digest.update(data)
                    self.sent += count
                    self.max_pending = max(self.max_pending, len(self.pending))
                write_bytes(p.stdin.fileno(), pack(1, cursor, data))
                cursor += len(data)
            # Read after each frame, or deliberately only once the window is
            # full. All peer buffers and workload outputs remain finite.
            if self.ack < total and (len(self.pending) == WINDOW or cursor == total or
                                    (not slow and select.select([p.stdout], [], [], 0)[0])):
                self.consume(read_frame(p.stdout.fileno()))
            assert self.exit is None, 'exit before explicit EOF'

    def finish(self, p, total):
        assert self.ack == total and not self.pending and self.receipts >= 1
        self.receipts_before_eof = self.receipts
        write_bytes(p.stdin.fileno(), pack(2, total))
        while self.exit is None: self.consume(read_frame(p.stdout.fileno()))
        assert p.wait(timeout=8) == 23
        assert p.stderr.read() == b''
        assert self.out == total and self.err == total

    def verify(self, s, total):
        assert json.loads((s.root/'result').read_text()) == {'count': total, 'sha256': self.digest.hexdigest()}
        assert (s.root/'launches').read_bytes() == b'launch\n'
        print(json.dumps({'bytes': total, 'pending_limit': WINDOW,
                          'max_pending': self.max_pending, 'receipts_before_eof': self.receipts_before_eof,
                          'receipts_total': self.receipts,
                          'sha256': self.digest.hexdigest(), 'stdout': self.out, 'stderr': self.err,
                          'launches': 1, 'exit': self.exit}), flush=True)


def full_duplex():
    with Session() as s:
        p = s.spawn({})
        s.pin()
        flow = Flow(); flow.attach(p)
        total = 5 * 1024 * 1024 + 7919
        flow.send_to(p, total)
        assert flow.receipts > 1
        flow.finish(p, total); flow.verify(s, total)
    print('PASS >4MiB public full duplex / bounded pending window', flush=True)


def recovery():
    with Session() as s:
        p = s.spawn({}, mode='eof'); s.pin()
        f = Flow(); f.attach(p)
        f.send_to(p, 256*1024)
        # Leave unacknowledged bytes in the bounded pending window.
        data = pattern(f.sent, CHUNK)
        offset = f.sent; f.pending.extend(data); f.sent += len(data); f.digest.update(data)
        write_bytes(p.stdin.fileno(), pack(1, offset, data))
        # Force a live attachment replacement, then attempt old-generation EOF
        # and input. Old pipe writes may succeed locally; broker authority ends.
        q = s.spawn({'offsets': {'stdout': f.out, 'stderr': f.err}, 'stdin_start': f.ack}, force=True)
        f.attach(q)
        try: write_bytes(p.stdin.fileno(), pack(2, 0)+pack(1, 0, b'BAD'))
        except BrokenPipeError: pass
        p.wait(timeout=8)  # input stays open: displacement must stop forwarding
        f.send_to(q, 512*1024, replay=True)
        # Frontend death with unknown input delivery, same-session receipt recovery.
        data = pattern(f.sent, CHUNK); offset = f.sent
        f.pending.extend(data); f.sent += len(data); f.digest.update(data)
        write_bytes(q.stdin.fileno(), pack(1, offset, data))
        q.kill(); q.wait(timeout=8)
        r = s.spawn({'offsets': {'stdout': f.out, 'stderr': f.err}, 'stdin_start': f.ack}, force=True)
        f.attach(r)
        total = 1024*1024+13
        f.send_to(r, total, replay=True)
        # Lose the response to explicit EOF. Workload proves broker closed its
        # pipe, but its actual nonzero exit is held for the next attachment.
        f.receipts_before_eof = f.receipts
        write_bytes(r.stdin.fileno(), pack(2, total))
        until(lambda: (s.root/'result').exists(), 'EOF reached workload')
        r.kill(); r.wait(timeout=8)
        t = s.spawn({'offsets': {'stdout': f.out, 'stderr': f.err}}, force=True)
        hello = f.attach(t)
        assert hello['stdin_eof'] and hello['stdin_eof_at'] == total, hello
        (s.root/'exit-go').touch()
        while f.exit is None: f.consume(read_frame(t.stdout.fileno()))
        assert t.wait(timeout=8) == 23 and t.stderr.read() == b''
        f.verify(s, total)
        # Replay tails and retained actual exit survive all frontend deaths.
        replay = s.spawn({'offsets': {'stdout': total-31, 'stderr': total-47}}, force=True)
        hello = read_json(replay.stdout.fileno())
        assert hello['exit'] == {'code': 23} and hello['stdin_eof']
        chunks = {3: bytearray(), 4: bytearray()}
        while True:
            kind, value, data = read_frame(replay.stdout.fileno())
            if kind == 6: assert value == {'code': 23}; break
            if kind in chunks: chunks[kind].extend(data)
        assert chunks[3] == pattern(total-31, 31)
        assert chunks[4] == pattern(total-47, 47).translate(XOR)
        assert replay.wait(timeout=8) == 23
    print('PASS live force / frontend death / receipt and output replay / lost EOF / actual exit', flush=True)


def slow_and_partial():
    with Session() as s:
        p = s.spawn({}, mode='partial'); s.pin()
        f = Flow(); f.attach(p)
        # Frame exceeds the kernel pipe capacity. Send it from a bounded
        # transport buffer while the workload is known to have read zero.
        total = WINDOW
        data = pattern(0, total)
        f.pending.extend(data); f.sent = total; f.digest.update(data); f.max_pending = total
        write_bytes(p.stdin.fileno(), pack(1, 0, data))
        kind, pos, rest = read_frame(p.stdout.fileno())
        assert kind == 5 and 0 < pos < total and not rest, (kind, pos)
        f.consume((kind, pos, rest))
        # The receipt is a partial actual pipe write, not app consumption and
        # not the successfully forwarded whole transport frame.
        assert not (s.root/'result').exists()
        print(json.dumps({'partial_receipt': pos, 'transport_payload': total,
                          'application_bytes_read_at_barrier': 0}), flush=True)
        (s.root/'read-go').touch()
        while f.ack < total: f.consume(read_frame(p.stdout.fileno()))
        f.send_to(p, 512*1024, tiny=True, slow=True)
        f.finish(p, 512*1024); f.verify(s, 512*1024)
    print('PASS truthful partial writes / slow reader / high-frequency input / both outputs', flush=True)


def malformed():
    cases = {
        'wrong-direction': pack(3, 0, b'BAD'),
        'oversize': b'\x01'+struct.pack('>I', 16*1024*1024+1),
        'bad-eof-length': b'\x02'+struct.pack('>I', 9),
        'overflow': pack(1, (1 << 64)-1, b'BAD'),
        'gap': pack(1, 1, b'BAD'),
        'truncated-data': pack(1, 0, b'BAD')[:-1],
        'truncated-eof': pack(2, 0)[:-1],
    }
    for name, bad in cases.items():
        with Session() as s:
            p = s.spawn({}); s.pin(); f = Flow(); f.attach(p)
            write_bytes(p.stdin.fileno(), bad)
            if name.startswith('truncated'): p.stdin.close()
            # Kept-open stdin on other errors proves no lingering forwarder.
            assert p.wait(timeout=8) == 1
            assert p.stdout.read() == b'' and p.stderr.read() == b''
            state = s.query({'action': 'session'})
            assert state['leader_exit'] is None and state['failure'] is None
            # Post-create failure cannot renew creation authority.
            again = s.spawn({})
            assert 'already exists' in read_json(again.stdout.fileno())['error']
            assert again.wait(timeout=8) == 1
            q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
            hello = f.attach(q)
            assert hello['offsets']['stdin'] == 0 and not hello.get('stdin_eof')
            assert 'stdin_eof_at' not in hello
            f.send_to(q, 8192); f.finish(q, 8192); f.verify(s, 8192)
        print('PASS public malformed '+name+' detaches without stdin/EOF/terminal mutation', flush=True)


def broker_wire_fencing_and_eof():
    # Exercise backend validation without the new public decoder shielding it.
    for name, bad in [('direction', pack(6, 0)),
                      ('oversize', b'\x01'+struct.pack('>I', 0xffffffff)),
                      ('truncated', pack(2, 0)[:-1]),
                      ('range', pack(1, (1 << 64)-1, b'x'))]:
        with Session() as s:
            p = s.spawn({}); s.pin(); read_json(p.stdout.fileno())
            sock, hello = s.socket({'action': 'attach-framed-v1', 'force': True})
            with sock:
                assert hello['attachment'] == 'framed-v1'
                sock.sendall(bad)
                if name == 'truncated': sock.shutdown(socket.SHUT_WR)
                try: assert sock.recv(1) == b''
                except ConnectionResetError: pass
            p.wait(timeout=8)
            q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
            f = Flow(); hello = f.attach(q)
            assert hello['offsets']['stdin'] == 0 and 'stdin_eof_at' not in hello
            f.send_to(q, 8192); f.finish(q, 8192); f.verify(s, 8192)
        print('PASS direct broker '+name+' detaches without mutation', flush=True)
    with Session() as s:
        p = s.spawn({}, mode='partial'); s.pin(); read_json(p.stdout.fileno())
        sock, hello = s.socket({'action': 'attach-framed-v1', 'force': True})
        with sock:
            # This frame is decoded and demonstrably partly inside accept_stdin.
            sock.sendall(pack(1, 0, pattern(0, WINDOW)))
            kind, pos, _ = read_frame(sock.fileno())
            assert kind == 5 and pos == 4096
            # Queue stale data and valid EOF behind that blocked write. A forced
            # replacement must drop the old future/lock and reject all of them.
            sock.sendall(pack(1, WINDOW, b'BAD')+pack(2, WINDOW+3))
            f = Flow(); f.sent = WINDOW; f.pending.extend(pattern(0, WINDOW))
            f.digest.update(pattern(0, WINDOW)); f.max_pending = WINDOW
            q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
            hello = f.attach(q)
            assert hello['offsets']['stdin'] == 4096 and 'stdin_eof_at' not in hello
            p.wait(timeout=8)
            (s.root/'read-go').touch()
            f.send_to(q, 2*WINDOW, replay=True)
            f.finish(q, 2*WINDOW); f.verify(s, 2*WINDOW)
    with Session() as s:
        p = s.spawn({}); s.pin(); read_json(p.stdout.fileno())
        # Generation-free EOF control records future intent independently.
        eof = s.query({'action': 'stdin-eof', 'offset': 8192})
        assert eof['stdin_eof_at'] == 8192 and not eof['stdin_eof']
        q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
        f = Flow(); hello = f.attach(q)
        assert hello['stdin_eof_at'] == 8192 and not hello.get('stdin_eof')
        # A complete frame beyond declared EOF is rejected before ANY write.
        write_bytes(q.stdin.fileno(), pack(1, 0, pattern(0, 8193)))
        assert q.wait(timeout=8) == 1
        r = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
        hello = f.attach(r)
        assert hello['stdin_eof_at'] == 8192 and hello['offsets']['stdin'] == 0
        # Receipt and output may race terminal now: EOF was explicit already.
        data = pattern(0, 8192); f.pending.extend(data); f.sent = 8192; f.digest.update(data)
        write_bytes(r.stdin.fileno(), pack(1, 0, data))
        while f.exit is None: f.consume(read_frame(r.stdout.fileno()))
        assert r.wait(timeout=8) == 23
        assert f.out == 8192 and f.err == 8192
        f.verify(s, 8192)
        # Lost EOF acknowledgement never grants authority to reopen input.
        bad = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}, 'stdin_start': 8193}, force=True)
        assert 'EOF' in read_json(bad.stdout.fileno())['error']
        assert bad.wait(timeout=8) == 1
        state = s.query({'action': 'stdin-eof', 'offset': 8193})
        assert 'already declared' in state['error']
    print('PASS blocked stale-generation input/EOF fenced / future EOF survives / cannot reopen', flush=True)


def raw_and_negotiation():
    old = os.environ.get('PIPEKEEP_RECEIPTS_OLD_BINARY')
    if os.environ.get('PIPEKEEP_REQUIRE_RECEIPTS_OLD'):
        assert old and Path(old).is_file(), 'actual accepted old binary required'
    # Raw-created capable broker can negotiate framing without another launch.
    with Session() as s:
        p = s.spawn({}, framed=False); s.pin()
        hello = read_json(p.stdout.fileno()); assert 'attachment' not in hello
        if old:
            rejected = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}, 'stdin_eof': 0},
                               force=True, binary=old)
            assert rejected.wait(timeout=8) == 1 and b'--framed' in rejected.stderr.read()
            assert p.poll() is None
        q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
        f = Flow(); f.attach(q); p.wait(timeout=8)
        f.send_to(q, 8192); f.finish(q, 8192); f.verify(s, 8192)
    for old_peer in ([False, True] if old else [False]):
        with Session() as s:
            # Existing nobuffer rejects framed takeover BEFORE install/EOF;
            # actual old raw broker rejects the distinct action the same way.
            p = s.spawn({}, framed=False, nobuffer=not old_peer,
                        binary=old if old_peer else None)
            s.pin(); hello = read_json(p.stdout.fileno())
            q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}, 'stdin_eof': 0}, force=True)
            assert q.wait(timeout=8) == 1
            response, diagnostic = q.stdout.read(), q.stderr.read()
            if not old_peer: assert 'buffered session' in json.loads(response)['error']
            else: assert b'opening response' in diagnostic
            # Original raw attachment is still authoritative and usable.
            assert p.poll() is None
            write_bytes(p.stdin.fileno(), b'ab\x00\xff')
            assert read_exact(p.stdout.fileno(), 4) == b'ab\x00\xff'
            assert read_exact(p.stderr.fileno(), 4) == b'ab\x00\xff'.translate(XOR)
            eof = s.query({'action': 'stdin-eof', 'offset': 4})
            assert eof['stdin'] == 4 and eof['stdin_eof']
            assert p.wait(timeout=8) == 23
            assert (s.root/'launches').read_bytes() == b'launch\n'
        print('PASS '+('actual old broker' if old_peer else 'nobuffer broker')+' rejects without takeover or EOF', flush=True)
    for binary, flags in ([(BINARY, {'nobuffer': True})] + ([(old, {})] if old else [])):
        with Session() as s:
            p = s.spawn({}, binary=binary, **flags)
            assert p.wait(timeout=8) == 1
            assert not list(s.root.iterdir()), 'CLI rejection launched/allocated'
            assert b'--framed' in p.stderr.read()
        print('PASS '+('old frontend new option' if binary == old else 'framed+nobuffer')+' rejects before creation', flush=True)
    if not old: print('OLD PEER GATE NOT REQUESTED; no old-peer acceptance claimed', flush=True)


def authoritative_opening_and_bad_broker_output():
    cases = [('missing-marker', None), ('nobuffer-marker', None),
             ('direction', pack(1, 0, b'BAD')),
             ('oversize', b'\x03'+struct.pack('>I', 0xffffffff)),
             ('truncation', pack(3, 0, b'BAD')[:-1]),
             ('range', pack(3, (1 << 64)-1, b'BAD')),
             ('gap', pack(3, 1, b'BAD')),
             ('regressed-receipt', pack(5, 0)),
             ('invalid-exit', b'\x06'+struct.pack('>I', 2)+b'{}'),
             ('overflow-signal', b'\x06'+struct.pack('>I', len(b'{"signal":2147483647}'))+b'{"signal":2147483647}')]
    for name, bad in cases:
        with Session() as s:
            s.path.mkdir()
            with socket.socket(socket.AF_UNIX) as listener:
                listener.settimeout(8)
                listener.bind(str(s.path/'broker.sock')); listener.listen(1)
                p = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}})
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(8)
                    request = read_json(connection.fileno())
                    assert request['action'] == 'attach-framed-v1'
                    hello = {'offsets': {'stdin': 1 if name == 'regressed-receipt' else 0,
                                         'stdout': 0, 'stderr': 0}}
                    if name != 'missing-marker': hello['attachment'] = 'framed-v1'
                    if name == 'nobuffer-marker': hello['nobuffer'] = True
                    if bad is None:
                        # Public input queued before negotiation cannot reach a
                        # broker which has not affirmed the requested mode.
                        write_bytes(p.stdin.fileno(), pack(1, 0, b'BAD')+pack(2, 3))
                    connection.sendall(json.dumps(hello).encode()+b'\n')
                    if bad is not None:
                        assert read_json(p.stdout.fileno())['attachment'] == 'framed-v1'
                        connection.sendall(bad)
                        if name == 'truncation': connection.shutdown(socket.SHUT_WR)
                    assert p.wait(timeout=8) == 1
                    assert p.stdout.read() == b''
                    diagnostic = p.stderr.read()
                    if bad is None: assert b'did not confirm' in diagnostic
                    else: assert diagnostic == b''
                    try: assert connection.recv(1) == b'', 'unconfirmed input forwarded'
                    except ConnectionResetError: pass
                assert not (s.root/'launches').exists()
        print('PASS broker opening/output '+name+' closes local forwarding', flush=True)


def transport_disconnect_and_signal():
    with Session() as s:
        p = s.spawn({}); s.pin(); f = Flow(); f.attach(p)
        # Close only the public output reader. Keep stdin open and cause output;
        # the frontend must exit, releasing its input forwarding future.
        p.stdout.close()
        write_bytes(p.stdin.fileno(), pack(1, 0, pattern(0, 8192)))
        assert p.wait(timeout=8) == 1
        q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
        f.sent = 8192; f.pending.extend(pattern(0, 8192)); f.digest.update(pattern(0, 8192))
        hello = f.attach(q)
        assert not hello.get('stdin_eof')
        f.send_to(q, 16384, replay=True); f.finish(q, 16384); f.verify(s, 16384)
    with Session() as s:
        p = s.spawn({}, mode='signal'); s.pin()
        hello = read_json(p.stdout.fileno()); assert hello['attachment'] == 'framed-v1'
        tails = {3: bytearray(), 4: bytearray()}
        while True:
            kind, val, data = read_frame(p.stdout.fileno())
            if kind == 6: assert val == {'signal': 15}; break
            if kind in tails: tails[kind].extend(data)
        assert tails == {3: b'out\x00\xff', 4: b'err\xff\x00'}
        assert p.wait(timeout=8) == 143 and p.stderr.read() == b''
        q = s.spawn({'offsets': {'stdout': 0, 'stderr': 0}}, force=True)
        assert read_json(q.stdout.fileno())['exit'] == {'signal': 15}
        while read_frame(q.stdout.fileno())[0] != 6: pass
        assert q.wait(timeout=8) == 143
    print('PASS output-reader disconnect stops proxy / signal and tails retained', flush=True)


if __name__ == '__main__':
    if sys.argv[1] == '--workload':
        workload(sys.argv[2], sys.argv[3])
        raise AssertionError('workload returned')
    BINARY = str(Path(sys.argv[1]).resolve())
    assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0
    full_duplex()
    recovery()
    slow_and_partial()
    malformed()
    broker_wire_fencing_and_eof()
    raw_and_negotiation()
    authoritative_opening_and_bad_broker_output()
    transport_disconnect_and_signal()
    print('PUBLIC RECEIPTS MATRIX PASSED; exact cleanup verified ECHILD', flush=True)

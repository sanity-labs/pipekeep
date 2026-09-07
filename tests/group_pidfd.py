"""Bounded, barrier-synchronized real binary tests; no saved-PID group signals.
Only this disposable harness is a subreaper. Every signaled fixture is pinned
by an individual pidfd, acquired at an unreaped-child/ready barrier. No root,
cgroup, sysctl, PID exhaustion, or host-wide signaling. Process-local seccomp
injects errors into broker syscalls, without production test hooks.
"""
import ctypes
import errno
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
import startup


def until(predicate, message, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(.01)
    raise AssertionError(message)


def fixture(mode, path):
    path = Path(path)
    signal.alarm(20)  # independent bounded failure watchdog
    if mode == 'instant':
        os._exit(23)
    if mode == 'recreate':
        # Join the broker's existing group; no test signals that group.
        os.setpgid(0, os.getppid())
        (path / 'left').touch()
        until(lambda: (path / 'recreate').exists(), 'recreate barrier')
        os.setpgid(0, 0)
        (path / 'recreated').touch()
        while True:
            signal.pause()
    if mode in ('leader', 'ignore-leader'):
        if mode == 'ignore-leader':
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        (path / 'ready').write_text(str(os.getpid()))
        until(lambda: (path / 'finish').exists(), 'leader finish', 15)
        os._exit(23)
    child = os.fork()
    if child == 0:
        if mode.startswith('escape'):
            os.setsid()
        if mode == 'ignore-child':
            def term(_sig, _frame):
                with (path / 'terms').open('a') as stream:
                    stream.write('TERM\n')
            signal.signal(signal.SIGTERM, term)
        if mode not in ('holder', 'escape-holder'):
            os.close(1)
            os.close(2)
        (path / 'child').write_text(str(os.getpid()))
        until(lambda: (path / 'child-go').exists(), 'child ownership barrier')
        if mode == 'zombie':
            os._exit(19)
        while True:
            signal.pause()
    until(lambda: (path / 'leader-go').exists(), 'leader exit barrier')
    os._exit(23)


if len(sys.argv) > 1 and sys.argv[1] == '--fixture':
    fixture(sys.argv[2], sys.argv[3])
    raise AssertionError('fixture fell through')

BINARY = str(Path(sys.argv[1]).resolve())
SCRIPT = str(Path(__file__).resolve())
LIBC = ctypes.CDLL(None, use_errno=True)
assert LIBC.prctl(36, 1, 0, 0, 0) == 0  # PR_SET_CHILD_SUBREAPER: harness only
CAP = 'group-pidfd-cancel-v1'


def read_json(sock):
    data = b''
    while not data.endswith(b'\n'):
        byte = sock.recv(1)
        if not byte:
            raise AssertionError('broker closed before response: ' + repr(data))
        data += byte
    return json.loads(data)


def seccomp(kind):
    if kind == 'sigchld':
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        return
    # Linux x86_64/aarch64 syscall numbers are identical for these two calls.
    # Test only these reviewed ABIs; other architectures run real cases and
    # explicitly omit this injection submatrix.
    class Filter(ctypes.Structure):
        _fields_ = [('code', ctypes.c_ushort), ('jt', ctypes.c_ubyte),
                    ('jf', ctypes.c_ubyte), ('k', ctypes.c_uint)]
    class Program(ctypes.Structure):
        _fields_ = [('len', ctypes.c_ushort), ('filter', ctypes.POINTER(Filter))]
    allow = 0x7fff0000
    deny = 0x50000
    if kind.startswith('startup-'):
        connect = 42 if os.uname().machine == 'x86_64' else 203
        wait4 = 61 if os.uname().machine == 'x86_64' else 260
        code = [(0x20, 0, 0, 0), (0x15, 0, 1, connect),
                (6, 0, 0, deny | errno.ECONNREFUSED)]
        if kind != 'startup-timeout':
            error = errno.ECHILD if kind == 'startup-wait-echild' else errno.EIO
            code += [(0x15, 0, 3, wait4), (0x20, 0, 0, 32),
                     (0x15, 0, 1, os.WNOHANG), (6, 0, 0, deny | error)]
        code += [(6, 0, 0, allow)]
    elif kind == 'wait':
        syscall = 61 if os.uname().machine == 'x86_64' else 260
        code = [(0x20, 0, 0, 0), (0x15, 0, 1, syscall),
                (6, 0, 0, deny | errno.ECHILD), (6, 0, 0, allow)]
    elif kind == 'accept':
        syscall = 288 if os.uname().machine == 'x86_64' else 242
        code = [(0x20, 0, 0, 0), (0x15, 0, 1, syscall), (6, 0, 0, deny | errno.EMFILE), (6, 0, 0, allow)]
    elif kind == 'open':
        code = [(0x20, 0, 0, 0), (0x15, 0, 1, 434), (6, 0, 0, deny | errno.ENOSYS), (6, 0, 0, allow)]
    elif kind == 'acquire':
        code = [(0x20, 0, 0, 0), (0x15, 0, 3, 434), (0x20, 0, 0, 16),
                (0x15, 1, 0, os.getpid()), (6, 0, 0, deny | errno.EMFILE), (6, 0, 0, allow)]
    elif kind == 'flags':
        code = [(0x20, 0, 0, 0), (0x15, 0, 1, 424), (6, 0, 0, deny | errno.EINVAL), (6, 0, 0, allow)]
    else:  # zero probes work; TERM/KILL return EPERM
        code = [(0x20, 0, 0, 0), (0x15, 0, 3, 424), (0x20, 0, 0, 24),
                (0x15, 1, 0, 0), (6, 0, 0, deny | errno.EPERM), (6, 0, 0, allow)]
    filters = (Filter * len(code))(*(Filter(*item) for item in code))
    program = Program(len(code), filters)
    if LIBC.prctl(38, 1, 0, 0, 0) != 0 or LIBC.prctl(22, 2, ctypes.byref(program), 0, 0) != 0:
        os._exit(98)


class Session:
    def __init__(self, mode, hardened=True, grace=1, ttl=4, settle=3, fault=None, binary=BINARY):
        self.temp = tempfile.TemporaryDirectory(prefix='pk-group-')
        self.root = Path(self.temp.name)
        self.path = self.root / hashlib.sha256(b'test').hexdigest()[:32]
        self.path.mkdir()
        self.socket = self.path / 'broker.sock'
        self.handles = []
        self.children = []
        self.sockets = []
        self.env = dict(os.environ, PIPEKEEP_RUNTIME_DIR=str(self.root),
                        PIPEKEEP_SESSION_TTL_SECS=str(ttl), PIPEKEEP_CANCEL_GRACE_SECS=str(grace),
                        PIPEKEEP_CANCEL_SETTLE_SECS=str(settle))
        self.log = (self.root / 'broker.log').open('w+')
        args = [binary, '__broker', '--id', 'test', '--session-dir', str(self.path)]
        if hardened:
            args += ['--group-pidfd']
        args += ['--', sys.executable, SCRIPT, '--fixture', mode, str(self.root)]
        self.broker = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                       stderr=self.log, env=self.env, start_new_session=True,
                                       preexec_fn=(lambda: seccomp(fault)) if fault else None)
        # Popen has not polled/reaped; default SIGCHLD, no other reaper.
        self.handles.append(os.pidfd_open(self.broker.pid))

    def connect(self, action):
        until(lambda: self.socket.exists() or self.broker.poll() is not None, 'broker ready')
        if not self.socket.exists():
            self.log.seek(0)
            raise RuntimeError(self.log.read())
        sock = socket.socket(socket.AF_UNIX)
        sock.settimeout(6)
        sock.connect(str(self.socket))
        sock.sendall(json.dumps({'action': action}).encode() + b'\n')
        self.sockets.append(sock)
        return sock

    def query(self):
        sock = self.connect('session')
        result = read_json(sock)
        sock.close()
        return result

    def opening(self):
        sock = self.connect('attach')
        result = read_json(sock)
        sock.close()
        return result

    def child(self):
        file = self.root / 'child'
        until(lambda: file.exists() and file.stat().st_size, 'child readiness')
        pid = int(file.read_text())
        # Child remains at its ownership barrier; parent has not reaped it.
        fd = os.pidfd_open(pid)
        self.handles.append(fd)
        self.children.append(pid)
        return pid, fd

    def release(self):
        (self.root / 'child-go').touch()
        (self.root / 'leader-go').touch()

    def reap(self, pid, expected):
        result = until(lambda: os.waitpid(pid, os.WNOHANG)[1], 'exact descendant reap')
        assert os.waitstatus_to_exitcode(result) == expected, (result, expected)
        self.children.remove(pid)
        print(json.dumps({'exact_descendant_wait': pid, 'status': expected}), flush=True)

    def cancel(self):
        sock = self.connect('cancel-group-pidfd')
        result = read_json(sock)
        sock.close()
        return result

    def close(self):
        for sock in self.sockets:
            sock.close()
        for fd in reversed(self.handles):
            try:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.close(fd)
        self.broker.wait(timeout=5)
        # After broker exit all controlled orphans belong to this harness.
        end = time.monotonic() + 5
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid == 0:
                assert time.monotonic() < end, 'fixture child survived exact cleanup'
                time.sleep(.01)
        self.log.close()
        self.temp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def retained23(s):
    return until(lambda: (h if (h := s.opening()).get('exit') == {'code': 23} else None), 'retained exit23')


def readable(sock):
    return bool(select.select([sock], [], [], 0)[0])


def settled(s, result, outcome='cancel_won', code=23):
    assert result['outcome'] == outcome, result
    assert result['exit'] == {'code': code}, result
    assert result['session']['cancel_state'] == 'settled', result
    assert CAP in result['session']['capabilities']
    repeated = s.cancel()
    assert repeated['outcome'] == 'already_exited', repeated
    assert repeated['exit'] == result['exit']
    print(json.dumps({'cancel': result, 'repeat': repeated}), flush=True)


def run():
    print(json.dumps({'binary': BINARY, 'sha256': hashlib.sha256(Path(BINARY).read_bytes()).hexdigest(),
                      'kernel': os.uname().release, 'uid': os.getuid()}), flush=True)
    startup.compatibility(BINARY)
    with Session('instant') as s:
        try:
            fact = s.query()['session']
        except RuntimeError as error:
            if (os.environ.get('PIPEKEEP_REQUIRE_GROUP_PIDFD_TESTS') or
                    'group pidfd unsupported; workload not started' not in str(error)):
                raise
            print('UNSUPPORTED group-pidfd runtime: ' + str(error), flush=True)
            return
        assert CAP in fact['capabilities'], fact
        retained23(s)
        settled(s, s.cancel(), 'already_exited')
    print('PASS instant leader exit/acquire-before-wait/natural23/no-signal-after-settlement', flush=True)

    with Session('leader') as s:
        until(lambda: (s.root / 'ready').exists(), 'live leader ready')
        # Ordinary cancel on an opted-in session must use the same safe path.
        sock = s.connect('cancel')
        result = read_json(sock)
        assert result['exit'] == {'signal': 15}, result
        assert result['outcome'] == 'cancel_won', result
        repeated = s.cancel()
        assert repeated['outcome'] == 'already_exited' and repeated['exit'] == {'signal': 15}
        print(json.dumps({'live_leader_term': result, 'repeat': repeated}), flush=True)
    print('PASS opted-in ordinary cancel uses safe TERM and retains actual signal15', flush=True)

    for mode, expected in [('child', -15), ('ignore-child', -9), ('holder', -15), ('zombie', 19)]:
        with Session(mode) as s:
            pid, fd = s.child()
            assert CAP in s.opening()['session']['capabilities']
            s.release()
            until(lambda: s.query()['leader_exit'] == {'code': 23}, 'positive leader reap')
            if mode != 'holder':
                retained23(s)  # reaped leader and both EOF, child still in group
            first = s.connect('cancel-group-pidfd')
            # Socket accept order does not establish cancellation admission order.
            until(lambda: s.query()['session']['cancel_state'] == 'running', 'first caller admitted')
            second = s.connect('cancel-group-pidfd')
            until(lambda: select.select([fd], [], [], 0)[0], 'real descendant termination')
            assert not readable(first), 'zombie-only group falsely settled'
            time.sleep(.12)  # probe repeatedly while deliberately unreaped
            assert not readable(first), 'zombie-only group falsely settled after signal success'
            s.reap(pid, expected)
            settled(s, read_json(first))
            result = read_json(second)
            assert result['outcome'] == 'already_exited', result
            if mode == 'ignore-child':
                assert (s.root / 'terms').read_text() == 'TERM\n'
        print('PASS delayed/queued/zombie-reap/' + mode, flush=True)

    with Session('ignore-child', ttl=1, grace=2) as s:
        pid, fd = s.child()
        s.release()
        retained23(s)
        time.sleep(.65)  # cancellation begins after old terminal TTL started
        transport = subprocess.Popen([BINARY, 'cancel', '--id', 'test', '--require-group-pidfd'],
                                     env=s.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        transport_fd = os.pidfd_open(transport.pid)
        until(lambda: (s.root / 'terms').exists(), 'TERM admission barrier')
        begun = time.monotonic()
        signal.pidfd_send_signal(transport_fd, signal.SIGSTOP)
        time.sleep(.25)
        signal.pidfd_send_signal(transport_fd, signal.SIGKILL)
        transport.wait(timeout=3)
        os.close(transport_fd)
        queued = s.connect('cancel-group-pidfd')
        until(lambda: select.select([fd], [], [], 0)[0], 'KILL despite stopped/dead requester')
        elapsed = time.monotonic() - begun
        assert 1.6 <= elapsed < 2.8, elapsed
        assert s.broker.poll() is None, 'old terminal TTL dropped active cancel'
        assert (s.root / 'terms').read_text() == 'TERM\n', 'duplicate TERM sequence'
        s.reap(pid, -9)
        result = read_json(queued)
        assert result['outcome'] == 'already_exited', result
        settled(s, s.cancel(), 'already_exited')
        print(json.dumps({'one_grace_seconds': elapsed, 'lost_reply': result}), flush=True)
        until(lambda: s.broker.poll() is not None, 'fresh idle TTL expiry', 3)
        assert not s.path.exists()
    print('PASS stopped/killed requester/lost-reply/one-grace/TTL-admission-and-expiry', flush=True)

    with Session('child') as s:
        pid, fd = s.child()
        s.release()
        retained23(s)
        lost = s.connect('cancel-group-pidfd')
        until(lambda: select.select([fd], [], [], 0)[0], 'TERM')
        s.reap(pid, -15)
        until(lambda: s.query()['session']['cancel_state'] == 'settled', 'settlement before lost read')
        lost.close()  # reply is never read; safe redelivery cannot attribute winner
        settled(s, s.cancel(), 'already_exited')
    print('PASS response lost after settlement', flush=True)

    with Session('recreate', grace=1) as s:
        until(lambda: (s.root / 'left').exists(), 'leader left group')
        pending = s.connect('cancel-group-pidfd')
        until(lambda: s.query()['session']['cancel_state'] == 'running', 'cancel admitted in empty group')
        time.sleep(.15)
        assert not readable(pending)
        (s.root / 'recreate').touch()
        until(lambda: (s.root / 'recreated').exists(), 'leader recreated original group')
        result = read_json(pending)
        assert result['exit'] == {'signal': 9}, result
        assert result['outcome'] == 'cancel_won', result
        assert s.cancel()['outcome'] == 'already_exited'
        print(json.dumps({'recreated_group_cancel': result}), flush=True)
    print('PASS living-leader group absence is reversible until reap', flush=True)

    for mode in ('escape', 'escape-holder'):
        with Session(mode, grace=0, settle=1) as s:
            pid, fd = s.child()
            s.release()
            if mode == 'escape':
                retained23(s)
                settled(s, s.cancel(), 'already_exited')
            else:
                until(lambda: s.query()['leader_exit'] == {'code': 23}, 'escaped-holder leader reap')
                result = s.cancel()
                assert 'deadline expired' in result['error'], result
                assert 'exit' not in result
                assert s.cancel()['error'] == result['error']
                print(json.dumps({'escaped_output_unresolved': result}), flush=True)
            assert not select.select([fd], [], [], 0)[0], 'escaped child was signaled'
            signal.pidfd_send_signal(fd, signal.SIGKILL)
            s.reap(pid, -9)
    print('PASS original-group-only/escaped-survivor/escaped-pipe-timeout', flush=True)

    with Session('leader', hardened=False) as s:
        until(lambda: (s.root / 'ready').exists(), 'legacy ready')
        assert s.query()['session'] is None
        result = s.cancel()
        assert 'lacks verified' in result['error'], result
        # Guarded CLI cannot downgrade either.
        cli = subprocess.run([BINARY, 'cancel', '--id', 'test', '--require-group-pidfd'],
                             env=s.env, capture_output=True, timeout=3)
        assert cli.returncode == 1 and b'lacks verified' in cli.stderr
        (s.root / 'finish').touch()
        retained23(s)
    print('PASS non-opted-session guarded cancellation refuses before signal', flush=True)

    if os.uname().machine in ('x86_64', 'aarch64'):
        startup.ambiguous_startup(BINARY, seccomp)
        with Session('leader', fault='wait', ttl=1) as s:
            until(lambda: (s.root / 'ready').exists(), 'failed-wait workload ready')
            # Pin the still-live leader at its fixture barrier for exact cleanup.
            leader = int((s.root / 'ready').read_text())
            leader_fd = os.pidfd_open(leader)
            s.handles.append(leader_fd)
            until(lambda: s.query()['failure'], 'real hardened wait4 failure')
            (s.root / 'finish').touch()
            until(lambda: select.select([leader_fd], [], [], 0)[0], 'leader actually exited')
            until(lambda: s.query()['stdout_closed'] and s.query()['stderr_closed'], 'both EOF')
            time.sleep(1.2)
            state = s.query()
            assert state['leader_exit'] is None and 'command wait failed' in state['failure'], state
            assert s.broker.poll() is None and s.path.is_dir(), 'failed wait started terminal TTL'
            assert 'exit' not in s.opening(), 'failed wait fabricated an attachment exit'
            result = s.cancel()
            assert 'command wait failed' in result['error'] and 'exit' not in result, result
            assert s.cancel()['error'] == result['error']
            assert s.query()['session']['cancel_state'] == 'failed'
            print(json.dumps({'hardened_wait_failure': state, 'cancel': result}), flush=True)
        print('PASS real hardened wait4 ECHILD/EOF retains no actual exit/no settlement/sticky failure', flush=True)
        for fault in ('open', 'flags', 'sigchld'):
            with Session('leader', fault=fault) as s:
                assert s.broker.wait(timeout=3) == 1
                assert not (s.root / 'ready').exists(), 'unsupported workload dispatched'
                s.log.seek(0)
                message = s.log.read()
                assert 'workload not started' in message, message
                print(json.dumps({'unsupported': fault, 'error': message}), flush=True)
        with Session('leader', fault='acquire') as s:
            until(lambda: (s.root / 'ready').exists(), 'failed acquisition workload did launch')
            fact = s.opening()['session']
            assert fact['capabilities'] == [] and fact['workload_started'], fact
            assert 'acquisition/verification failed' in fact['cancel_error']
            assert 'workload started' in s.cancel()['error']
            # Broker retains the actual child and exit, without fallback.
            (s.root / 'finish').touch()
            assert retained23(s)['exit'] == {'code': 23}
            print(json.dumps({'failed_acquisition': fact}), flush=True)
        with Session('leader', fault='signal') as s:
            until(lambda: (s.root / 'ready').exists(), 'EPERM fixture ready')
            result = s.cancel()
            assert 'syscall failed' in result['error'] and 'Operation not permitted' in result['error'], result
            assert 'exit' not in result
            assert s.cancel()['error'] == result['error']
            (s.root / 'finish').touch()
            retained23(s)
            print(json.dumps({'real_syscall_error': result}), flush=True)
        print('PASS syscall ENOSYS/EINVAL predispatch, EMFILE acquisition, EPERM signal', flush=True)
    else:
        print('UNSUPPORTED seccomp injection ABI: ' + os.uname().machine, flush=True)
        assert not os.environ.get('PIPEKEEP_REQUIRE_GROUP_PIDFD_TESTS')

    if os.uname().machine in ('x86_64', 'aarch64'):
        with Session('leader', fault='accept', ttl=1) as s:
            until(lambda: (s.root / 'ready').exists(), 'accept error workload dispatch')
            pending = s.connect('session')
            def retained_accept_error():
                s.log.seek(0)
                return 'broker accept failed; session/workload retained' in s.log.read()
            until(retained_accept_error, 'real accept4 EMFILE handling')
            assert s.broker.poll() is None, 'admission error dropped authority owner'
            (s.root / 'finish').touch()
            until(lambda: s.broker.poll() is not None, 'resource failure still permits idle TTL', 3)
            assert s.broker.returncode == 0
            pending.close()
        print('PASS accept4 EMFILE retains broker/workload ownership until ordinary idle expiry', flush=True)

    # Failed cancellation can expire while the original group remains alive.
    # Expiry must neither fabricate settlement nor imply workload termination.
    if os.uname().machine in ('x86_64', 'aarch64'):
        with Session('ignore-child', fault='signal', ttl=1) as s:
            pid, fd = s.child()
            s.release()
            retained23(s)
            result = s.cancel()
            assert 'syscall failed' in result['error']
            until(lambda: s.broker.poll() is not None, 'failed session idle expiry', 3)
            assert not s.path.exists()
            assert not select.select([fd], [], [], 0)[0], 'expiry silently killed workload'
            signal.pidfd_send_signal(fd, signal.SIGKILL)
            s.reap(pid, -9)
        print('PASS failed-operation expiry loses authority with original member alive', flush=True)

    with tempfile.TemporaryDirectory(prefix='pk-public-') as root:
        env = dict(os.environ, PIPEKEEP_RUNTIME_DIR=root, PIPEKEEP_SESSION_TTL_SECS='2')
        if os.uname().machine in ('x86_64', 'aarch64'):
            rejected = subprocess.run([BINARY, '--id', 'unsupported', '--group-pidfd', '--', '/bin/sh', '-c',
                                       'touch "' + root + '/must-not-launch"'],
                                      input=b'{}\n', capture_output=True, env=env, timeout=5,
                                      preexec_fn=lambda: seccomp('open'))
            assert rejected.returncode == 1 and b'workload not started' in rejected.stdout, rejected.stdout
            assert not (Path(root) / 'must-not-launch').exists()
            print(json.dumps({'public_unsupported_creation': json.loads(rejected.stdout)}), flush=True)
        opened = subprocess.run([BINARY, '--id', 'public', '--group-pidfd', '--', '/bin/sh', '-c', 'exit 23'],
                                input=b'{"stdin_eof":0}\n', capture_output=True, env=env, timeout=5)
        assert opened.returncode == 23, (opened.stdout, opened.stderr)
        header = json.loads(opened.stdout.splitlines()[0])
        assert CAP in header['session']['capabilities'], header
        queried = subprocess.run([BINARY, 'session', '--id', 'public'], capture_output=True, env=env, timeout=3)
        assert queried.returncode == 0
        assert json.loads(queried.stdout)['leader_exit'] == {'code': 23}
        retrofit = subprocess.run([BINARY, '--id', 'public', '--group-pidfd', '--', '/bin/true'],
                                  input=b'{"offsets":{"stdout":0,"stderr":0}}\n',
                                  capture_output=True, env=env, timeout=3)
        assert retrofit.returncode == 1 and b'cannot be retrofitted' in retrofit.stdout
        for guard in (['--require-group-pidfd'], []):
            canceled = subprocess.run([BINARY, 'cancel', '--id', 'public'] + guard,
                                      capture_output=True, env=env, timeout=3)
            assert canceled.returncode == 23, canceled.stderr
            result = json.loads(canceled.stdout)
            assert result['outcome'] == 'already_exited', result
            assert result['session']['cancel_state'] == 'settled'
        print(json.dumps({'public_opening': header, 'public_control': json.loads(queried.stdout),
                          'public_cancel': result, 'retrofit_error': json.loads(retrofit.stdout)}), flush=True)
        until(lambda: not list(Path(root).iterdir()), 'public broker idle cleanup', 4)
        # The detached broker was adopted by this test subreaper.
        until(lambda: os.waitpid(-1, os.WNOHANG)[0], 'public broker reap')
    print('PASS public opt-in/verified-fact/guarded-and-ordinary-cancel/no-retrofit', flush=True)

    legacy = os.environ.get('PIPEKEEP_LEGACY_BINARY')
    if legacy:
        print(json.dumps({'legacy_binary': legacy, 'sha256': hashlib.sha256(Path(legacy).read_bytes()).hexdigest()}), flush=True)
        with Session('leader', hardened=False, binary=legacy) as s:
            until(lambda: (s.root / 'ready').exists(), 'shipped broker workload ready')
            cli = subprocess.run([BINARY, 'cancel', '--id', 'test', '--require-group-pidfd'],
                                 env=s.env, capture_output=True, timeout=3)
            assert cli.returncode == 1, cli.stdout
            assert b'control response' in cli.stderr, cli.stderr
            s.log.flush()
            s.log.seek(0)
            log = s.log.read()
            assert 'unknown variant `cancel-group-pidfd`' in log, log
            (s.root / 'finish').touch()
            assert retained23(s)['exit'] == {'code': 23}, 'old broker was signaled'
            print(json.dumps({'shipped_guard_cli': cli.stderr.decode(), 'shipped_broker_log': log}), flush=True)
        print('PASS actual shipped broker rejects unique action before signaling', flush=True)

    try:
        os.waitpid(-1, os.WNOHANG)
    except ChildProcessError:
        print('CAPABLE MATRIX PASSED; exact cleanup verified ECHILD', flush=True)
    else:
        raise AssertionError('unreaped fixture remains')


if __name__ == '__main__':
    run()

"""Finite-output real peers. This isolated harness alone is a subreaper.
All fixtures have barriers, finite production/watchdogs and exact pidfd cleanup.
No production fault control, process matching or capability skip.
"""
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
import threading
import time
import receipts as wire

SCRIPT = str(Path(__file__).resolve())
CAP = 'finite-output-v1'
ATTACH = 'framed-output-v1'


def until(test, message, timeout=8):
    return wire.until(test, message, timeout)


def put(root, name, value):
    (root/(name+'.tmp')).write_text(json.dumps(value))
    (root/(name+'.tmp')).rename(root/name)


def fixture(root, mode, count):
    root = Path(root)
    signal.alarm(25)
    with (root/'launches').open('ab') as f: f.write(b'launch\n')
    children = []
    if mode in ('group-flood', 'escape-idle', 'escape-flood', 'unreaped', 'joined'):
        for i in range(2 if mode in ('group-flood', 'unreaped', 'escape-flood') else 1):
            pid = os.fork()
            if pid == 0:
                signal.alarm(25)
                if mode.startswith('escape') or mode=='joined': os.setsid()
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                signal.signal(signal.SIGPIPE, signal.SIG_IGN)
                put(root, 'child-ready'+str(i), os.getpid())
                until(lambda: (root/'children-go').exists(), 'child ownership', 15)
                if mode=='joined': os.write(1,b'A'*(count+1))
                if mode not in ('escape-idle','joined'):
                    for _ in range(8192):  # 32 MiB finite flood maximum per child
                        try: os.write(1+i, bytes([65+i])*4096)
                        except BrokenPipeError: break
                put(root, 'survived'+str(i), True)
                while True: signal.pause()
            children.append(pid)
        for i in range(len(children)): until(lambda: (root/('child-ready'+str(i))).exists(), 'child ready')
    if mode in ('finite','zero-exit','escape-idle','escape-flood','joined'):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if mode == 'flush':
        def term(_s, _f):
            os.write(1, b'F'*(256*1024))
            os._exit(42)
        signal.signal(signal.SIGTERM, term)
    elif mode == 'manual':
        def manual_term(_s,_f):
            with (root/'terms').open('ab') as f: f.write(b'T')
        signal.signal(signal.SIGTERM, manual_term)
    put(root, 'ready', [os.getppid(), os.getpid(), children])
    until(lambda: (root/'go').exists(), 'ownership barrier', 15)
    if mode in ('stream', 'eof'):
        total = 0
        while True:
            data = os.read(0, 4096)
            if not data: break
            total += len(data)
            assert total <= 1024*1024
            os.write(1, data)
            os.write(2, data.translate(wire.XOR))
        put(root, 'input', total)
        os._exit(23)
    if mode in ('group-flood', 'unreaped', 'joined'):
        os._exit(23)
    if mode == 'manual':
        until(lambda: (root/'produce').exists(), 'manual production', 15)
    if mode.startswith('escape'):
        os.write(1, b'A'*(count+1))
        os._exit(23)
    if mode == 'flush':
        os.write(1, b'A'*(count+1))
        while True: signal.pause()
    if mode == 'concurrent':
        def output(fd, byte):
            for _ in range(count): os.write(fd, byte)
        a = threading.Thread(target=output, args=(1,b'A'))
        b = threading.Thread(target=output, args=(2,b'B'))
        a.start(); b.start(); a.join(); b.join()
    else:
        os.write(1, b'A'*count)
        os.write(2, b'B'*count)
    os._exit(0 if mode == 'zero-exit' else 23)


def frame(fd):
    header = wire.read_exact(fd, 5)
    kind, size = header[0], struct.unpack('>I', header[1:])[0]
    assert size <= 40960
    data = wire.read_exact(fd, size)
    assert kind in (3,4,5,7,8), ('legacy or invalid frame', kind)
    if kind in (7,8): return kind, json.loads(data), b''
    return kind, struct.unpack('>Q',data[:8])[0], data[8:]


def accounting(p):
    charged = sum(p[s]['retained']+p[s]['reserved'] for s in ('stdout','stderr'))
    assert charged <= p['limit'], p
    assert p['tail_read'] <= 262144, p
    assert 0 <= p['io_inflight'] <= 3, p


class Session:
    def __init__(self, limit, mode='finite', count=1, grace=0, settle=2, reaping=True):
        self.temp = tempfile.TemporaryDirectory(prefix='pk-bounds-')
        self.root = Path(self.temp.name)
        self.path = self.root/hashlib.sha256(b'owned').hexdigest()[:32]
        self.env = dict(os.environ, PIPEKEEP_RUNTIME_DIR=str(self.root), PIPEKEEP_SESSION_TTL_SECS='30',
                        PIPEKEEP_CANCEL_GRACE_SECS=str(grace), PIPEKEEP_CANCEL_SETTLE_SECS=str(settle))
        self.limit, self.mode, self.count = limit, mode, count
        self.proxies, self.handles, self.descendants, self.waits = [], {}, [], []
        self.reaping = reaping
        self.done = threading.Event()
        self.thread = None
        self.broker = None
        self.leader = None
        self.actual_leader_result = None
        self.fault = None

    def spawn(self, hello=None, bounded=True, force=False, binary=None, creating=False, extra=()):
        args = [binary or BINARY, '--id', 'owned']
        if bounded: args += ['--output-bounded']
        if creating: args += ['--group-pidfd', '--output-limit', str(self.limit)]
        if force: args += ['--force']
        args += list(extra)
        args += ['--', sys.executable, SCRIPT, '--fixture', str(self.root), self.mode, str(self.count)]
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env, bufsize=0, preexec_fn=self.fault)
        self.proxies.append(p)
        try: wire.write_bytes(p.stdin.fileno(), json.dumps(hello or {}).encode()+b'\n')
        except BrokenPipeError: pass
        return p

    def pin(self):
        until(lambda: (self.root/'ready').exists(), 'workload launched')
        self.broker, self.leader, self.descendants = json.loads((self.root/'ready').read_text())
        for pid in [self.broker, self.leader]+self.descendants: self.handles[pid] = os.pidfd_open(pid)
        def reap():
            pending = set(self.descendants)
            while pending and not self.done.wait(.002):
                if not self.reaping: continue
                for pid in list(pending):
                    try: result, status = os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError: continue  # still owned by original leader
                    if result:
                        self.waits.append([pid,status]); pending.remove(pid)
        self.thread = threading.Thread(target=reap)
        self.thread.start()

    def query(self, action='session', **fields):
        with socket.socket(socket.AF_UNIX) as sock:
            sock.settimeout(8); sock.connect(str(self.path/'broker.sock'))
            sock.sendall(json.dumps(dict(action=action, **fields)).encode()+b'\n')
            return wire.read_json(sock.fileno())

    def ready(self):
        p = self.spawn(creating=True)
        self.pin()
        h = wire.read_json(p.stdout.fileno())
        assert h['attachment'] == ATTACH and CAP in h['session']['capabilities'], h
        assert 'exit' not in h and 'stdout_eof' not in h
        return p, h

    def go(self): (self.root/'go').touch()

    def collect(self, p, offsets=(0,0)):
        data = [bytearray(),bytearray()]
        positions = list(offsets)
        states = []
        while True:
            kind, value, payload = frame(p.stdout.fileno())
            if kind in (3,4):
                i = kind-3
                assert value == positions[i]
                data[i].extend(payload); positions[i] += len(payload)
            elif kind in (7,8):
                accounting(value); states.append(value)
                if kind == 8:
                    assert [value[s]['retained'] for s in ('stdout','stderr')] == positions
                    file_bytes=[]
                    for name, payload, start in zip(('stdout','stderr'),data,offsets):
                        path=self.path/(name+'.buffer')
                        if path.is_file():
                            stored=path.read_bytes()
                            assert len(stored)==value[name]['retained'] and stored[start:]==payload,(name,value)
                            file_bytes.append(len(stored))
                        else: file_bytes.append(value[name]['retained'])  # private /dev/full fault fixture
                    assert sum(file_bytes)<=value['limit']
                    print(json.dumps({'retained_file_bytes':file_bytes,'shared_limit':value['limit'],
                                      'replay_offsets':list(offsets),'replay_sha256':[hashlib.sha256(x).hexdigest() for x in data]}),flush=True)
                    self.actual_leader_result = value['leader_exit']
                    code = p.wait(timeout=8)
                    expected = value['leader_exit'].get('code',128+value['leader_exit'].get('signal',0)) if value['leader_exit'] else 1
                    if expected == 0 and value['first_stop'] is not None: expected = 1
                    assert code == expected, (code,value)
                    assert p.stderr.read() == b''
                    return data, value, states

    def __enter__(self): return self
    def __exit__(self, *error):
        if not self.handles and (self.root/'ready').exists(): self.pin()
        for p in self.proxies:
            if p.poll() is None: p.kill()
            self.waits.append([p.pid, p.wait(timeout=8), 'frontend-returncode'])
            for stream in (p.stdin,p.stdout,p.stderr):
                if not stream.closed: stream.close()
        for pid,fd in reversed(list(self.handles.items())):
            try: signal.pidfd_send_signal(fd,signal.SIGKILL)
            except ProcessLookupError: pass
        self.done.set()
        if self.thread: self.thread.join(timeout=2); assert not self.thread.is_alive()
        for pid,fd in self.handles.items():
            assert select.select([fd],[],[],8)[0], ('cleanup pidfd',pid)
            os.close(fd)
        end = time.monotonic()+8
        while True:
            try: pid,status = os.waitpid(-1,os.WNOHANG)
            except ChildProcessError: break
            assert time.monotonic()<end, 'cleanup wait/reap deadline'
            if pid: self.waits.append([pid,status,'adopted-waitstatus'])
            else: time.sleep(.005)
        print(json.dumps({'cleanup':'ECHILD','mode':self.mode,'exact_wait_receipts':self.waits,'broker_owned_leader':self.leader,'actual_broker_wait_result':self.actual_leader_result}),flush=True)
        self.temp.cleanup()


def boundaries():
    for limit, count, overflow in [(0,0,False),(0,1,True),(2,1,False),(1,1,True),(8192,4096,False),(8191,4096,True)]:
        with Session(limit,count=count,grace=1) as s:
            p,_=s.ready();s.go();data,fact,states=s.collect(p)
            assert fact['first_stop'] == ('output_limit' if overflow else None), fact
            assert sum(map(len,data)) == min(limit,2*count)
            assert data[0]==b'A'*len(data[0]) and data[1]==b'B'*len(data[1])
            assert fact['leader_exit'] is not None
            assert all(fact[n]['collection']['state']=='eof' for n in ('stdout','stderr')),fact
            if overflow: assert fact['original_group_absent']
            # Replay after a completed or lost controller uses the same prefixes.
            q=s.spawn({'offsets':{'stdout':0,'stderr':0}},force=True)
            h=wire.read_json(q.stdout.fileno());assert h['output']['first_stop']==fact['first_stop']
            again,retained,_=s.collect(q);assert again==data
            assert (s.root/'launches').read_bytes()==b'launch\n'
            print(json.dumps({'case':'boundary','limit':limit,'count':count,'fact':fact,'hashes':[hashlib.sha256(x).hexdigest() for x in data]}),flush=True)
    with Session(1,mode='zero-exit',count=1,grace=1) as s:
        p,_=s.ready();s.go();_,fact,_=s.collect(p)
        assert fact['leader_exit']=={'code':0} and p.returncode==1,fact
    for limit in (1,7,31,4095):
        with Session(limit,mode='concurrent',count=8192,grace=1) as s:
            p,_=s.ready();s.go();data,fact,states=s.collect(p)
            assert sum(map(len,data))==limit
            assert data==[b'A'*len(data[0]),b'B'*len(data[1])]
            assert fact['first_stop']=='output_limit' and fact['original_group_absent']
            print(json.dumps({'case':'concurrent','limit':limit,'fact':fact,'hashes':[hashlib.sha256(x).hexdigest() for x in data]}),flush=True)
    print('PASS exact/zero/first excess/shared concurrent prefixes/replay/natural zero',flush=True)


def groups(modes=('group-flood','flush','escape-idle','escape-flood','unreaped')):
    for mode in modes:
        with Session(1024,mode=mode,count=1024,grace=1,reaping=mode!='unreaped') as s:
            p,_=s.ready();s.go()
            if mode in ('group-flood','escape-flood','unreaped'):
                # In escape-flood the leader must finish its final write and
                # actually exit before children can fill its shared stdout pipe.
                leader=until(lambda:(f if (f:=s.query())['leader_exit']=={'code':23} else None),
                             'actual natural leader reap')
                print(json.dumps({'case':mode,'before_children_release':{'leader_exit':leader['leader_exit']}}),flush=True)
            (s.root/'children-go').touch()
            data,fact,_=s.collect(p)
            assert fact['first_stop']=='output_limit',fact
            if mode=='flush': assert fact['leader_exit']=={'signal':9},fact
            else: assert fact['leader_exit']=={'code':23},fact
            if mode=='unreaped':
                assert not fact['original_group_absent']
                assert any(fact[n]['collection'].get('cause')=='operation_deadline' for n in ('stdout','stderr')),fact
            else: assert fact['original_group_absent'],fact
            if mode.startswith('escape'):
                causes={fact[n]['collection'].get('cause') for n in ('stdout','stderr')}
                assert ('tail_time' if mode=='escape-idle' else 'tail_bytes') in causes,fact
                for child in s.descendants:
                    assert not select.select([s.handles[child]],[],[],0)[0], 'escaped survivor falsely settled'
                if mode=='escape-flood':
                    until(lambda: (s.root/'survived0').exists() and (s.root/'survived1').exists(),'EPIPE survivors')
            elif mode!='unreaped': assert all(fact[n]['collection']['state']=='eof' for n in ('stdout','stderr')),fact
            print(json.dumps({'case':mode,'fact':fact,'hashes':[hashlib.sha256(x).hexdigest() for x in data]}),flush=True)
    print('PASS group modes: '+', '.join(modes),flush=True)


def transport_and_cancel():
    with Session(1,count=32,grace=1) as s:
        p,_=s.ready();p.kill();p.wait(timeout=5);s.go()
        until(lambda:(f if (f:=s.query()['output'])['first_stop']=='output_limit' and f['original_group_absent'] else None),'automatic detached stop')
        q=s.spawn({'offsets':{'stdout':0,'stderr':0}},force=True)
        h=wire.read_json(q.stdout.fileno());assert h['output']['first_stop']=='output_limit'
        data,fact,_=s.collect(q)
        assert sum(map(len,data))==1 and fact['leader_exit']=={'code':23}
        assert (s.root/'launches').read_bytes()==b'launch\n'
        print(json.dumps({'case':'automatic-overflow-controller-dead','fact':fact}),flush=True)
    with Session(1024,mode='manual',count=1024*1024,grace=1) as s:
        p,_=s.ready();s.go()
        cancel=socket.socket(socket.AF_UNIX);cancel.connect(str(s.path/'broker.sock'))
        cancel.sendall(b'{"action":"cancel-output-v1"}\n')
        until(lambda:s.query()['session']['cancel_state']=='running','manual owner started')
        # Actual controller process and both transports die before overflow.
        p.kill();p.wait(timeout=5);cancel.close()
        time.sleep(.65);start=time.monotonic();(s.root/'produce').touch()
        until(lambda:s.query()['output']['first_stop']=='output_limit','detached overflow')
        q=s.spawn({'offsets':{'stdout':0,'stderr':0}},force=True)
        h=wire.read_json(q.stdout.fileno());assert h['output']['first_stop']=='output_limit'
        _,fact,_=s.collect(q)
        assert time.monotonic()-start<.85,'manual grace was restarted'
        assert fact['leader_exit']=={'signal':9} and fact['original_group_absent']
        assert (s.root/'terms').read_bytes()==b'T','duplicate TERM owner'
        result=s.query('cancel-output-v1')
        assert result['outcome']=='already_exited' and result['output']['first_stop']=='output_limit',result
        assert (s.root/'launches').read_bytes()==b'launch\n'
        print(json.dumps({'case':'lost-controller-manual-before-overflow','fact':fact,'redelivery':result}),flush=True)
    print('PASS actual controller death/detached overflow/lost cancel reply/manual deadline/redelivery',flush=True)


def joined_tail_deadline():
    with Session(1024,mode='joined',count=1024,grace=1,settle=1) as s:
        p,_=s.ready()
        # Hold the original leader until the one manual owner starts; the
        # escaped writer supplies the first excess late in that operation.
        c=socket.socket(socket.AF_UNIX);c.connect(str(s.path/'broker.sock'))
        start=time.monotonic();c.sendall(b'{"action":"cancel-output-v1"}\n')
        until(lambda:s.query()['session']['cancel_state']=='running','manual owner')
        s.go();until(lambda:s.query()['output']['original_group_absent'],'latched original absence')
        c.close()
        remaining=1.91-(time.monotonic()-start)
        if remaining>0: time.sleep(remaining)
        (s.root/'children-go').touch()
        _,fact,_=s.collect(p)
        assert fact['first_stop']=='output_limit' and fact['leader_exit']=={'code':23},fact
        assert fact['original_group_absent'] and fact['tail_read']==0,fact
        assert any(fact[n]['collection'].get('cause')=='operation_deadline' for n in ('stdout','stderr')),fact
        assert time.monotonic()-start<2.16,'tail acquired a new finish deadline'
        assert not select.select([s.handles[s.descendants[0]]],[],[],0)[0]
        failed=s.query('cancel-output-v1')
        assert 'error' in failed and failed['output']['original_group_absent'],failed
        print(json.dumps({'case':'joined-tail-operation-deadline','fact':fact,'cancel':failed}),flush=True)
    print('PASS real older manual operation clamps tail and retains group absence',flush=True)


def storage():
    with tempfile.TemporaryDirectory(prefix='pk-bounds-spawn-error-') as root:
        root=Path(root);session=root/'session';session.mkdir()
        p=subprocess.Popen([BINARY,'__broker','--id','owned','--session-dir',str(session),
                            '--group-pidfd','--output-limit','1','--',str(root/'missing-command')],
                           stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        out,err=p.communicate(timeout=5)
        assert p.returncode==1 and b'cannot start command' in err
        assert session.is_dir() and not (session/'prelaunch').exists(), 'ambiguous dispatch removed retained session'
        print(json.dumps({'case':'failed-spawn-retains-dispatch-uncertainty','pid':p.pid,'actual_returncode':p.returncode,
                          'session_retained':True,'error':err.decode()}),flush=True)
    for prelaunch in (True,False):
        with Session(1024,count=32,grace=1) as s:
            s.path.mkdir()
            if prelaunch: (s.path/'stdout.buffer').mkdir()
            else: (s.path/'stdout.buffer').symlink_to('/dev/full')
            args=[BINARY,'__broker','--id','owned','--session-dir',str(s.path),
                  '--group-pidfd','--output-limit','1024','--',sys.executable,SCRIPT,
                  '--fixture',str(s.root),'finite','32']
            b=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                               env=s.env,start_new_session=True)
            s.proxies.append(b)
            if prelaunch:
                assert b.wait(timeout=5)==1
                error=b.stderr.read().decode()
                assert 'storage_open stdout' in error and 'workload not started' in error,error
                assert not (s.root/'ready').exists() and not (s.root/'launches').exists()
                assert (s.path/'prelaunch').exists()
                print(json.dumps({'case':'storage-open-prelaunch','actual_exit':b.returncode,'error':error,'launches':0}),flush=True)
            else:
                s.pin();p=s.spawn({'offsets':{'stdout':0,'stderr':0}})
                h=wire.read_json(p.stdout.fileno());assert h['attachment']==ATTACH
                s.go();_,fact,_=s.collect(p)
                assert fact['first_stop']=='storage_fault' and fact['stdout']['storage_fault']=='write',fact
                assert fact['stdout']['retained']==0 and fact['stdout']['reserved']==0
                assert fact['original_group_absent'] and fact['leader_exit']=={'code':23},fact
                assert s.query()['failure'] is None
                print(json.dumps({'case':'real-dev-full-write','fact':fact}),flush=True)
    for missing in (True,False):
        with Session(128,count=16,grace=1) as s:
            p,_=s.ready();s.go();out,initial,_=s.collect(p)
            assert out==[b'A'*16,b'B'*16] and initial['first_stop'] is None
            if missing: (s.path/'stdout.buffer').unlink()
            else: (s.path/'stdout.buffer').write_bytes(b'')
            q=s.spawn({'offsets':{'stdout':0,'stderr':0}},force=True)
            wire.read_json(q.stdout.fileno())
            try:
                while True:
                    kind,_,_=frame(q.stdout.fileno())
                    assert kind!=8,'replay fault emitted final success'
            except EOFError: pass
            assert q.wait(timeout=5)==1
            cause='replay_open' if missing else 'replay_read'
            fact=until(lambda:(p if (p:=s.query()['output'])['stdout']['storage_fault']==cause and p['original_group_absent'] else None),'retained replay fault')
            assert fact['stdout']['retained']==16 and fact['first_stop']=='storage_fault',fact
            assert not fact['stdout']['prefix_complete'] and fact['stdout']['collection']['state']=='eof'
            print(json.dumps({'case':cause,'fact':fact}),flush=True)
    print('PASS prelaunch no-launch/real write failure/replay open+read/retained positions',flush=True)


def retention_and_startup():
    with Session(128,count=16,grace=1) as s:
        s.env['PIPEKEEP_SESSION_TTL_SECS']='1'
        p,_=s.ready()
        time.sleep(1.15);assert s.path.exists() and p.poll() is None
        s.go();s.collect(p)
        control=socket.socket(socket.AF_UNIX);control.connect(str(s.path/'broker.sock'))
        time.sleep(1.15);assert s.path.exists(),'active control was disposed'
        control.close()
        until(lambda:not s.path.exists(),'finite idle lifetime',3)
        assert select.select([s.handles[s.broker]],[],[],3)[0]
        print(json.dumps({'case':'finite-retention','attachment_and_control_pinned':True,'directory_expired':True,'broker_pidfd_exit':True}),flush=True)
    import group_pidfd as faults
    for fault in ('startup-wait-echild','startup-wait-eio','startup-timeout'):
        with Session(128,count=0,grace=1) as s:
            s.fault=lambda:faults.seccomp(fault)
            p=s.spawn(creating=True);s.pin()
            out,err=p.communicate(timeout=8)
            assert p.returncode==1 and b'workload may have started' in out and b'do not retry creation' in out,(out,err)
            assert s.path.is_dir() and not (s.path/'prelaunch').exists()
            assert not select.select([s.handles[s.leader],s.handles[s.broker]],[],[],0)[0]
            s.fault=None
            retry=s.spawn(creating=True);out2,_=retry.communicate(timeout=5)
            assert retry.returncode==1 and b'already exists' in out2
            s.go();until(lambda:s.query()['leader_exit']=={'code':23},'actual leader after ambiguous startup')
            q=s.spawn({'offsets':{'stdout':0,'stderr':0}})
            h=wire.read_json(q.stdout.fileno());assert h['attachment']==ATTACH
            _,fact,_=s.collect(q)
            assert fact['leader_exit']=={'code':23} and (s.root/'launches').read_bytes()==b'launch\n'
            print(json.dumps({'case':fault,'opening_error':out.decode(),'fact':fact,'launches':1}),flush=True)
    print('PASS bounded active retention/idle expiry/startup ambiguity/no renewed creation',flush=True)


def malformed_handshakes():
    with Session(0,count=0,grace=1) as s:
        p,_=s.ready();s.go();_,valid,_=s.collect(p)
    cases=('old-attachment','missing-policy','missing-capability','legacy-exit','bad-accounting','truncated-zero')
    for case in cases:
        with tempfile.TemporaryDirectory(prefix='pk-bounds-handshake-') as root:
            path=Path(root)/hashlib.sha256(b'owned').hexdigest()[:32];path.mkdir()
            with socket.socket(socket.AF_UNIX) as listener:
                listener.bind(str(path/'broker.sock'));listener.listen();listener.settimeout(5)
                p=subprocess.Popen([BINARY,'--id','owned','--output-bounded','--','/bin/false'],
                                   stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                                   env=dict(os.environ,PIPEKEEP_RUNTIME_DIR=root),bufsize=0)
                try:
                    wire.write_bytes(p.stdin.fileno(),b'{"offsets":{"stdout":0,"stderr":0}}\n')
                    sock,_=listener.accept()
                    with sock:
                        request=wire.read_json(sock.fileno());assert request['action']=='attach-output-v1'
                        fact=json.loads(json.dumps(valid))
                        hello={'attachment':ATTACH,'offsets':{'stdin':0,'stdout':0,'stderr':0},'output':fact,
                               'session':{'capabilities':['group-pidfd-cancel-v1',CAP],'workload_started':True,'cancel_state':'settled'}}
                        if case=='old-attachment': hello['attachment']='framed-v1'
                        if case=='missing-policy': del hello['output']
                        if case=='missing-capability': hello['session']['capabilities']=[]
                        if case=='bad-accounting': fact['stdout']['retained']=1
                        sock.sendall(json.dumps(hello).encode()+b'\n')
                        if case in ('legacy-exit','truncated-zero'):
                            wire.read_json(p.stdout.fileno())
                            if case=='legacy-exit': kind,payload=6,json.dumps({'code':0}).encode()
                            else:
                                fact['leader_exit']={'code':0};fact['first_stop']='output_limit';fact['stdout']['prefix_complete']=False
                                kind,payload=8,json.dumps(fact).encode()
                            sock.sendall(bytes([kind])+struct.pack('>I',len(payload))+payload)
                            if case=='truncated-zero': assert frame(p.stdout.fileno())[0]==8
                        assert p.wait(timeout=5)==1,case
                        sock.settimeout(1)
                        try: assert sock.recv(1)==b'','invalid broker handshake forwarded stdin'
                        except ConnectionResetError: pass  # rejected unread terminal may reset transport
                finally:
                    if p.poll() is None:p.kill()
                    status=p.wait(timeout=5)
                    for stream in (p.stdin,p.stdout,p.stderr):stream.close()
                    print(json.dumps({'case':case,'frontend_wait_pid':p.pid,'actual_returncode':status,'launches':0}),flush=True)
    print('PASS affirmative policy handshakes/new terminal validation/legacy Exit rejection',flush=True)


def receipts():
    with Session(32768,mode='stream',count=0) as s:
        p,_=s.ready();s.go()
        data=wire.pattern(0,16384)
        wire.write_bytes(p.stdin.fileno(),wire.pack(1,0,data)+wire.pack(2,len(data)))
        out,fact,_=s.collect(p)
        assert out==[data,data.translate(wire.XOR)] and fact['first_stop'] is None
        assert json.loads((s.root/'input').read_text())==len(data)
        q=s.spawn({'offsets':{'stdout':8192,'stderr':4096},'stdin_eof':16384},force=True)
        h=wire.read_json(q.stdout.fileno());assert h['offsets']['stdin']==16384 and h['stdin_eof_at']==16384
        out2,_,_=s.collect(q,(8192,4096));assert out2==[data[8192:],data.translate(wire.XOR)[4096:]]
    with Session(17,mode='stream',count=0) as s:
        p,_=s.ready();s.go()
        wire.write_bytes(p.stdin.fileno(),wire.pack(1,0,b'x'*2048)+wire.pack(2,2048))
        out,fact,_=s.collect(p);assert sum(map(len,out))==17 and fact['first_stop']=='output_limit'
    print('PASS bounded framed input/EOF/receipts/independent tails/output boundary',flush=True)


def compatibility():
    peers=[BINARY]+[os.environ[k] for k in ('PIPEKEEP_RECEIPTS_OLD_BINARY','PIPEKEEP_LEGACY_BINARY') if k in os.environ]
    if os.environ.get('PIPEKEEP_REQUIRE_OUTPUT_OLD'): assert len(peers)==3,'both actual old peers required'
    with Session(8192,mode='stream') as s:
        p,_=s.ready();s.go()
        before=s.query()
        for binary in peers:
            for extra in ((),('--framed',)):
                q=s.spawn({'offsets':{'stdout':0,'stderr':0},'stdin_eof':0},bounded=False,force=True,binary=binary,extra=extra)
                out,err=q.communicate(timeout=5)
                assert q.returncode==1,(out,err)
                assert s.query()['output']['first_stop'] is None
                assert s.query()['leader_exit'] is None
        for action in ('attach','attach-framed-v1','stdin-eof','cancel','cancel-group-pidfd','attach-output-v999'):
            try: result=s.query(action,force=True,offset=0,stdin_eof=0)
            except EOFError: continue
            assert 'error' in result,result
        wire.write_bytes(p.stdin.fileno(),wire.pack(1,0,b'good')+wire.pack(2,4))
        out,fact,_=s.collect(p);assert out==[b'good',b'good'.translate(wire.XOR)] and fact['first_stop'] is None
        assert s.query()['pid']==before['pid']
    # Public malformed creation must not allocate or launch; new flags always
    # follow recognized --id for all invocations accepted by the new parser.
    for binary in peers:
        with tempfile.TemporaryDirectory(prefix='pk-bounds-options-') as root:
            env=dict(os.environ,PIPEKEEP_RUNTIME_DIR=root)
            options=[['--output-bounded','--group-pidfd','--output-limit',n] for n in ('-1','+1','1k','18446744073709551616','9223372036854775808')]
            options += [['--output-bounded'],['--output-bounded','--output-limit','2'],['--output-bounded','--group-pidfd','--output-limit','2','--nobuffer']]
            if binary!=BINARY: options += [['--output-bounded','--group-pidfd','--output-limit','2']]
            for flags in options:
                result=subprocess.run([binary,'--id','bad']+flags+['--','/bin/false'],input=b'{}\n',capture_output=True,env=env,timeout=4)
                assert result.returncode==1 and not list(Path(root).iterdir()),(flags,result.stdout,result.stderr)
    # Actual old broker rejects new distinct action without installing a lease.
    for binary in peers:
        with Session(8192,mode='stream') as s:
            p=s.spawn(creating=False,bounded=False,binary=binary);s.pin();s.go()
            h=wire.read_json(p.stdout.fileno());assert 'output' not in h
            q=s.spawn({'offsets':{'stdout':0,'stderr':0},'stdin_eof':0},force=True)
            out,err=q.communicate(timeout=5);assert q.returncode==1,(out,err)
            assert s.query('pid')['pid']==s.leader
            # Original raw frontend remains installed and stdin remains open.
            wire.write_bytes(p.stdin.fileno(),b'old')
            until(lambda:(s.path/'stdout.buffer').stat().st_size==3,'old broker input unaffected')
    print('PASS old/new action/flag/force fencing before stdin or lease mutation',flush=True)


if __name__=='__main__':
    if sys.argv[1]=='--fixture': fixture(sys.argv[2],sys.argv[3],int(sys.argv[4]));raise AssertionError('fixture returned')
    BINARY=str(Path(sys.argv[1]).resolve())
    assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
    boundaries();groups();transport_and_cancel();joined_tail_deadline();storage();retention_and_startup();malformed_handshakes();receipts();compatibility()
    try: os.waitpid(-1,os.WNOHANG)
    except ChildProcessError: pass
    else: raise AssertionError('fixture remains')
    print('FINITE OUTPUT MATRIX PASSED; exact cleanup verified ECHILD',flush=True)

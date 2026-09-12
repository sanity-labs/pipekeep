//! One collector coordinator, two charged blocking writes, one serialized replay.
use super::*;
use crate::output::{
    Collection, CollectionCause, StopReason, StorageFault, StreamFact, READ_SIZE, TAIL_BYTES,
    TAIL_MILLIS,
};
use std::io::Write;
use tokio::time::Instant;

type WriteTask = tokio::task::JoinHandle<fs::File>;

fn stream_fact(policy: &mut OutputFact, stream: Stream) -> &mut StreamFact {
    match stream {
        Stream::Stdout => &mut policy.stdout,
        Stream::Stderr => &mut policy.stderr,
    }
}

pub(super) fn fact(shared: &Shared, meta: &Meta) -> Option<OutputFact> {
    meta.output.clone().map(|mut p| {
        p.leader_exit = meta.status.clone();
        let cancel = shared
            .cancellation
            .as_ref()
            .expect("bounded cancellation")
            .lock()
            .expect("cancellation poisoned");
        p.original_group_absent = cancel.group_absent();
        p.group_control = cancel.output_control();
        drop(cancel);
        for stream in [&mut p.stdout, &mut p.stderr] {
            stream.prefix_complete &=
                stream.collection == Collection::Eof && stream.reserved == 0 && p.io_inflight == 0;
        }
        p.sealed = p.io_inflight == 0 && (p.first_stop.is_some() || p.collected());
        p
    })
}
pub(super) fn snapshot(shared: &Shared) -> Option<OutputFact> {
    fact(shared, &shared.meta.lock().expect("metadata poisoned"))
}

pub(super) fn stop(shared: &Arc<Shared>, reason: StopReason) {
    {
        let mut meta = shared.meta.lock().expect("metadata poisoned");
        meta.output
            .as_mut()
            .expect("bounded policy")
            .first_stop
            .get_or_insert(reason);
    }
    // Synchronous admission into the same operation before returning. Neither
    // storage completion, the collector nor any connection owns its lifetime.
    start_hardened(shared);
    shared.changed.notify_waiters();
}

fn storage_fault(shared: &Arc<Shared>, stream: Stream, fault: StorageFault) {
    {
        let mut meta = shared.meta.lock().expect("metadata poisoned");
        let p = meta.output.as_mut().unwrap();
        let s = stream_fact(p, stream);
        s.storage_fault.get_or_insert(fault);
        s.prefix_complete = false;
        p.first_stop.get_or_insert(StopReason::StorageFault);
    }
    start_hardened(shared);
    shared.changed.notify_waiters();
}

// std::File writes return actual completed counts; Tokio File::write can merely
// accept bytes into a pending blocking operation. Publish only these counts.
// No future cancellation can drop this worker's file, reservation or I/O pin.
fn write_prefix<W: Write>(file: &mut W, data: &[u8], stream: Stream, shared: &Arc<Shared>) {
    let mut written = 0;
    let mut fault = None;
    while written < data.len() {
        match file.write(&data[written..]) {
            Ok(0) | Err(_) => {
                fault = Some(if written == 0 {
                    StorageFault::Write
                } else {
                    StorageFault::PartialWrite
                });
                break;
            }
            Ok(n) => {
                written += n;
                let mut meta = shared.meta.lock().expect("metadata poisoned");
                let p = meta.output.as_mut().unwrap();
                let s = stream_fact(p, stream);
                s.reserved = s.reserved.checked_sub(n as u64).expect("charged write");
                s.retained = s.retained.checked_add(n as u64).expect("bounded position");
                let end = s.retained;
                p.available();
                match stream {
                    Stream::Stdout => meta.stdout_end = end,
                    Stream::Stderr => meta.stderr_end = end,
                }
                drop(meta);
                shared.changed.notify_waiters();
            }
        }
    }
    if fault.is_none() && file.flush().is_err() {
        fault = Some(StorageFault::Flush);
    }
    // Only a returned error/completion releases unused reservation. A stuck
    // syscall retains the entire still outstanding portion and I/O pin.
    {
        let mut meta = shared.meta.lock().expect("metadata poisoned");
        let p = meta.output.as_mut().unwrap();
        let s = stream_fact(p, stream);
        s.reserved = s
            .reserved
            .checked_sub((data.len() - written) as u64)
            .expect("unused reservation");
        if written != data.len() {
            s.discarded += (data.len() - written) as u64;
            s.prefix_complete = false;
        }
        if fault.is_some() {
            p.first_stop.get_or_insert(StopReason::StorageFault);
        }
        // Keep the pin until fault admission has also completed.
    }
    if let Some(fault) = fault {
        storage_fault(shared, stream, fault);
    }
    shared
        .meta
        .lock()
        .expect("metadata poisoned")
        .output
        .as_mut()
        .unwrap()
        .io_inflight -= 1;
    shared.changed.notify_waiters();
}

fn admit(file: fs::File, data: Vec<u8>, stream: Stream, shared: Arc<Shared>) -> WriteTask {
    // Reservation and pin are installed by the coordinator before spawning.
    tokio::task::spawn_blocking(move || {
        let mut file = file;
        write_prefix(&mut file, &data, stream, &shared);
        file
    })
}

fn close_collection(shared: &Arc<Shared>, cause: CollectionCause) {
    let mut meta = shared.meta.lock().expect("metadata poisoned");
    let p = meta.output.as_mut().unwrap();
    for s in [&mut p.stdout, &mut p.stderr] {
        if s.collection == Collection::Reading {
            s.collection = Collection::Unconfirmed(cause);
        }
    }
    drop(meta);
    shared.changed.notify_waiters();
}

fn observed(
    shared: &Arc<Shared>,
    stream: Stream,
    result: std::io::Result<usize>,
    tail: bool,
) -> usize {
    let mut meta = shared.meta.lock().expect("metadata poisoned");
    let p = meta.output.as_mut().unwrap();
    match result {
        Ok(0) => {
            stream_fact(p, stream).collection = Collection::Eof;
            match stream {
                Stream::Stdout => meta.stdout_closed = true,
                Stream::Stderr => meta.stderr_closed = true,
            }
            shared.announce_if_terminal(&meta);
        }
        Ok(count) => {
            if tail {
                p.tail_read += count as u64; // checked against shared cap before each read
                let s = stream_fact(p, stream);
                s.discarded += count as u64;
                s.prefix_complete = false;
            }
            return count;
        }
        Err(_) => {
            let s = stream_fact(p, stream);
            s.collection = Collection::Unconfirmed(CollectionCause::OutputRead);
            s.prefix_complete = false;
            p.first_stop.get_or_insert(StopReason::OutputReadFault);
            drop(meta);
            stop(shared, StopReason::OutputReadFault);
            return 0;
        }
    }
    drop(meta);
    shared.changed.notify_waiters();
    0
}

async fn await_write(
    task: &mut Option<WriteTask>,
) -> std::result::Result<fs::File, tokio::task::JoinError> {
    match task {
        Some(task) => task.await,
        None => std::future::pending().await,
    }
}
async fn read_pipe<R: AsyncRead + Unpin>(
    reader: &mut Option<R>,
    buffer: &mut [u8],
    enabled: bool,
) -> std::io::Result<usize> {
    if enabled {
        reader.as_mut().expect("enabled pipe").read(buffer).await
    } else {
        std::future::pending().await
    }
}

pub(super) async fn collect<O: AsyncRead + Unpin, E: AsyncRead + Unpin>(
    mut stdout: Option<O>,
    mut stderr: Option<E>,
    stdout_file: fs::File,
    stderr_file: fs::File,
    shared: Arc<Shared>,
) {
    let mut files = [Some(stdout_file), Some(stderr_file)];
    let mut writes = [None, None];
    let mut out = [0; READ_SIZE];
    let mut err = [0; READ_SIZE];
    let mut prefer_stdout = true;
    let mut tail_deadline = None;
    for (missing, stream) in [
        (stdout.is_none(), Stream::Stdout),
        (stderr.is_none(), Stream::Stderr),
    ] {
        if missing {
            let mut meta = shared.meta.lock().expect("metadata poisoned");
            stream_fact(meta.output.as_mut().unwrap(), stream).collection =
                Collection::Unconfirmed(CollectionCause::PipeSetup);
            drop(meta);
            stop(&shared, StopReason::PipeSetupFault);
        }
    }
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let p = snapshot(&shared).unwrap();
        if p.collected() && writes.iter().all(Option::is_none) {
            return;
        }
        let now = Instant::now();
        let (absent, finished, finish) = {
            let c = shared
                .cancellation
                .as_ref()
                .unwrap()
                .lock()
                .expect("cancellation poisoned");
            (c.group_absent(), c.finished(), c.finish_deadline())
        };
        let stopped = p.first_stop.is_some();
        if finish.is_some_and(|end| now >= end) {
            close_collection(
                &shared,
                if p.io_inflight != 0 {
                    CollectionCause::StoragePending
                } else {
                    CollectionCause::OperationDeadline
                },
            );
            return; // outstanding blocking workers still own files, charge and pin
        }
        // Acquisition can fail before any operation starts. Keep collecting
        // within budget in that case; unavailable control alone is not a stop.
        if finished && !absent && (stopped || finish.is_some()) {
            close_collection(&shared, CollectionCause::GroupUnconfirmed);
            return;
        }
        if stopped && absent && writes.iter().all(Option::is_none) && tail_deadline.is_none() {
            tail_deadline =
                Some((now + Duration::from_millis(TAIL_MILLIS)).min(finish.unwrap_or(now)));
        }
        let tail = stopped && tail_deadline.is_some();
        if tail {
            if now >= tail_deadline.unwrap() {
                close_collection(&shared, CollectionCause::TailTime);
                return;
            }
            if p.tail_read == TAIL_BYTES {
                close_collection(&shared, CollectionCause::TailBytes);
                return;
            }
        }
        let read_size = if tail {
            (TAIL_BYTES - p.tail_read).min(READ_SIZE as u64) as usize
        } else {
            p.available().saturating_add(1).min(READ_SIZE as u64) as usize
        };
        let reading = !stopped || tail;
        let out_enabled = reading
            && p.stdout.collection == Collection::Reading
            && (writes[0].is_none() || (!tail && p.available() == 0));
        let err_enabled = reading
            && p.stderr.collection == Collection::Reading
            && (writes[1].is_none() || (!tail && p.available() == 0));
        let deadline = tail_deadline.or(finish);
        enum Event {
            Write(usize, std::result::Result<fs::File, tokio::task::JoinError>),
            Read(Stream, std::io::Result<usize>),
            Wake,
        }
        let (left, right) = writes.split_at_mut(1);
        let event = tokio::select! {
            biased;
            r = await_write(&mut left[0]) => Event::Write(0, r),
            r = await_write(&mut right[0]) => Event::Write(1, r),
            _ = &mut changed => Event::Wake,
            _ = async { match deadline { Some(end) => tokio::time::sleep_until(end).await, None => std::future::pending().await } } => Event::Wake,
            r = async {
                let out_read = read_pipe(&mut stdout, &mut out[..read_size], out_enabled);
                let err_read = read_pipe(&mut stderr, &mut err[..read_size], err_enabled);
                tokio::pin!(out_read, err_read);
                if prefer_stdout {
                    tokio::select! { biased; r = out_read => (Stream::Stdout, r), r = err_read => (Stream::Stderr, r) }
                } else {
                    tokio::select! { biased; r = err_read => (Stream::Stderr, r), r = out_read => (Stream::Stdout, r) }
                }
            } => Event::Read(r.0, r.1),
        };
        match event {
            Event::Wake => {}
            Event::Write(i, result) => {
                writes[i] = None;
                match result {
                    Ok(file) => files[i] = Some(file),
                    Err(_) => {
                        storage_fault(
                            &shared,
                            if i == 0 {
                                Stream::Stdout
                            } else {
                                Stream::Stderr
                            },
                            StorageFault::WorkerLost,
                        );
                    }
                }
            }
            Event::Read(stream, result) => {
                let n = observed(&shared, stream, result, tail);
                prefer_stdout = matches!(stream, Stream::Stderr);
                if n != 0 && !tail {
                    let retained = {
                        let mut meta = shared.meta.lock().expect("metadata poisoned");
                        let p = meta.output.as_mut().unwrap();
                        // Recheck under the reservation lock: a blocking worker
                        // may have sealed admission after the read was selected.
                        let retained = if p.first_stop.is_some() {
                            0
                        } else {
                            (n as u64).min(p.available()) as usize
                        };
                        let s = stream_fact(p, stream);
                        s.reserved += retained as u64;
                        s.discarded += (n - retained) as u64;
                        if n != retained {
                            s.prefix_complete = false;
                            p.first_stop.get_or_insert(StopReason::OutputLimit);
                        }
                        if retained != 0 {
                            p.io_inflight += 1;
                        }
                        p.available();
                        retained
                    };
                    if retained != n {
                        stop(&shared, StopReason::OutputLimit);
                    }
                    if retained != 0 {
                        let (i, buffer) = match stream {
                            Stream::Stdout => (0, &out),
                            Stream::Stderr => (1, &err),
                        };
                        writes[i] = Some(admit(
                            files[i].take().unwrap(),
                            buffer[..retained].to_vec(),
                            stream,
                            shared.clone(),
                        ));
                    }
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

async fn replay(shared: Arc<Shared>, stream: Stream, offset: u64, end: u64) -> Result<Vec<u8>> {
    // A canceled attachment cannot create another blocking read until this one
    // actually returns. The worker holds the gate and the lifetime pin.
    let gate = shared.replay_gate.clone().lock_owned().await;
    shared
        .meta
        .lock()
        .expect("metadata poisoned")
        .output
        .as_mut()
        .unwrap()
        .io_inflight += 1;
    let result = tokio::task::spawn_blocking(move || {
        let _gate = gate;
        let path = match stream {
            Stream::Stdout => &shared.stdout_path,
            Stream::Stderr => &shared.stderr_path,
        };
        let result = match fs::File::open(path) {
            Ok(file) => read_at(&file, offset, end).map_err(|_| StorageFault::ReplayRead),
            Err(_) => Err(StorageFault::ReplayOpen),
        };
        if let Err(fault) = result {
            storage_fault(&shared, stream, fault);
        }
        shared
            .meta
            .lock()
            .expect("metadata poisoned")
            .output
            .as_mut()
            .unwrap()
            .io_inflight -= 1;
        shared.changed.notify_waiters();
        result
    })
    .await?;
    result.map_err(|fault| anyhow::anyhow!("bounded replay fault: {fault:?}; session retained"))
}

pub(super) async fn send_output<W: AsyncWrite + Unpin>(
    mut writer: W,
    shared: Arc<Shared>,
    generation: u64,
    mut stdout: u64,
    mut stderr: u64,
    mut ack: watch::Receiver<u64>,
) -> Result<()> {
    let mut last = Vec::new();
    let mut prefer_stdout = true;
    loop {
        if !shared.is_current_attachment(generation) {
            return Ok(());
        }
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let p = snapshot(&shared).unwrap();
        let bytes = serde_json::to_vec(&p)?;
        if bytes != last {
            write_frame(&mut writer, &Frame::OutputState(p.clone())).await?;
            last = bytes;
        }
        if ack.has_changed().unwrap_or(false) {
            let offset = *ack.borrow_and_update();
            write_frame(&mut writer, &Frame::StdinPosition { offset }).await?;
        }
        let stream = if stdout < p.stdout.retained && (prefer_stdout || stderr == p.stderr.retained)
        {
            Some(Stream::Stdout)
        } else if stderr < p.stderr.retained {
            Some(Stream::Stderr)
        } else {
            None
        };
        if let Some(stream) = stream {
            let (position, end) = match stream {
                Stream::Stdout => (&mut stdout, p.stdout.retained),
                Stream::Stderr => (&mut stderr, p.stderr.retained),
            };
            let data = replay(shared.clone(), stream, *position, end).await?;
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            let n = data.len() as u64;
            let frame = match stream {
                Stream::Stdout => Frame::StdoutData {
                    offset: *position,
                    data,
                },
                Stream::Stderr => Frame::StderrData {
                    offset: *position,
                    data,
                },
            };
            write_frame(&mut writer, &frame).await?;
            *position += n;
            prefer_stdout = matches!(stream, Stream::Stderr);
            continue;
        }
        let cancel_finished = shared
            .cancellation
            .as_ref()
            .unwrap()
            .lock()
            .expect("cancellation poisoned")
            .finished();
        if p.collected()
            && p.io_inflight == 0
            && (p.leader_exit.is_some() || cancel_finished)
            && (p.first_stop.is_none() || p.original_group_absent || cancel_finished)
        {
            write_frame(&mut writer, &Frame::OutputEnd(p)).await?;
            return Ok(());
        }
        tokio::select! { _ = changed => {}, _ = ack.changed() => { ack.mark_changed(); } }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32};

    struct Kernel {
        exists: AtomicBool,
        terms: AtomicU32,
        kills: AtomicU32,
    }
    struct Handle(Arc<Kernel>);
    impl GroupSignal for Handle {
        fn signal(&self, sig: i32) -> std::io::Result<bool> {
            if sig == libc::SIGTERM {
                self.0.terms.fetch_add(1, Ordering::SeqCst);
            }
            if sig == libc::SIGKILL {
                self.0.kills.fetch_add(1, Ordering::SeqCst);
                self.0.exists.store(false, Ordering::SeqCst);
            }
            Ok(self.0.exists.load(Ordering::SeqCst))
        }
    }
    fn fixture(limit: u64, exists: bool) -> (tempfile::TempDir, Arc<Shared>, Arc<Kernel>) {
        let dir = tempfile::tempdir().unwrap();
        let kernel = Arc::new(Kernel {
            exists: AtomicBool::new(exists),
            terms: AtomicU32::new(0),
            kills: AtomicU32::new(0),
        });
        let (terminal, _) = watch::channel(false);
        let (stdout_live, _) = broadcast::channel(1);
        let (stderr_live, _) = broadcast::channel(1);
        let shared = Arc::new(Shared {
            meta: StdMutex::new(Meta {
                output: Some(OutputFact::new(limit)),
                status: Some(ExitResult {
                    code: Some(23),
                    signal: None,
                }),
                ..Meta::default()
            }),
            replay_gate: Arc::new(Mutex::new(())),
            output_bounded: true,
            child_stdin: Mutex::new(None),
            changed: Notify::new(),
            terminal,
            stdout_live,
            stderr_live,
            stdout_path: dir.path().join("stdout"),
            stderr_path: dir.path().join("stderr"),
            command_pid: 0,
            cancellation: Some(StdMutex::new(Cancellation::verify_acquired(Ok(Box::new(
                Handle(kernel.clone()),
            ))))),
            nobuffer: false,
            attachment: StdMutex::new(AttachmentState {
                next_generation: 1,
                current: None,
            }),
            active_attachments: AtomicUsize::new(0),
        });
        (dir, shared, kernel)
    }
    fn charge(shared: &Shared, stream: Stream, n: u64) {
        let mut meta = shared.meta.lock().unwrap();
        let p = meta.output.as_mut().unwrap();
        assert!(p.available() >= n);
        stream_fact(p, stream).reserved += n;
        p.io_inflight += 1;
        p.available();
    }
    async fn absent(shared: &Shared) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !snapshot(shared).unwrap().original_group_absent {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    struct FaultWriter {
        bytes: Vec<u8>,
        cap: usize,
        flush_fails: bool,
    }
    impl Write for FaultWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let n = bytes.len().min(self.cap - self.bytes.len());
            if n == 0 {
                return Err(std::io::Error::other("private fault"));
            }
            self.bytes.extend_from_slice(&bytes[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            if self.flush_fails {
                Err(std::io::Error::other("private flush"))
            } else {
                Ok(())
            }
        }
    }
    #[tokio::test]
    async fn actual_partial_write_write_and_flush_positions_are_distinct() {
        for (cap, flush_fails, fault) in [
            (0, false, StorageFault::Write),
            (3, false, StorageFault::PartialWrite),
            (8, true, StorageFault::Flush),
        ] {
            let (_dir, shared, _) = fixture(8, false);
            start_hardened_with(&shared, Duration::ZERO, Duration::from_millis(100));
            charge(&shared, Stream::Stdout, 8);
            let mut writer = FaultWriter {
                bytes: Vec::new(),
                cap,
                flush_fails,
            };
            write_prefix(&mut writer, b"abcdefgh", Stream::Stdout, &shared);
            absent(&shared).await;
            let p = snapshot(&shared).unwrap();
            assert_eq!(writer.bytes, b"abcdefgh"[..cap]);
            assert_eq!(p.stdout.retained, cap as u64);
            assert_eq!(p.stdout.reserved, 0);
            assert_eq!(p.stdout.storage_fault, Some(fault));
            assert_eq!(p.first_stop, Some(StopReason::StorageFault));
            assert_eq!(p.io_inflight, 0);
            assert!(!p.stdout.prefix_complete);
            assert!(shared.meta.lock().unwrap().failure.is_none());
            observed(
                &shared,
                Stream::Stderr,
                Err(std::io::Error::other("later read")),
                false,
            );
            assert_eq!(
                snapshot(&shared).unwrap().first_stop,
                Some(StopReason::StorageFault)
            );
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_uncancelled_blocking_writes_stay_charged_while_owner_kills() {
        let (_dir, shared, kernel) = fixture(8, true);
        let (release_tx, release_rx) = watch::channel(false);
        let mut workers = Vec::new();
        for stream in [Stream::Stdout, Stream::Stderr] {
            charge(&shared, stream, 4);
            let shared = shared.clone();
            let mut rx = release_rx.clone();
            workers.push(tokio::task::spawn_blocking(move || {
                // Private barrier simulates a filesystem syscall not returning.
                tokio::runtime::Handle::current().block_on(async {
                    while !*rx.borrow() {
                        rx.changed().await.unwrap();
                    }
                });
                let mut bytes = Vec::new();
                write_prefix(&mut bytes, b"abcd", stream, &shared);
                bytes
            }));
        }
        assert_eq!(snapshot(&shared).unwrap().available(), 0);
        start_hardened_with(
            &shared,
            Duration::from_millis(20),
            Duration::from_millis(100),
        );
        stop(&shared, StopReason::OutputLimit);
        absent(&shared).await;
        let p = snapshot(&shared).unwrap();
        assert_eq!(
            (
                p.stdout.retained,
                p.stderr.retained,
                p.stdout.reserved,
                p.stderr.reserved
            ),
            (0, 0, 4, 4)
        );
        assert_eq!(p.io_inflight, 2);
        assert!(!p.sealed);
        assert_eq!(kernel.terms.load(Ordering::SeqCst), 1);
        assert_eq!(kernel.kills.load(Ordering::SeqCst), 1);
        assert!(!start_hardened_with(
            &shared,
            Duration::from_secs(99),
            Duration::from_secs(99)
        ));
        release_tx.send_replace(true);
        for worker in workers {
            assert_eq!(worker.await.unwrap(), b"abcd");
        }
        let p = snapshot(&shared).unwrap();
        assert_eq!(
            (
                p.stdout.retained,
                p.stderr.retained,
                p.available(),
                p.io_inflight
            ),
            (4, 4, 0, 0)
        );
        assert!(p.sealed && p.original_group_absent);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_replay_future_cannot_cancel_blocking_io_or_release_lifetime_pin() {
        use std::os::unix::ffi::OsStrExt;
        let (_dir, shared, _) = fixture(4, true);
        let path = std::ffi::CString::new(shared.stdout_path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let first = tokio::spawn(replay(shared.clone(), Stream::Stdout, 0, 4));
        tokio::time::timeout(Duration::from_secs(1), async {
            while snapshot(&shared).unwrap().io_inflight == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second = tokio::spawn(replay(shared.clone(), Stream::Stdout, 0, 4));
        tokio::task::yield_now().await;
        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        assert_eq!(snapshot(&shared).unwrap().io_inflight, 1);
        start_hardened_with(
            &shared,
            Duration::from_millis(10),
            Duration::from_millis(60),
        );
        stop(&shared, StopReason::OutputLimit);
        absent(&shared).await;
        close_collection(&shared, CollectionCause::StoragePending);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (busy, eligible) = hardened_lifetime_state(&shared);
        assert!(
            busy && eligible,
            "completed cancel cannot dispose unresolved I/O"
        );
        assert!(!snapshot(&shared).unwrap().sealed);
        // This exact-owned FIFO writer lets the outstanding kernel open return.
        let path = shared.stdout_path.clone();
        tokio::task::spawn_blocking(move || fs::OpenOptions::new().write(true).open(path).unwrap())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while snapshot(&shared).unwrap().io_inflight != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let p = snapshot(&shared).unwrap();
        assert_eq!(p.stdout.storage_fault, Some(StorageFault::ReplayRead));
        assert_eq!(p.first_stop, Some(StopReason::OutputLimit));
        assert!(p.original_group_absent && p.sealed);
        assert!(!hardened_lifetime_state(&shared).0);
    }
    #[tokio::test]
    async fn real_replay_open_and_short_read_failures_latch_without_generic_failure() {
        for fault in [StorageFault::ReplayOpen, StorageFault::ReplayRead] {
            let (_dir, shared, _) = fixture(4, false);
            if fault == StorageFault::ReplayRead {
                fs::write(&shared.stdout_path, b"").unwrap();
            }
            shared
                .meta
                .lock()
                .unwrap()
                .output
                .as_mut()
                .unwrap()
                .stdout
                .retained = 4;
            start_hardened_with(&shared, Duration::ZERO, Duration::from_millis(100));
            assert!(replay(shared.clone(), Stream::Stdout, 0, 4).await.is_err());
            absent(&shared).await;
            let p = snapshot(&shared).unwrap();
            assert_eq!(p.stdout.storage_fault, Some(fault));
            assert_eq!(p.stdout.retained, 4); // recorded boundary never resets on failed replay
            assert_eq!(p.first_stop, Some(StopReason::StorageFault));
            assert_eq!(p.io_inflight, 0);
            assert!(shared.meta.lock().unwrap().failure.is_none());
        }
    }
    struct FailingRead;
    impl AsyncRead for FailingRead {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("private output read fault")))
        }
    }
    #[tokio::test]
    async fn collector_read_fault_is_not_eof_and_group_fact_survives_deadline() {
        let (_dir, shared, _) = fixture(4, false);
        start_hardened_with(&shared, Duration::ZERO, Duration::from_millis(100));
        collect(
            Some(FailingRead),
            Some(tokio::io::empty()),
            fs::File::create(&shared.stdout_path).unwrap(),
            fs::File::create(&shared.stderr_path).unwrap(),
            shared.clone(),
        )
        .await;
        absent(&shared).await;
        tokio::time::sleep(Duration::from_millis(130)).await;
        let p = snapshot(&shared).unwrap();
        assert_eq!(p.first_stop, Some(StopReason::OutputReadFault));
        assert_eq!(
            p.stdout.collection,
            Collection::Unconfirmed(CollectionCause::OutputRead)
        );
        assert_eq!(p.stderr.collection, Collection::Eof);
        assert!(p.original_group_absent);
        assert_eq!(p.leader_exit.unwrap().code, Some(23));
        assert!(!shared.meta.lock().unwrap().stdout_closed);
        assert!(shared
            .cancellation
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .is_err());
    }
    #[tokio::test]
    async fn tail_at_exact_cap_is_unconfirmed_and_one_extra_byte_allows_eof() {
        for count in [TAIL_BYTES, TAIL_BYTES - 1] {
            let (_dir, shared, _) = fixture(0, false);
            start_hardened_with(&shared, Duration::ZERO, Duration::from_secs(1));
            stop(&shared, StopReason::OutputLimit);
            let data = vec![b'x'; count as usize];
            collect(
                Some(data.as_slice()),
                Some(tokio::io::empty()),
                fs::File::create(&shared.stdout_path).unwrap(),
                fs::File::create(&shared.stderr_path).unwrap(),
                shared.clone(),
            )
            .await;
            let p = snapshot(&shared).unwrap();
            assert_eq!(p.tail_read, count);
            assert_eq!(
                p.stdout.collection,
                if count == TAIL_BYTES {
                    Collection::Unconfirmed(CollectionCause::TailBytes)
                } else {
                    Collection::Eof
                }
            );
            assert!(p.original_group_absent);
        }
    }
    #[tokio::test]
    async fn old_operation_deadline_caps_tail_and_preserves_absence() {
        let (_dir, shared, _) = fixture(0, false);
        start_hardened_with(&shared, Duration::ZERO, Duration::from_millis(120));
        absent(&shared).await;
        let original = shared
            .cancellation
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .finish_deadline();
        tokio::time::sleep(Duration::from_millis(80)).await;
        stop(&shared, StopReason::OutputLimit);
        let (_writer, reader) = tokio::io::duplex(1);
        collect(
            Some(reader),
            Some(tokio::io::empty()),
            fs::File::create(&shared.stdout_path).unwrap(),
            fs::File::create(&shared.stderr_path).unwrap(),
            shared.clone(),
        )
        .await;
        let p = snapshot(&shared).unwrap();
        assert!(p.original_group_absent);
        assert_eq!(
            p.stdout.collection,
            Collection::Unconfirmed(CollectionCause::OperationDeadline)
        );
        assert_eq!(
            shared
                .cancellation
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .finish_deadline(),
            original
        );
    }
}

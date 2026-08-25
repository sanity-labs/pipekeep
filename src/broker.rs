use crate::protocol::{
    read_frame, read_line, write_error_line, write_frame, write_json_line, AllOffsets,
    BrokerRequest, CancelOutcome, ExitResult, Frame, OutputOffsets, ServerHello,
};
use crate::runtime;
use anyhow::{bail, Context, Result};
use nix::errno::Errno;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::fs;
use std::future::poll_fn;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{ChildStdin, Command};
use tokio::sync::{broadcast, mpsc, watch, Mutex, Notify};

const CHUNK_SIZE: usize = 32 * 1024;

#[derive(Clone, Debug)]
struct OutputChunk {
    offset: u64,
    data: Vec<u8>,
}

struct AttachmentState {
    next_generation: u64,
    current: Option<AttachmentSlot>,
}

struct AttachmentSlot {
    generation: u64,
    cancel: watch::Sender<bool>,
}

struct AttachmentLease {
    generation: u64,
    cancel: watch::Receiver<bool>,
}

fn attachment_is_current_locked(attachment: &AttachmentState, generation: u64) -> bool {
    attachment
        .current
        .as_ref()
        .is_some_and(|slot| slot.generation == generation)
}

#[derive(Default)]
struct Meta {
    stdin_position: u64,
    stdin_eof: bool,
    stdin_eof_at: Option<u64>,
    stdout_end: u64,
    stderr_end: u64,
    stdout_closed: bool,
    stderr_closed: bool,
    status: Option<ExitResult>,
}

struct Shared {
    meta: StdMutex<Meta>,
    child_stdin: Mutex<Option<ChildStdin>>,
    changed: Notify,
    terminal: watch::Sender<bool>,
    stdout_live: broadcast::Sender<OutputChunk>,
    stderr_live: broadcast::Sender<OutputChunk>,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    command_pid: i32,
    nobuffer: bool,
    attachment: StdMutex<AttachmentState>,
    active_attachments: AtomicUsize,
}

impl Shared {
    fn announce_if_terminal(&self, meta: &Meta) {
        if meta.status.is_some() && meta.stdout_closed && meta.stderr_closed {
            self.terminal.send_replace(true);
        }
    }

    fn install_attachment(&self, force: bool) -> std::result::Result<AttachmentLease, ()> {
        let mut attachment = self.attachment.lock().expect("attachment state poisoned");
        if attachment.current.is_some() && !force {
            return Err(());
        }
        let generation = attachment.next_generation;
        attachment.next_generation += 1;
        let (cancel_tx, cancel_rx) = watch::channel(false);
        if let Some(previous) = attachment.current.replace(AttachmentSlot {
            generation,
            cancel: cancel_tx,
        }) {
            previous.cancel.send_replace(true);
        }
        Ok(AttachmentLease {
            generation,
            cancel: cancel_rx,
        })
    }

    fn compare_clear_attachment(&self, generation: u64) {
        let mut attachment = self.attachment.lock().expect("attachment state poisoned");
        if attachment
            .current
            .as_ref()
            .is_some_and(|slot| slot.generation == generation)
        {
            attachment.current = None;
        }
    }

    fn is_current_attachment(&self, generation: u64) -> bool {
        let attachment = self.attachment.lock().expect("attachment state poisoned");
        attachment_is_current_locked(&attachment, generation)
    }
}

struct SessionGuard(PathBuf);

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub async fn run(
    _id: String,
    session_dir: PathBuf,
    command: Vec<String>,
    nobuffer: bool,
) -> Result<i32> {
    let _guard = SessionGuard(session_dir.clone());
    fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700))?;

    let stdout_path = session_dir.join("stdout.buffer");
    let stderr_path = session_dir.join("stderr.buffer");
    fs::File::create(&stdout_path)?;
    fs::File::create(&stderr_path)?;

    let mut child_command = Command::new(&command[0]);
    child_command
        .args(&command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    unsafe {
        child_command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = child_command
        .spawn()
        .with_context(|| format!("cannot start command {:?}", command[0]))?;
    let command_pid = child.id().context("command has no PID")? as i32;
    fs::write(session_dir.join("command.pid"), command_pid.to_string())?;
    let child_stdin = child.stdin.take().context("cannot open command stdin")?;
    let child_stdout = child.stdout.take().context("cannot open command stdout")?;
    let child_stderr = child.stderr.take().context("cannot open command stderr")?;

    let socket_path = runtime::socket_path(&session_dir);
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("cannot bind broker socket {}", socket_path.display()))?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    fs::write(
        session_dir.join("broker.pid"),
        std::process::id().to_string(),
    )?;

    let (terminal_tx, terminal_rx) = watch::channel(false);
    let (stdout_live, _) = broadcast::channel(64);
    let (stderr_live, _) = broadcast::channel(64);
    let shared = Arc::new(Shared {
        meta: StdMutex::new(Meta::default()),
        child_stdin: Mutex::new(Some(child_stdin)),
        changed: Notify::new(),
        terminal: terminal_tx,
        stdout_live,
        stderr_live,
        stdout_path,
        stderr_path,
        command_pid,
        nobuffer,
        attachment: StdMutex::new(AttachmentState {
            next_generation: 1,
            current: None,
        }),
        active_attachments: AtomicUsize::new(0),
    });

    tokio::spawn(drain_output(child_stdout, Stream::Stdout, shared.clone()));
    tokio::spawn(drain_output(child_stderr, Stream::Stderr, shared.clone()));
    let wait_shared = shared.clone();
    tokio::spawn(async move {
        let result = child.wait().await;
        let mut stdin = wait_shared.child_stdin.lock().await;
        stdin.take();
        drop(stdin);
        let mut meta = wait_shared.meta.lock().expect("metadata state poisoned");
        meta.status = Some(match result {
            Ok(status) => ExitResult::from_status(status),
            Err(_) => ExitResult {
                code: Some(1),
                signal: None,
            },
        });
        wait_shared.announce_if_terminal(&meta);
        drop(meta);
        wait_shared.changed.notify_waiters();
    });

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let cleanup_shared = shared.clone();
    tokio::spawn(async move {
        wait_for_true(terminal_rx).await;
        while cleanup_shared.active_attachments.load(Ordering::Acquire) != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let ttl = std::env::var("PIPEKEEP_SESSION_TTL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300_u64);
        tokio::time::sleep(Duration::from_secs(ttl)).await;
        while cleanup_shared.active_attachments.load(Ordering::Acquire) != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _ = shutdown_tx.send(()).await;
    });

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let connection_shared = shared.clone();
                connection_shared.active_attachments.fetch_add(1, Ordering::AcqRel);
                tokio::spawn(async move {
                    let _guard = ConnectionGuard(connection_shared.clone());
                    if let Err(error) = handle_connection(stream, connection_shared).await {
                        eprintln!("pipekeep broker connection: {error:#}");
                    }
                });
            }
            _ = shutdown_rx.recv() => break,
        }
    }
    Ok(0)
}

#[derive(Clone, Copy)]
enum Stream {
    Stdout,
    Stderr,
}

async fn drain_output<R: AsyncRead + Unpin>(mut reader: R, stream: Stream, shared: Arc<Shared>) {
    let path = match stream {
        Stream::Stdout => &shared.stdout_path,
        Stream::Stderr => &shared.stderr_path,
    };
    let mut file = match tokio::fs::OpenOptions::new().append(true).open(path).await {
        Ok(file) => file,
        Err(_) => return,
    };
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => {
                let mut meta = shared.meta.lock().expect("metadata state poisoned");
                match stream {
                    Stream::Stdout => meta.stdout_closed = true,
                    Stream::Stderr => meta.stderr_closed = true,
                }
                shared.announce_if_terminal(&meta);
                drop(meta);
                shared.changed.notify_waiters();
                return;
            }
            Ok(count) => {
                let data = buffer[..count].to_vec();
                if !shared.nobuffer
                    && (file.write_all(&data).await.is_err() || file.flush().await.is_err())
                {
                    return;
                }
                let mut meta = shared.meta.lock().expect("metadata state poisoned");
                let offset = match stream {
                    Stream::Stdout => {
                        let offset = meta.stdout_end;
                        meta.stdout_end += count as u64;
                        offset
                    }
                    Stream::Stderr => {
                        let offset = meta.stderr_end;
                        meta.stderr_end += count as u64;
                        offset
                    }
                };
                drop(meta);
                if shared.nobuffer {
                    let chunk = OutputChunk { offset, data };
                    match stream {
                        Stream::Stdout => {
                            let _ = shared.stdout_live.send(chunk);
                        }
                        Stream::Stderr => {
                            let _ = shared.stderr_live.send(chunk);
                        }
                    }
                }
                shared.changed.notify_waiters();
            }
        }
    }
}

async fn handle_connection(mut stream: UnixStream, shared: Arc<Shared>) -> Result<()> {
    let line = read_line(&mut stream)
        .await?
        .context("connection ended before its broker request")?;
    let request: BrokerRequest = serde_json::from_slice(&line).context("invalid broker request")?;
    match request {
        BrokerRequest::Pid => {
            write_json_line(&mut stream, &serde_json::json!({"pid": shared.command_pid})).await
        }
        BrokerRequest::Cancel => match cancel_process_group(shared.clone()).await {
            Ok((outcome, result)) => {
                write_json_line(
                    &mut stream,
                    &serde_json::json!({"exit": result, "outcome": outcome}),
                )
                .await
            }
            Err(error) => write_error_line(&mut stream, &error.to_string()).await,
        },
        BrokerRequest::Attach {
            offsets,
            stdin_start,
            stdin_eof,
            force,
        } => attach(stream, shared, offsets, stdin_start, stdin_eof, force).await,
        BrokerRequest::StdinEof { offset } => match declare_stdin_eof(&shared, offset).await {
            Ok(()) => {
                let response = {
                    let meta = shared.meta.lock().expect("metadata state poisoned");
                    serde_json::json!({
                        "stdin": meta.stdin_position,
                        "stdin_eof": meta.stdin_eof,
                        "stdin_eof_at": meta.stdin_eof_at,
                    })
                };
                write_json_line(&mut stream, &response).await
            }
            Err(error) => write_error_line(&mut stream, &error.to_string()).await,
        },
    }
}

struct AttachmentGuard {
    shared: Arc<Shared>,
    generation: u64,
}

impl Drop for AttachmentGuard {
    fn drop(&mut self) {
        self.shared.compare_clear_attachment(self.generation);
    }
}

struct ConnectionGuard(Arc<Shared>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.active_attachments.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn attach(
    mut stream: UnixStream,
    shared: Arc<Shared>,
    offsets: OutputOffsets,
    stdin_start: u64,
    stdin_eof: Option<u64>,
    force: bool,
) -> Result<()> {
    let lease = match shared.install_attachment(force) {
        Ok(lease) => lease,
        Err(()) => {
            write_error_line(&mut stream, "session already has an attached client").await?;
            return Ok(());
        }
    };
    let generation = lease.generation;
    let _guard = AttachmentGuard {
        shared: shared.clone(),
        generation,
    };

    // Subscribe before taking the snapshot so a live chunk cannot fall between
    // the returned offset and the receiver subscription.
    let stdout_live = shared.stdout_live.subscribe();
    let stderr_live = shared.stderr_live.subscribe();

    let output_offsets_are_ahead = {
        let meta = shared.meta.lock().expect("metadata state poisoned");
        offsets.stdout > meta.stdout_end || offsets.stderr > meta.stderr_end
    };
    if output_offsets_are_ahead {
        return fail_opening(
            &mut stream,
            &shared,
            generation,
            "requested output offset is ahead of the stream",
        )
        .await;
    }
    if !shared.is_current_attachment(generation) {
        write_error_line(&mut stream, "attachment superseded").await?;
        return Ok(());
    }

    if let Err(error) = apply_opening_stdin(&shared, generation, stdin_start, stdin_eof).await {
        return fail_opening(&mut stream, &shared, generation, &error.to_string()).await;
    }
    if !shared.is_current_attachment(generation) {
        write_error_line(&mut stream, "attachment superseded").await?;
        return Ok(());
    }

    let hello = {
        let meta = shared.meta.lock().expect("metadata state poisoned");
        let stdout = if shared.nobuffer {
            meta.stdout_end
        } else {
            offsets.stdout
        };
        let stderr = if shared.nobuffer {
            meta.stderr_end
        } else {
            offsets.stderr
        };
        ServerHello {
            offsets: AllOffsets {
                stdin: meta.stdin_position,
                stdout,
                stderr,
            },
            nobuffer: shared.nobuffer,
            stdin_eof: meta.stdin_eof,
            stdout_eof: meta.stdout_closed.then_some(meta.stdout_end),
            stderr_eof: meta.stderr_closed.then_some(meta.stderr_end),
            exit: meta
                .status
                .clone()
                .filter(|_| meta.stdout_closed && meta.stderr_closed),
        }
    };
    if !shared.is_current_attachment(generation) {
        write_error_line(&mut stream, "attachment superseded").await?;
        return Ok(());
    }
    write_json_line(&mut stream, &hello).await?;
    let stdout = hello.offsets.stdout;
    let stderr = hello.offsets.stderr;

    let (reader, writer) = stream.into_split();
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    let input_shared = shared.clone();
    let mut input =
        tokio::spawn(async move { receive_input(reader, input_shared, generation, ack_tx).await });
    let output_shared = shared.clone();
    let mut output = tokio::spawn(async move {
        if output_shared.nobuffer {
            send_live_output(
                writer,
                output_shared,
                generation,
                stdout_live,
                stderr_live,
                stdout,
                stderr,
                ack_rx,
            )
            .await
        } else {
            send_buffered_output(writer, output_shared, generation, stdout, stderr, ack_rx).await
        }
    });

    tokio::select! {
        biased;
        _ = wait_for_true(lease.cancel.clone()) => {
            input.abort();
            output.abort();
        }
        result = &mut output => {
            input.abort();
            result??;
        }
        result = &mut input => {
            output.abort();
            result??;
        }
    }
    Ok(())
}

async fn fail_opening(
    stream: &mut UnixStream,
    shared: &Shared,
    generation: u64,
    error: &str,
) -> Result<()> {
    shared.compare_clear_attachment(generation);
    write_error_line(stream, error).await?;
    Ok(())
}

async fn receive_input<R: AsyncRead + Unpin>(
    mut reader: R,
    shared: Arc<Shared>,
    generation: u64,
    acknowledgements: mpsc::UnboundedSender<u64>,
) -> Result<()> {
    while let Some(frame) = read_frame(&mut reader).await? {
        match frame {
            Frame::StdinData { offset, data } => {
                accept_stdin(&shared, generation, offset, &data, &acknowledgements).await?;
            }
            Frame::StdinEof { offset } => {
                accept_stdin_eof(&shared, generation, offset, &acknowledgements).await?;
            }
            _ => bail!("client sent a server-to-client frame"),
        }
    }
    Ok(())
}

async fn apply_opening_stdin(
    shared: &Shared,
    generation: u64,
    stdin_start: u64,
    stdin_eof: Option<u64>,
) -> Result<()> {
    // Lock order for attachment-scoped stdin state is:
    // child_stdin (async serialization), attachment (generation authority),
    // then metadata (retained stream state). The synchronous locks are never
    // held across await; forced install takes the same attachment lock, so each
    // mutation linearizes either before takeover or not at all.
    let mut stdin = shared.child_stdin.lock().await;
    let attachment = shared.attachment.lock().expect("attachment state poisoned");
    if !attachment_is_current_locked(&attachment, generation) {
        return Ok(());
    }
    let should_close = {
        let mut meta = shared.meta.lock().expect("metadata state poisoned");
        let mut new_position = meta.stdin_position;
        if stdin_start > new_position {
            if meta.stdin_eof {
                bail!("stdin start is beyond the recorded EOF");
            }
            if !shared.nobuffer {
                bail!("buffered stdin contains a gap at byte {new_position}");
            }
            if meta.stdin_eof_at.is_some_and(|eof| stdin_start > eof) {
                bail!("stdin start is beyond the declared EOF");
            }
            new_position = stdin_start;
        }
        if let Some(requested) = stdin_eof {
            if let Some(existing) = meta.stdin_eof_at {
                if existing != requested {
                    bail!("stdin EOF was already declared at byte {existing}");
                }
            }
        }
        let effective_eof = stdin_eof.or(meta.stdin_eof_at);
        if let Some(offset) = effective_eof {
            if offset < new_position {
                bail!("stdin EOF at byte {offset} is behind accepted byte {new_position}");
            }
        }

        meta.stdin_position = new_position;
        if meta.stdin_eof_at.is_none() {
            meta.stdin_eof_at = stdin_eof;
        }
        let should_close = effective_eof == Some(meta.stdin_position);
        if should_close {
            meta.stdin_eof = true;
        }
        should_close
    };
    if should_close {
        stdin.take();
        shared.changed.notify_waiters();
    }
    Ok(())
}

async fn declare_stdin_eof(shared: &Shared, offset: u64) -> Result<()> {
    // child_stdin is also the serialization lock for accepted input and EOF
    // declarations. Control EOF is deliberately generation-free.
    let mut stdin = shared.child_stdin.lock().await;
    let should_close = {
        let mut meta = shared.meta.lock().expect("metadata state poisoned");
        declare_stdin_eof_locked(&mut meta, offset)?
    };
    if should_close {
        stdin.take();
        shared.changed.notify_waiters();
    }
    Ok(())
}

fn declare_stdin_eof_locked(meta: &mut Meta, offset: u64) -> Result<bool> {
    if offset < meta.stdin_position {
        bail!(
            "stdin EOF at byte {offset} is behind accepted byte {}",
            meta.stdin_position
        );
    }
    if let Some(existing) = meta.stdin_eof_at {
        if existing != offset {
            bail!("stdin EOF was already declared at byte {existing}");
        }
    } else {
        meta.stdin_eof_at = Some(offset);
    }
    if offset == meta.stdin_position {
        meta.stdin_eof = true;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn stdin_frame_start_locked(
    meta: &Meta,
    offset: u64,
    data_len: usize,
    nobuffer: bool,
) -> Result<(usize, u64)> {
    if meta.stdin_eof {
        bail!("stdin data arrived after EOF");
    }
    let position = meta.stdin_position;
    if offset > position && !nobuffer {
        bail!("stdin contains a gap at offset {position}");
    }
    let start = usize::try_from(position.saturating_sub(offset))
        .unwrap_or(usize::MAX)
        .min(data_len);
    let logical_position = position.max(offset);
    let remaining = data_len - start;
    let frame_end = logical_position
        .checked_add(remaining as u64)
        .context("stdin frame position overflowed")?;
    if meta.stdin_eof_at.is_some_and(|eof| frame_end > eof) {
        bail!("stdin data extends beyond its declared EOF");
    }
    Ok((start, logical_position))
}

enum StdinWriteOutcome {
    Superseded,
    NoStdin,
    Duplicate { position: u64 },
    Wrote { position: u64, reached_eof: bool },
}

async fn accept_stdin(
    shared: &Shared,
    generation: u64,
    offset: u64,
    data: &[u8],
    acknowledgements: &mpsc::UnboundedSender<u64>,
) -> Result<()> {
    let mut stdin_guard = shared.child_stdin.lock().await;
    if data.is_empty() {
        let (position, should_close) = {
            let attachment = shared.attachment.lock().expect("attachment state poisoned");
            if !attachment_is_current_locked(&attachment, generation) {
                return Ok(());
            }
            let mut meta = shared.meta.lock().expect("metadata state poisoned");
            let (_start, _logical_position) =
                stdin_frame_start_locked(&meta, offset, data.len(), shared.nobuffer)?;
            if offset > meta.stdin_position && shared.nobuffer {
                meta.stdin_position = offset;
            }
            let should_close = meta.stdin_eof_at == Some(meta.stdin_position);
            if should_close {
                meta.stdin_eof = true;
                stdin_guard.take();
            }
            (meta.stdin_position, should_close)
        };
        if should_close {
            shared.changed.notify_waiters();
        }
        let _ = acknowledgements.send(position);
        return Ok(());
    }

    loop {
        let outcome = poll_fn(|cx| {
            let attachment = shared.attachment.lock().expect("attachment state poisoned");
            if !attachment_is_current_locked(&attachment, generation) {
                return Poll::Ready(Ok(StdinWriteOutcome::Superseded));
            }
            let mut meta = shared.meta.lock().expect("metadata state poisoned");
            let (start, logical_position) =
                match stdin_frame_start_locked(&meta, offset, data.len(), shared.nobuffer) {
                    Ok(plan) => plan,
                    Err(error) => return Poll::Ready(Err(error)),
                };
            if start >= data.len() {
                return Poll::Ready(Ok(StdinWriteOutcome::Duplicate {
                    position: meta.stdin_position,
                }));
            }
            if stdin_guard.is_none() {
                return Poll::Ready(Ok(StdinWriteOutcome::NoStdin));
            };
            let poll = {
                let stdin = stdin_guard.as_mut().expect("stdin checked above");
                Pin::new(stdin).poll_write(cx, &data[start..])
            };
            match poll {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(anyhow::anyhow!("command stdin closed"))),
                Poll::Ready(Ok(count)) => {
                    let position = logical_position + count as u64;
                    meta.stdin_position = position;
                    let reached_eof = meta.stdin_eof_at == Some(position);
                    if reached_eof {
                        meta.stdin_eof = true;
                        stdin_guard.take();
                    }
                    Poll::Ready(Ok(StdinWriteOutcome::Wrote {
                        position,
                        reached_eof,
                    }))
                }
            }
        })
        .await?;

        match outcome {
            StdinWriteOutcome::Superseded | StdinWriteOutcome::NoStdin => return Ok(()),
            StdinWriteOutcome::Duplicate { position } => {
                let _ = acknowledgements.send(position);
                return Ok(());
            }
            StdinWriteOutcome::Wrote {
                position,
                reached_eof,
            } => {
                let _ = acknowledgements.send(position);
                if reached_eof {
                    shared.changed.notify_waiters();
                    return Ok(());
                }
            }
        }
    }
}

async fn accept_stdin_eof(
    shared: &Shared,
    generation: u64,
    offset: u64,
    acknowledgements: &mpsc::UnboundedSender<u64>,
) -> Result<()> {
    let mut stdin = shared.child_stdin.lock().await;
    let (position, should_close) = {
        let attachment = shared.attachment.lock().expect("attachment state poisoned");
        if !attachment_is_current_locked(&attachment, generation) {
            return Ok(());
        }
        let mut meta = shared.meta.lock().expect("metadata state poisoned");
        let should_close = declare_stdin_eof_locked(&mut meta, offset)?;
        if should_close {
            stdin.take();
        }
        (meta.stdin_position, should_close)
    };
    if should_close {
        shared.changed.notify_waiters();
    }
    let _ = acknowledgements.send(position);
    Ok(())
}

// Generation checks prevent new output frames from starting after displacement.
// A frame already being written to that frontend's own socket may complete or
// tear; that bounded transport effect never mutates workload or retained state.
async fn send_buffered_output<W: tokio::io::AsyncWrite + Unpin>(
    mut writer: W,
    shared: Arc<Shared>,
    generation: u64,
    mut stdout_position: u64,
    mut stderr_position: u64,
    mut acknowledgements: mpsc::UnboundedReceiver<u64>,
) -> Result<()> {
    let stdout_file = fs::File::open(&shared.stdout_path)?;
    let stderr_file = fs::File::open(&shared.stderr_path)?;
    let mut prefer_stdout = true;
    loop {
        if !shared.is_current_attachment(generation) {
            return Ok(());
        }
        while let Ok(position) = acknowledgements.try_recv() {
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
        }
        let notified = shared.changed.notified();
        let (stdout_end, stderr_end, terminal) = {
            let meta = shared.meta.lock().expect("metadata state poisoned");
            (
                meta.stdout_end,
                meta.stderr_end,
                meta.status
                    .clone()
                    .filter(|_| meta.stdout_closed && meta.stderr_closed),
            )
        };

        let stdout_ready = stdout_position < stdout_end;
        let stderr_ready = stderr_position < stderr_end;
        if stdout_ready && (prefer_stdout || !stderr_ready) {
            let data = read_at(&stdout_file, stdout_position, stdout_end)?;
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            write_frame(
                &mut writer,
                &Frame::StdoutData {
                    offset: stdout_position,
                    data: data.clone(),
                },
            )
            .await?;
            stdout_position += data.len() as u64;
            prefer_stdout = false;
            continue;
        }
        if stderr_ready {
            let data = read_at(&stderr_file, stderr_position, stderr_end)?;
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            write_frame(
                &mut writer,
                &Frame::StderrData {
                    offset: stderr_position,
                    data: data.clone(),
                },
            )
            .await?;
            stderr_position += data.len() as u64;
            prefer_stdout = true;
            continue;
        }
        if let Some(status) = terminal {
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            write_frame(&mut writer, &Frame::Exit(status)).await?;
            return Ok(());
        }

        tokio::select! {
            Some(position) = acknowledgements.recv() => {
                if !shared.is_current_attachment(generation) {
                    return Ok(());
                }
                write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
            }
            _ = notified => {}
        }
    }
}

fn read_at(file: &fs::File, offset: u64, end: u64) -> Result<Vec<u8>> {
    let count = ((end - offset) as usize).min(CHUNK_SIZE);
    let mut data = vec![0_u8; count];
    let read = file.read_at(&mut data, offset)?;
    if read == 0 {
        bail!("buffer file ended before its recorded position");
    }
    data.truncate(read);
    Ok(data)
}

#[allow(clippy::too_many_arguments)]
async fn send_live_output<W: tokio::io::AsyncWrite + Unpin>(
    mut writer: W,
    shared: Arc<Shared>,
    generation: u64,
    mut stdout_live: broadcast::Receiver<OutputChunk>,
    mut stderr_live: broadcast::Receiver<OutputChunk>,
    mut stdout_position: u64,
    mut stderr_position: u64,
    mut acknowledgements: mpsc::UnboundedReceiver<u64>,
) -> Result<()> {
    let mut prefer_stdout = true;
    loop {
        if !shared.is_current_attachment(generation) {
            return Ok(());
        }
        while let Ok(position) = acknowledgements.try_recv() {
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
        }
        if prefer_stdout {
            if let Ok(chunk) = stdout_live.try_recv() {
                send_live_chunk(
                    &mut writer,
                    &shared,
                    generation,
                    Stream::Stdout,
                    &mut stdout_position,
                    chunk,
                )
                .await?;
                prefer_stdout = false;
                continue;
            }
        } else if let Ok(chunk) = stderr_live.try_recv() {
            send_live_chunk(
                &mut writer,
                &shared,
                generation,
                Stream::Stderr,
                &mut stderr_position,
                chunk,
            )
            .await?;
            prefer_stdout = true;
            continue;
        }
        // If the preferred stream had nothing ready, do not delay the other.
        if let Ok(chunk) = stdout_live.try_recv() {
            send_live_chunk(
                &mut writer,
                &shared,
                generation,
                Stream::Stdout,
                &mut stdout_position,
                chunk,
            )
            .await?;
            prefer_stdout = false;
            continue;
        }
        if let Ok(chunk) = stderr_live.try_recv() {
            send_live_chunk(
                &mut writer,
                &shared,
                generation,
                Stream::Stderr,
                &mut stderr_position,
                chunk,
            )
            .await?;
            prefer_stdout = true;
            continue;
        }

        let changed = shared.changed.notified();
        let (terminal, stdout_end, stderr_end) = {
            let meta = shared.meta.lock().expect("metadata state poisoned");
            (
                meta.status
                    .clone()
                    .filter(|_| meta.stdout_closed && meta.stderr_closed),
                meta.stdout_end,
                meta.stderr_end,
            )
        };
        if let Some(status) = terminal {
            // Any missing bytes have already fallen out of the live queue.
            stdout_position = stdout_position.max(stdout_end);
            stderr_position = stderr_position.max(stderr_end);
            let _ = (stdout_position, stderr_position);
            if !shared.is_current_attachment(generation) {
                return Ok(());
            }
            write_frame(&mut writer, &Frame::Exit(status)).await?;
            return Ok(());
        }

        tokio::select! {
            Some(position) = acknowledgements.recv() => {
                if !shared.is_current_attachment(generation) {
                    return Ok(());
                }
                write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
            }
            result = stdout_live.recv() => if let Ok(chunk) = result {
                send_live_chunk(
                    &mut writer,
                    &shared,
                    generation,
                    Stream::Stdout,
                    &mut stdout_position,
                    chunk,
                ).await?;
            },
            result = stderr_live.recv() => if let Ok(chunk) = result {
                send_live_chunk(
                    &mut writer,
                    &shared,
                    generation,
                    Stream::Stderr,
                    &mut stderr_position,
                    chunk,
                ).await?;
            },
            _ = changed => {}
        }
    }
}

async fn send_live_chunk<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    shared: &Shared,
    generation: u64,
    stream: Stream,
    position: &mut u64,
    chunk: OutputChunk,
) -> Result<()> {
    let skip = position.saturating_sub(chunk.offset) as usize;
    if skip >= chunk.data.len() {
        return Ok(());
    }
    let start = skip;
    let offset = chunk.offset + start as u64;
    *position = offset;
    let data = chunk.data[start..].to_vec();
    let frame = match stream {
        Stream::Stdout => Frame::StdoutData { offset, data },
        Stream::Stderr => Frame::StderrData { offset, data },
    };
    let length = match &frame {
        Frame::StdoutData { data, .. } | Frame::StderrData { data, .. } => data.len(),
        _ => unreachable!(),
    };
    if !shared.is_current_attachment(generation) {
        return Ok(());
    }
    write_frame(writer, &frame).await?;
    *position += length as u64;
    Ok(())
}

async fn cancel_process_group(shared: Arc<Shared>) -> Result<(CancelOutcome, ExitResult)> {
    let pgid = Pid::from_raw(shared.command_pid);
    // Each signal attempt doubles as the liveness inspection: Ok means the
    // signal reached at least one remaining group member, ESRCH means the
    // group had already settled before anything could be signaled. Deciding
    // the outcome from the attempts themselves keeps the group-disappeared
    // race honest — a group gone by signal time is never counted as won.
    let mut signaled = match signal::killpg(pgid, Signal::SIGTERM) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(error) => return Err(error.into()),
    };

    let grace = std::env::var("PIPEKEEP_CANCEL_GRACE_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3_u64);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(grace);
    while group_exists(pgid) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if group_exists(pgid) {
        match signal::killpg(pgid, Signal::SIGKILL) {
            Ok(()) => signaled = true,
            Err(Errno::ESRCH) => {}
            Err(error) => return Err(error.into()),
        }
    }
    while group_exists(pgid) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let terminal = shared.terminal.subscribe();
    wait_for_true(terminal).await;
    let meta = shared.meta.lock().expect("metadata state poisoned");
    let status = meta
        .status
        .clone()
        .context("command has no terminal result")?;
    let outcome = if signaled {
        CancelOutcome::CancelWon
    } else {
        CancelOutcome::AlreadyExited
    };
    Ok((outcome, status))
}

fn group_exists(pgid: Pid) -> bool {
    match signal::kill(Pid::from_raw(-pgid.as_raw()), None) {
        Ok(()) | Err(Errno::EPERM) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => true,
    }
}

async fn wait_for_true(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBroker {
        shared: Arc<Shared>,
        child: tokio::process::Child,
        _dir: tempfile::TempDir,
    }

    impl TestBroker {
        fn new(nobuffer: bool) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let stdout_path = dir.path().join("stdout.buffer");
            let stderr_path = dir.path().join("stderr.buffer");
            fs::File::create(&stdout_path).unwrap();
            fs::File::create(&stderr_path).unwrap();
            let mut child = Command::new("/bin/cat")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let child_stdin = child.stdin.take().unwrap();
            let command_pid = child.id().unwrap() as i32;
            let (terminal, _) = watch::channel(false);
            let (stdout_live, _) = broadcast::channel(4);
            let (stderr_live, _) = broadcast::channel(4);
            Self {
                shared: Arc::new(Shared {
                    meta: StdMutex::new(Meta::default()),
                    child_stdin: Mutex::new(Some(child_stdin)),
                    changed: Notify::new(),
                    terminal,
                    stdout_live,
                    stderr_live,
                    stdout_path,
                    stderr_path,
                    command_pid,
                    nobuffer,
                    attachment: StdMutex::new(AttachmentState {
                        next_generation: 1,
                        current: None,
                    }),
                    active_attachments: AtomicUsize::new(0),
                }),
                child,
                _dir: dir,
            }
        }

        async fn kill(mut self) {
            let _ = self.child.kill().await;
            let _ = self.child.wait().await;
        }
    }

    #[tokio::test]
    async fn superseded_opening_cannot_mutate_stdin_state_or_close_child_stdin() {
        let broker = TestBroker::new(true);
        let old = broker.shared.install_attachment(false).unwrap().generation;
        let new = broker.shared.install_attachment(true).unwrap().generation;

        apply_opening_stdin(&broker.shared, old, 5, Some(5))
            .await
            .unwrap();

        {
            let meta = broker.shared.meta.lock().expect("metadata state poisoned");
            assert_eq!(meta.stdin_position, 0);
            assert_eq!(meta.stdin_eof_at, None);
            assert!(!meta.stdin_eof);
        }
        assert!(broker.shared.child_stdin.lock().await.is_some());
        assert!(broker.shared.is_current_attachment(new));
        broker.kill().await;
    }

    #[tokio::test]
    async fn superseded_stdin_data_cannot_record_position_or_close_child_stdin() {
        let broker = TestBroker::new(false);
        let old = broker.shared.install_attachment(false).unwrap().generation;
        let new = broker.shared.install_attachment(true).unwrap().generation;
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();

        accept_stdin(&broker.shared, old, 0, b"x", &ack_tx)
            .await
            .unwrap();

        {
            let meta = broker.shared.meta.lock().expect("metadata state poisoned");
            assert_eq!(meta.stdin_position, 0);
            assert_eq!(meta.stdin_eof_at, None);
            assert!(!meta.stdin_eof);
        }
        assert!(ack_rx.try_recv().is_err());
        assert!(broker.shared.child_stdin.lock().await.is_some());
        assert!(broker.shared.is_current_attachment(new));
        broker.kill().await;
    }

    #[tokio::test]
    async fn nobuffer_empty_frame_advancing_to_eof_closes_under_authority() {
        let broker = TestBroker::new(true);
        let generation = broker.shared.install_attachment(false).unwrap().generation;
        {
            let mut meta = broker.shared.meta.lock().expect("metadata state poisoned");
            meta.stdin_eof_at = Some(5);
        }
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();

        accept_stdin(&broker.shared, generation, 5, b"", &ack_tx)
            .await
            .unwrap();

        {
            let meta = broker.shared.meta.lock().expect("metadata state poisoned");
            assert_eq!(meta.stdin_position, 5);
            assert_eq!(meta.stdin_eof_at, Some(5));
            assert!(meta.stdin_eof);
        }
        assert_eq!(ack_rx.try_recv().unwrap(), 5);
        assert!(broker.shared.child_stdin.lock().await.is_none());
        broker.kill().await;
    }

    #[tokio::test]
    async fn superseded_attachment_eof_cannot_mutate_sticky_state_or_close_child_stdin() {
        let broker = TestBroker::new(false);
        let old = broker.shared.install_attachment(false).unwrap().generation;
        let new = broker.shared.install_attachment(true).unwrap().generation;
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();

        accept_stdin_eof(&broker.shared, old, 0, &ack_tx)
            .await
            .unwrap();

        {
            let meta = broker.shared.meta.lock().expect("metadata state poisoned");
            assert_eq!(meta.stdin_eof_at, None);
            assert!(!meta.stdin_eof);
        }
        assert!(ack_rx.try_recv().is_err());
        assert!(broker.shared.child_stdin.lock().await.is_some());
        assert!(broker.shared.is_current_attachment(new));
        broker.kill().await;
    }

    #[tokio::test]
    async fn old_generation_cleanup_cannot_clear_newer_attachment() {
        let broker = TestBroker::new(false);
        let old = broker.shared.install_attachment(false).unwrap().generation;
        let new = broker.shared.install_attachment(true).unwrap().generation;

        broker.shared.compare_clear_attachment(old);
        assert!(broker.shared.is_current_attachment(new));
        broker.shared.compare_clear_attachment(new);
        assert!(!broker.shared.is_current_attachment(new));
        broker.kill().await;
    }
}

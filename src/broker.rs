use crate::protocol::{
    read_frame, read_line, write_error_line, write_frame, write_json_line, AllOffsets,
    CancelOutcome, ExitResult, Frame, OutputOffsets, ServerHello,
};
use crate::runtime;
use anyhow::{bail, Context, Result};
use nix::errno::Errno;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{ChildStdin, Command};
use tokio::sync::{broadcast, mpsc, watch, Mutex, Notify};

const CHUNK_SIZE: usize = 32 * 1024;

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
enum Request {
    Attach {
        #[serde(default)]
        offsets: OutputOffsets,
        #[serde(default)]
        stdin_start: u64,
        #[serde(default)]
        stdin_eof: Option<u64>,
    },
    #[serde(rename = "stdin-eof")]
    StdinEof {
        offset: u64,
    },
    Pid,
    Cancel,
}

#[derive(Clone, Debug)]
struct OutputChunk {
    offset: u64,
    data: Vec<u8>,
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
    meta: Mutex<Meta>,
    child_stdin: Mutex<Option<ChildStdin>>,
    changed: Notify,
    terminal: watch::Sender<bool>,
    stdout_live: broadcast::Sender<OutputChunk>,
    stderr_live: broadcast::Sender<OutputChunk>,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    command_pid: i32,
    nobuffer: bool,
    attached: AtomicBool,
    active_attachments: AtomicUsize,
}

impl Shared {
    fn announce_if_terminal(&self, meta: &Meta) {
        if meta.status.is_some() && meta.stdout_closed && meta.stderr_closed {
            self.terminal.send_replace(true);
        }
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
        meta: Mutex::new(Meta::default()),
        child_stdin: Mutex::new(Some(child_stdin)),
        changed: Notify::new(),
        terminal: terminal_tx,
        stdout_live,
        stderr_live,
        stdout_path,
        stderr_path,
        command_pid,
        nobuffer,
        attached: AtomicBool::new(false),
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
        let mut meta = wait_shared.meta.lock().await;
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
                let mut meta = shared.meta.lock().await;
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
                let mut meta = shared.meta.lock().await;
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
    let request: Request = serde_json::from_slice(&line).context("invalid broker request")?;
    match request {
        Request::Pid => {
            write_json_line(&mut stream, &serde_json::json!({"pid": shared.command_pid})).await
        }
        Request::Cancel => match cancel_process_group(shared.clone()).await {
            Ok((outcome, result)) => {
                write_json_line(
                    &mut stream,
                    &serde_json::json!({"exit": result, "outcome": outcome}),
                )
                .await
            }
            Err(error) => write_error_line(&mut stream, &error.to_string()).await,
        },
        Request::Attach {
            offsets,
            stdin_start,
            stdin_eof,
        } => attach(stream, shared, offsets, stdin_start, stdin_eof).await,
        Request::StdinEof { offset } => match declare_stdin_eof(&shared, offset).await {
            Ok(()) => {
                let meta = shared.meta.lock().await;
                write_json_line(
                    &mut stream,
                    &serde_json::json!({
                        "stdin": meta.stdin_position,
                        "stdin_eof": meta.stdin_eof,
                        "stdin_eof_at": meta.stdin_eof_at,
                    }),
                )
                .await
            }
            Err(error) => write_error_line(&mut stream, &error.to_string()).await,
        },
    }
}

struct AttachmentGuard(Arc<Shared>);

impl Drop for AttachmentGuard {
    fn drop(&mut self) {
        self.0.attached.store(false, Ordering::Release);
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
) -> Result<()> {
    if shared
        .attached
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        write_error_line(&mut stream, "session already has an attached client").await?;
        return Ok(());
    }
    let _guard = AttachmentGuard(shared.clone());

    // Subscribe before taking the snapshot so a live chunk cannot fall between
    // the returned offset and the receiver subscription.
    let stdout_live = shared.stdout_live.subscribe();
    let stderr_live = shared.stderr_live.subscribe();
    let mut meta = shared.meta.lock().await;
    if stdin_start > meta.stdin_position {
        if meta.stdin_eof {
            drop(meta);
            write_error_line(&mut stream, "stdin start is beyond the recorded EOF").await?;
            return Ok(());
        }
        if !shared.nobuffer {
            let position = meta.stdin_position;
            drop(meta);
            write_error_line(
                &mut stream,
                &format!("buffered stdin contains a gap at byte {position}"),
            )
            .await?;
            return Ok(());
        }
        if meta.stdin_eof_at.is_some_and(|eof| stdin_start > eof) {
            drop(meta);
            write_error_line(&mut stream, "stdin start is beyond the declared EOF").await?;
            return Ok(());
        }
        meta.stdin_position = stdin_start;
    }
    let known_stdin_eof = meta.stdin_eof_at;
    drop(meta);

    // Advancing the live-input start can itself reach a previously declared
    // EOF. Reapplying the sticky declaration closes the command's stdin in
    // that case; declaring the same absolute offset is intentionally
    // idempotent.
    if let Some(offset) = stdin_eof.or(known_stdin_eof) {
        if let Err(error) = declare_stdin_eof(&shared, offset).await {
            write_error_line(&mut stream, &error.to_string()).await?;
            return Ok(());
        }
    }

    let meta = shared.meta.lock().await;
    if offsets.stdout > meta.stdout_end || offsets.stderr > meta.stderr_end {
        drop(meta);
        write_error_line(
            &mut stream,
            "requested output offset is ahead of the stream",
        )
        .await?;
        return Ok(());
    }
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
    let hello = ServerHello {
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
    };
    drop(meta);
    write_json_line(&mut stream, &hello).await?;

    let (reader, writer) = stream.into_split();
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    let input_shared = shared.clone();
    let mut input = tokio::spawn(async move { receive_input(reader, input_shared, ack_tx).await });
    let output_shared = shared.clone();
    let mut output = tokio::spawn(async move {
        if output_shared.nobuffer {
            send_live_output(
                writer,
                output_shared,
                stdout_live,
                stderr_live,
                stdout,
                stderr,
                ack_rx,
            )
            .await
        } else {
            send_buffered_output(writer, output_shared, stdout, stderr, ack_rx).await
        }
    });

    tokio::select! {
        biased;
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

async fn receive_input<R: AsyncRead + Unpin>(
    mut reader: R,
    shared: Arc<Shared>,
    acknowledgements: mpsc::UnboundedSender<u64>,
) -> Result<()> {
    while let Some(frame) = read_frame(&mut reader).await? {
        match frame {
            Frame::StdinData { offset, data } => {
                accept_stdin(&shared, offset, &data, &acknowledgements).await?;
            }
            Frame::StdinEof { offset } => {
                accept_stdin_eof(&shared, offset, &acknowledgements).await?;
            }
            _ => bail!("client sent a server-to-client frame"),
        }
    }
    Ok(())
}

async fn declare_stdin_eof(shared: &Shared, offset: u64) -> Result<()> {
    // child_stdin is also the serialization lock for accepted input and EOF
    // declarations. Always take it before meta when both are needed.
    let mut stdin = shared.child_stdin.lock().await;
    let should_close = {
        let mut meta = shared.meta.lock().await;
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
            true
        } else {
            false
        }
    };
    if should_close {
        stdin.take();
        shared.changed.notify_waiters();
    }
    Ok(())
}

async fn accept_stdin(
    shared: &Shared,
    offset: u64,
    data: &[u8],
    acknowledgements: &mpsc::UnboundedSender<u64>,
) -> Result<()> {
    // Serialize the position check and write with EOF declarations.
    let mut stdin_guard = shared.child_stdin.lock().await;
    let (position, eof_at) = {
        let meta = shared.meta.lock().await;
        if meta.stdin_eof {
            bail!("stdin data arrived after EOF");
        }
        (meta.stdin_position, meta.stdin_eof_at)
    };
    if offset > position && !shared.nobuffer {
        bail!("stdin contains a gap at offset {position}");
    }
    let mut start = position.saturating_sub(offset) as usize;
    start = start.min(data.len());
    let mut logical_position = position.max(offset);
    if eof_at.is_some_and(|eof| logical_position + (data.len() - start) as u64 > eof) {
        bail!("stdin data extends beyond its declared EOF");
    }
    let Some(stdin) = stdin_guard.as_mut() else {
        return Ok(());
    };
    while start < data.len() {
        let count = stdin.write(&data[start..]).await?;
        if count == 0 {
            bail!("command stdin closed");
        }
        start += count;
        logical_position += count as u64;
        let mut meta = shared.meta.lock().await;
        meta.stdin_position = logical_position;
        let reached_eof = meta.stdin_eof_at == Some(logical_position);
        if reached_eof {
            meta.stdin_eof = true;
        }
        drop(meta);
        let _ = acknowledgements.send(logical_position);
        if reached_eof {
            stdin_guard.take();
            shared.changed.notify_waiters();
            break;
        }
    }
    if start == data.len() && logical_position == position {
        let _ = acknowledgements.send(position);
    }
    if data.is_empty() && offset > position && shared.nobuffer {
        let mut meta = shared.meta.lock().await;
        meta.stdin_position = offset;
        let _ = acknowledgements.send(offset);
    }
    Ok(())
}

async fn accept_stdin_eof(
    shared: &Shared,
    offset: u64,
    acknowledgements: &mpsc::UnboundedSender<u64>,
) -> Result<()> {
    declare_stdin_eof(shared, offset).await?;
    let position = shared.meta.lock().await.stdin_position;
    let _ = acknowledgements.send(position);
    Ok(())
}

async fn send_buffered_output<W: tokio::io::AsyncWrite + Unpin>(
    mut writer: W,
    shared: Arc<Shared>,
    mut stdout_position: u64,
    mut stderr_position: u64,
    mut acknowledgements: mpsc::UnboundedReceiver<u64>,
) -> Result<()> {
    let stdout_file = fs::File::open(&shared.stdout_path)?;
    let stderr_file = fs::File::open(&shared.stderr_path)?;
    let mut prefer_stdout = true;
    loop {
        while let Ok(position) = acknowledgements.try_recv() {
            write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
        }
        let notified = shared.changed.notified();
        let meta = shared.meta.lock().await;
        let stdout_end = meta.stdout_end;
        let stderr_end = meta.stderr_end;
        let terminal = meta
            .status
            .clone()
            .filter(|_| meta.stdout_closed && meta.stderr_closed);
        drop(meta);

        let stdout_ready = stdout_position < stdout_end;
        let stderr_ready = stderr_position < stderr_end;
        if stdout_ready && (prefer_stdout || !stderr_ready) {
            let data = read_at(&stdout_file, stdout_position, stdout_end)?;
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
            write_frame(&mut writer, &Frame::Exit(status)).await?;
            return Ok(());
        }

        tokio::select! {
            Some(position) = acknowledgements.recv() => {
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
    mut stdout_live: broadcast::Receiver<OutputChunk>,
    mut stderr_live: broadcast::Receiver<OutputChunk>,
    mut stdout_position: u64,
    mut stderr_position: u64,
    mut acknowledgements: mpsc::UnboundedReceiver<u64>,
) -> Result<()> {
    let mut prefer_stdout = true;
    loop {
        while let Ok(position) = acknowledgements.try_recv() {
            write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
        }
        if prefer_stdout {
            if let Ok(chunk) = stdout_live.try_recv() {
                send_live_chunk(&mut writer, Stream::Stdout, &mut stdout_position, chunk).await?;
                prefer_stdout = false;
                continue;
            }
        } else if let Ok(chunk) = stderr_live.try_recv() {
            send_live_chunk(&mut writer, Stream::Stderr, &mut stderr_position, chunk).await?;
            prefer_stdout = true;
            continue;
        }
        // If the preferred stream had nothing ready, do not delay the other.
        if let Ok(chunk) = stdout_live.try_recv() {
            send_live_chunk(&mut writer, Stream::Stdout, &mut stdout_position, chunk).await?;
            prefer_stdout = false;
            continue;
        }
        if let Ok(chunk) = stderr_live.try_recv() {
            send_live_chunk(&mut writer, Stream::Stderr, &mut stderr_position, chunk).await?;
            prefer_stdout = true;
            continue;
        }

        let changed = shared.changed.notified();
        let meta = shared.meta.lock().await;
        let terminal = meta
            .status
            .clone()
            .filter(|_| meta.stdout_closed && meta.stderr_closed);
        let stdout_end = meta.stdout_end;
        let stderr_end = meta.stderr_end;
        drop(meta);
        if let Some(status) = terminal {
            // Any missing bytes have already fallen out of the live queue.
            stdout_position = stdout_position.max(stdout_end);
            stderr_position = stderr_position.max(stderr_end);
            let _ = (stdout_position, stderr_position);
            write_frame(&mut writer, &Frame::Exit(status)).await?;
            return Ok(());
        }

        tokio::select! {
            Some(position) = acknowledgements.recv() => {
                write_frame(&mut writer, &Frame::StdinPosition { offset: position }).await?;
            }
            result = stdout_live.recv() => if let Ok(chunk) = result {
                send_live_chunk(&mut writer, Stream::Stdout, &mut stdout_position, chunk).await?;
            },
            result = stderr_live.recv() => if let Ok(chunk) = result {
                send_live_chunk(&mut writer, Stream::Stderr, &mut stderr_position, chunk).await?;
            },
            _ = changed => {}
        }
    }
}

async fn send_live_chunk<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
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
    let meta = shared.meta.lock().await;
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

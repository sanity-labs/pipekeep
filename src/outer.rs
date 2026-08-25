use crate::protocol::{
    read_line, write_json_line, ClientAction, ClientHello, OutputOffsets, ServerHello,
};
use anyhow::{bail, Context, Result};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write as _;
use std::os::unix::fs::FileExt;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify};

const CHUNK_SIZE: usize = 32 * 1024;
const LIVE_STDIN_CAPACITY: usize = 64 * 1024;

pub async fn run(command: Vec<String>, nobuffer: bool) -> Result<i32> {
    let input = InputBuffer::start(nobuffer)?;
    let sinks = Arc::new(Sinks {
        stdout: Mutex::new(tokio::io::stdout()),
        stderr: Mutex::new(tokio::io::stderr()),
    });
    let mut delivered = OutputOffsets::default();
    let mut create = true;
    let mut established = false;
    let mut backoff = Duration::from_millis(100);

    loop {
        match attempt(
            &command,
            input.clone(),
            sinks.clone(),
            &mut delivered,
            create,
        )
        .await?
        {
            AttemptResult::Exited(code) => return Ok(code),
            AttemptResult::Disconnected { opened } => {
                // If creation may have reached the remote broker, probe it as a
                // resume on the next attempt. A missing-session response causes
                // a fresh create attempt below.
                create = false;
                if opened {
                    established = true;
                    backoff = Duration::from_millis(100);
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
            AttemptResult::ServerError(error) => {
                if error.contains("session already has an attached client") {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                if !create && !established && error.contains("session does not exist") {
                    create = true;
                    continue;
                }
                bail!("remote pipekeep: {error}");
            }
        }
    }
}

enum AttemptResult {
    Exited(i32),
    Disconnected { opened: bool },
    ServerError(String),
}

enum SendOutcome {
    Reattach,
}

async fn attempt(
    command: &[String],
    input: Arc<InputBuffer>,
    sinks: Arc<Sinks>,
    delivered: &mut OutputOffsets,
    create: bool,
) -> Result<AttemptResult> {
    let input_state = input.attachment_state().await;
    let mut process = Command::new(&command[0]);
    process
        .args(&command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process
        .spawn()
        .with_context(|| format!("cannot start transport command {:?}", command[0]))?;
    let mut transport_input = child.stdin.take().context("transport has no stdin")?;
    let mut transport_output = child.stdout.take().context("transport has no stdout")?;
    let transport_error = child.stderr.take().context("transport has no stderr")?;

    let hello = ClientHello {
        action: None,
        offsets: if create { None } else { Some(*delivered) },
        stdin_start: input_state.start,
        stdin_eof: input_state.eof,
        force: false,
    };
    if write_json_line(&mut transport_input, &hello).await.is_err() {
        finish_transport(&mut child).await;
        return Ok(AttemptResult::Disconnected { opened: false });
    }
    let line = match read_line(&mut transport_output).await {
        Ok(Some(line)) => line,
        Ok(None) | Err(_) => {
            finish_transport(&mut child).await;
            return Ok(AttemptResult::Disconnected { opened: false });
        }
    };
    let value: serde_json::Value = serde_json::from_slice(&line)
        .context("transport returned an invalid opening JSON message")?;
    if let Some(error) = value.get("error").and_then(serde_json::Value::as_str) {
        finish_transport(&mut child).await;
        return Ok(AttemptResult::ServerError(error.to_owned()));
    }
    let server: ServerHello = serde_json::from_value(value)
        .context("transport returned an invalid pipekeep handshake")?;
    if server.offsets.stdout < delivered.stdout || server.offsets.stderr < delivered.stderr {
        bail!("server moved an output position backwards");
    }
    if !server.nobuffer
        && (server.offsets.stdout != delivered.stdout || server.offsets.stderr != delivered.stderr)
    {
        bail!("buffered server cannot provide the requested output positions");
    }
    delivered.stdout = server.offsets.stdout;
    delivered.stderr = server.offsets.stderr;

    let stdout_position = Arc::new(AtomicU64::new(delivered.stdout));
    let stderr_position = Arc::new(AtomicU64::new(delivered.stderr));
    let stdout_sink = sinks.clone();
    let stdout_counter = stdout_position.clone();
    let mut stdout_task = tokio::spawn(async move {
        copy_raw_output(transport_output, &stdout_sink.stdout, stdout_counter).await
    });
    let stderr_sink = sinks.clone();
    let stderr_counter = stderr_position.clone();
    let mut stderr_task = tokio::spawn(async move {
        copy_raw_output(transport_error, &stderr_sink.stderr, stderr_counter).await
    });

    let sender_input = input.clone();
    let stdin_position = server.offsets.stdin;
    let declared_eof = hello.stdin_eof;
    let server_stdin_eof = server.stdin_eof;
    let (eof_tx, mut eof_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut sender = tokio::spawn(async move {
        send_raw_stdin(
            &mut transport_input,
            sender_input,
            stdin_position,
            declared_eof,
            server_stdin_eof,
            eof_tx,
        )
        .await
    });

    enum Stop {
        Child(std::io::Result<std::process::ExitStatus>),
        Sender(std::result::Result<Result<SendOutcome>, tokio::task::JoinError>),
        Stdout(std::result::Result<Result<()>, tokio::task::JoinError>),
        Stderr(std::result::Result<Result<()>, tokio::task::JoinError>),
    }

    let (stop, mut stdout_done, mut stderr_done) = loop {
        let stop = tokio::select! {
            result = child.wait() => Stop::Child(result),
            result = &mut sender => Stop::Sender(result),
            result = &mut stdout_task => Stop::Stdout(result),
            result = &mut stderr_task => Stop::Stderr(result),
            Some(offset) = eof_rx.recv() => {
                if declare_remote_stdin_eof(command, offset, sinks.clone())
                    .await
                    .is_err()
                {
                    break (Stop::Sender(Ok(Ok(SendOutcome::Reattach))), false, false);
                }
                continue;
            }
        };
        match stop {
            Stop::Stdout(result) => break (Stop::Stdout(result), true, false),
            Stop::Stderr(result) => break (Stop::Stderr(result), false, true),
            other => break (other, false, false),
        }
    };

    let mut fatal = None;
    match stop {
        Stop::Child(result) => {
            if let Err(error) = result {
                fatal = Some(error.into());
            }
        }
        Stop::Sender(result) => match result {
            Ok(Ok(SendOutcome::Reattach)) | Ok(Err(_)) | Err(_) => {
                finish_transport(&mut child).await;
            }
        },
        Stop::Stdout(result) => match flatten_task(result) {
            Ok(()) => {
                let _ = child.wait().await;
            }
            Err(error) => {
                fatal = Some(error);
                finish_transport(&mut child).await;
            }
        },
        Stop::Stderr(result) => match flatten_task(result) {
            Ok(()) => {
                let _ = child.wait().await;
            }
            Err(error) => {
                fatal = Some(error);
                finish_transport(&mut child).await;
            }
        },
    }
    sender.abort();

    if !stdout_done {
        match flatten_task(stdout_task.await) {
            Ok(()) => {}
            Err(error) if fatal.is_none() => fatal = Some(error),
            Err(_) => {}
        }
        stdout_done = true;
    }
    if !stderr_done {
        match flatten_task(stderr_task.await) {
            Ok(()) => {}
            Err(error) if fatal.is_none() => fatal = Some(error),
            Err(_) => {}
        }
        stderr_done = true;
    }
    let _ = (stdout_done, stderr_done);

    delivered.stdout = stdout_position.load(Ordering::Acquire);
    delivered.stderr = stderr_position.load(Ordering::Acquire);
    if let Some(error) = fatal {
        return Err(error);
    }

    if let Some(exit) = server.exit {
        let stdout_complete = server.stdout_eof.is_some_and(|end| delivered.stdout >= end);
        let stderr_complete = server.stderr_eof.is_some_and(|end| delivered.stderr >= end);
        if stdout_complete && stderr_complete {
            return Ok(AttemptResult::Exited(exit.process_code()));
        }
    }
    Ok(AttemptResult::Disconnected { opened: true })
}

fn flatten_task(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.context("stream forwarding task failed")?
}

async fn declare_remote_stdin_eof(
    command: &[String],
    offset: u64,
    sinks: Arc<Sinks>,
) -> Result<()> {
    let mut process = Command::new(&command[0]);
    process
        .args(&command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process
        .spawn()
        .with_context(|| format!("cannot start EOF control command {:?}", command[0]))?;
    let mut input = child.stdin.take().context("EOF control has no stdin")?;
    let mut output = child.stdout.take().context("EOF control has no stdout")?;
    let error = child.stderr.take().context("EOF control has no stderr")?;
    let diagnostic_sinks = sinks.clone();
    let diagnostics =
        tokio::spawn(async move { forward_untracked_stderr(error, diagnostic_sinks).await });
    write_json_line(
        &mut input,
        &ClientHello {
            action: Some(ClientAction::StdinEof),
            offsets: None,
            stdin_start: 0,
            stdin_eof: Some(offset),
            force: false,
        },
    )
    .await?;
    input.shutdown().await?;
    let response = read_line(&mut output)
        .await?
        .context("EOF control ended before its response")?;
    let value: serde_json::Value = serde_json::from_slice(&response)?;
    if let Some(error) = value.get("error").and_then(serde_json::Value::as_str) {
        finish_transport(&mut child).await;
        diagnostics.abort();
        bail!("remote stdin EOF: {error}");
    }
    if value
        .get("stdin_eof_at")
        .and_then(serde_json::Value::as_u64)
        != Some(offset)
    {
        finish_transport(&mut child).await;
        diagnostics.abort();
        bail!("remote stdin EOF response did not confirm byte {offset}");
    }
    let status = child.wait().await?;
    diagnostics.await.context("EOF diagnostics task failed")??;
    if !status.success() {
        bail!("stdin EOF control transport exited with {status}");
    }
    Ok(())
}

async fn finish_transport(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => {}
        _ => {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
}

struct Sinks {
    stdout: Mutex<tokio::io::Stdout>,
    stderr: Mutex<tokio::io::Stderr>,
}

async fn copy_raw_output<R, W>(
    mut reader: R,
    sink: &Mutex<W>,
    position: Arc<AtomicU64>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        let mut writer = sink.lock().await;
        writer.write_all(&buffer[..count]).await?;
        writer.flush().await?;
        position.fetch_add(count as u64, Ordering::Release);
    }
}

async fn forward_untracked_stderr<R: AsyncRead + Unpin>(
    mut reader: R,
    sinks: Arc<Sinks>,
) -> Result<()> {
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        let mut stderr = sinks.stderr.lock().await;
        stderr.write_all(&buffer[..count]).await?;
        stderr.flush().await?;
    }
}

#[derive(Clone, Copy)]
struct InputState {
    start: u64,
    eof: Option<u64>,
}

#[derive(Default)]
struct InputMeta {
    end: u64,
    eof: bool,
    live_base: u64,
    live: VecDeque<u8>,
}

impl InputMeta {
    // The nobuffer input backlog is a bounded rolling window: only the newest
    // LIVE_STDIN_CAPACITY bytes stay replayable, and live_base advances past
    // everything discarded so absolute offsets expose the unretained range.
    fn retain_live_tail(&mut self, data: &[u8]) {
        self.live.extend(data);
        while self.live.len() > LIVE_STDIN_CAPACITY {
            self.live.pop_front();
            self.live_base += 1;
        }
    }
}

struct InputBuffer {
    meta: Mutex<InputMeta>,
    changed: Notify,
    file: Option<Arc<File>>,
}

impl InputBuffer {
    fn start(nobuffer: bool) -> Result<Arc<Self>> {
        let (file, writer) = if nobuffer {
            (None, None)
        } else {
            let file = tempfile::tempfile()?;
            let writer = file.try_clone()?;
            (Some(Arc::new(file)), Some(writer))
        };
        let input = Arc::new(Self {
            meta: Mutex::new(InputMeta::default()),
            changed: Notify::new(),
            file,
        });
        let task_input = input.clone();
        tokio::spawn(async move {
            read_local_stdin(task_input, writer).await;
        });
        Ok(input)
    }

    async fn attachment_state(&self) -> InputState {
        let meta = self.meta.lock().await;
        InputState {
            start: if self.file.is_some() {
                0
            } else {
                meta.live_base
            },
            eof: meta.eof.then_some(meta.end),
        }
    }

    async fn next(&self, position: u64) -> Result<InputPart> {
        let meta = self.meta.lock().await;
        if let Some(file) = &self.file {
            if position < meta.end {
                let count = ((meta.end - position) as usize).min(CHUNK_SIZE);
                let mut data = vec![0_u8; count];
                let read = file.read_at(&mut data, position)?;
                data.truncate(read);
                return Ok(InputPart::Data {
                    offset: position,
                    data,
                });
            }
        } else {
            let offset = position.max(meta.live_base);
            if offset < meta.end {
                let start = (offset - meta.live_base) as usize;
                let count = ((meta.end - offset) as usize).min(CHUNK_SIZE);
                let data = meta.live.iter().skip(start).take(count).copied().collect();
                return Ok(InputPart::Data { offset, data });
            }
        }
        if meta.eof {
            Ok(InputPart::Eof { offset: meta.end })
        } else {
            Ok(InputPart::Wait)
        }
    }
}

enum InputPart {
    Data { offset: u64, data: Vec<u8> },
    Eof { offset: u64 },
    Wait,
}

async fn read_local_stdin(input: Arc<InputBuffer>, mut file: Option<File>) {
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    loop {
        match stdin.read(&mut buffer).await {
            Ok(0) | Err(_) => {
                input.meta.lock().await.eof = true;
                input.changed.notify_waiters();
                return;
            }
            Ok(count) => {
                if let Some(file) = file.as_mut() {
                    if file.write_all(&buffer[..count]).is_err() {
                        input.meta.lock().await.eof = true;
                        input.changed.notify_waiters();
                        return;
                    }
                }
                let mut meta = input.meta.lock().await;
                meta.end += count as u64;
                if file.is_none() {
                    meta.retain_live_tail(&buffer[..count]);
                }
                drop(meta);
                input.changed.notify_waiters();
            }
        }
    }
}

async fn send_raw_stdin<W: AsyncWrite + Unpin>(
    writer: &mut W,
    input: Arc<InputBuffer>,
    mut position: u64,
    declared_eof: Option<u64>,
    server_eof: bool,
    eof: tokio::sync::mpsc::UnboundedSender<u64>,
) -> Result<SendOutcome> {
    if server_eof {
        return std::future::pending::<Result<SendOutcome>>().await;
    }
    loop {
        let notified = input.changed.notified();
        match input.next(position).await? {
            InputPart::Data { offset, data } => {
                if offset != position {
                    return Ok(SendOutcome::Reattach);
                }
                writer.write_all(&data).await?;
                writer.flush().await?;
                position += data.len() as u64;
            }
            InputPart::Eof { offset } => {
                if offset != position {
                    bail!("server stdin position is beyond local EOF");
                }
                if let Some(declared) = declared_eof {
                    if declared != offset {
                        bail!("declared stdin EOF changed during attachment");
                    }
                    return std::future::pending::<Result<SendOutcome>>().await;
                }
                eof.send(offset)
                    .map_err(|_| anyhow::anyhow!("stdin EOF controller stopped"))?;
                // Keep both the transport input and the notification channel
                // alive. Closing the transport's stdin would detach the
                // remote data proxy before its stdout and stderr are drained.
                let keep_eof_channel = eof;
                let result = std::future::pending::<Result<SendOutcome>>().await;
                drop(keep_eof_channel);
                return result;
            }
            InputPart::Wait => notified.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_input(chunks: &[&[u8]]) -> InputBuffer {
        let mut meta = InputMeta::default();
        for chunk in chunks {
            meta.end += chunk.len() as u64;
            meta.retain_live_tail(chunk);
        }
        InputBuffer {
            meta: Mutex::new(meta),
            changed: Notify::new(),
            file: None,
        }
    }

    #[test]
    fn live_stdin_window_keeps_newest_bounded_tail() {
        let mut meta = InputMeta::default();
        meta.end += LIVE_STDIN_CAPACITY as u64;
        meta.retain_live_tail(&vec![b'a'; LIVE_STDIN_CAPACITY]);
        assert_eq!(meta.live_base, 0);
        assert_eq!(meta.live.len(), LIVE_STDIN_CAPACITY);

        meta.end += 3;
        meta.retain_live_tail(b"xyz");
        assert_eq!(meta.live_base, 3);
        assert_eq!(meta.live.len(), LIVE_STDIN_CAPACITY);
        assert!(meta
            .live
            .iter()
            .skip(LIVE_STDIN_CAPACITY - 3)
            .eq(b"xyz".iter()));
    }

    #[tokio::test]
    async fn live_stdin_replays_retained_tail_and_exposes_discarded_range() {
        let discarded = 100_usize;
        let head = vec![b'a'; LIVE_STDIN_CAPACITY];
        let tail = vec![b'b'; discarded];
        let input = live_input(&[&head, &tail]);
        let base = discarded as u64;
        let end = (LIVE_STDIN_CAPACITY + discarded) as u64;

        // The opening handshake advertises the first retained byte.
        let state = input.attachment_state().await;
        assert_eq!(state.start, base);
        assert_eq!(state.eof, None);

        // A position inside the window replays exactly the retained bytes.
        match input.next(LIVE_STDIN_CAPACITY as u64).await.unwrap() {
            InputPart::Data { offset, data } => {
                assert_eq!(offset, LIVE_STDIN_CAPACITY as u64);
                assert_eq!(data, tail);
            }
            _ => panic!("expected retained data"),
        }

        // A position before the window jumps forward to the retained base:
        // the discarded range appears as an offset gap, never as other bytes.
        match input.next(0).await.unwrap() {
            InputPart::Data { offset, data } => {
                assert_eq!(offset, base);
                assert!(data.iter().all(|byte| *byte == b'a'));
            }
            _ => panic!("expected data at the retained base"),
        }

        // Nothing beyond the end until EOF is recorded.
        assert!(matches!(input.next(end).await.unwrap(), InputPart::Wait));
        input.meta.lock().await.eof = true;
        match input.next(end).await.unwrap() {
            InputPart::Eof { offset } => assert_eq!(offset, end),
            _ => panic!("expected EOF at the absolute end"),
        }
    }
}

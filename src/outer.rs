use crate::protocol::{
    read_frame, read_line, write_frame, write_json_line, ClientHello, Frame, OutputOffsets,
    ServerHello,
};
use anyhow::{bail, Context, Result};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write as _;
use std::os::unix::fs::FileExt;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

async fn attempt(
    command: &[String],
    input: Arc<InputBuffer>,
    sinks: Arc<Sinks>,
    delivered: &mut OutputOffsets,
    create: bool,
) -> Result<AttemptResult> {
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
    let diagnostic_sinks = sinks.clone();
    let diagnostics =
        tokio::spawn(async move { forward_diagnostics(transport_error, diagnostic_sinks).await });

    let hello = ClientHello {
        offsets: if create { None } else { Some(*delivered) },
    };
    if write_json_line(&mut transport_input, &hello).await.is_err() {
        diagnostics.abort();
        finish_transport(&mut child).await;
        return Ok(AttemptResult::Disconnected { opened: false });
    }
    let line = match read_line(&mut transport_output).await {
        Ok(Some(line)) => line,
        Ok(None) | Err(_) => {
            diagnostics.abort();
            finish_transport(&mut child).await;
            return Ok(AttemptResult::Disconnected { opened: false });
        }
    };
    let value: serde_json::Value = serde_json::from_slice(&line)
        .context("transport returned an invalid opening JSON message")?;
    if let Some(error) = value.get("error").and_then(serde_json::Value::as_str) {
        diagnostics.abort();
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

    let sender_input = input.clone();
    let mut sender = tokio::spawn(async move {
        send_stdin(&mut transport_input, sender_input, server.offsets.stdin).await
    });

    loop {
        tokio::select! {
            frame = read_frame(&mut transport_output) => {
                match frame {
                    Ok(Some(Frame::StdoutData { offset, data })) => {
                        deliver_output(
                            &sinks.stdout,
                            &mut delivered.stdout,
                            offset,
                            &data,
                            server.nobuffer,
                        ).await?;
                    }
                    Ok(Some(Frame::StderrData { offset, data })) => {
                        deliver_output(
                            &sinks.stderr,
                            &mut delivered.stderr,
                            offset,
                            &data,
                            server.nobuffer,
                        ).await?;
                    }
                    Ok(Some(Frame::StdinPosition { .. })) => {
                        // The next attachment uses the broker's authoritative
                        // opening position. In-process acknowledgements are
                        // intentionally only a retention hint.
                    }
                    Ok(Some(Frame::Exit(result))) => {
                        sender.abort();
                        diagnostics.abort();
                        finish_transport(&mut child).await;
                        return Ok(AttemptResult::Exited(result.process_code()));
                    }
                    Ok(Some(_)) => bail!("server sent a client-to-server frame"),
                    Ok(None) | Err(_) => {
                        sender.abort();
                        diagnostics.abort();
                        finish_transport(&mut child).await;
                        return Ok(AttemptResult::Disconnected { opened: true });
                    }
                }
            }
            result = &mut sender => {
                diagnostics.abort();
                finish_transport(&mut child).await;
                match result {
                    Ok(Ok(())) => unreachable!("stdin sender remains open after EOF"),
                    Ok(Err(_)) | Err(_) => {
                        return Ok(AttemptResult::Disconnected { opened: true });
                    }
                }
            }
        }
    }
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

async fn forward_diagnostics<R: tokio::io::AsyncRead + Unpin>(
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

async fn deliver_output<W: tokio::io::AsyncWrite + Unpin>(
    sink: &Mutex<W>,
    position: &mut u64,
    offset: u64,
    data: &[u8],
    gaps_allowed: bool,
) -> Result<()> {
    if offset > *position {
        if !gaps_allowed {
            bail!("buffered output contains a gap at byte {position}");
        }
        *position = offset;
    }
    let skip = position.saturating_sub(offset) as usize;
    if skip >= data.len() {
        return Ok(());
    }
    let bytes = &data[skip..];
    let mut writer = sink.lock().await;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    *position += bytes.len() as u64;
    Ok(())
}

#[derive(Default)]
struct InputMeta {
    end: u64,
    eof: bool,
    live_base: u64,
    live: VecDeque<u8>,
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
                    meta.live.extend(&buffer[..count]);
                    while meta.live.len() > LIVE_STDIN_CAPACITY {
                        meta.live.pop_front();
                        meta.live_base += 1;
                    }
                }
                drop(meta);
                input.changed.notify_waiters();
            }
        }
    }
}

async fn send_stdin<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    input: Arc<InputBuffer>,
    mut position: u64,
) -> Result<()> {
    loop {
        let notified = input.changed.notified();
        match input.next(position).await? {
            InputPart::Data { offset, data } => {
                write_frame(
                    writer,
                    &Frame::StdinData {
                        offset,
                        data: data.clone(),
                    },
                )
                .await?;
                position = offset + data.len() as u64;
            }
            InputPart::Eof { offset } => {
                write_frame(writer, &Frame::StdinEof { offset }).await?;
                // Transport EOF is deliberately not child stdin EOF. Keep the
                // transport pipe open until the exit frame arrives.
                std::future::pending::<()>().await;
            }
            InputPart::Wait => notified.await,
        }
    }
}

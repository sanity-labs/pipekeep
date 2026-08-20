use crate::protocol::{
    read_frame, read_line, write_error_line, write_frame, write_json_line, write_typed_error_line,
    CancelOutcome, ClientAction, ClientHello, ExitResult, Frame, OutputOffsets, ServerHello,
    ERROR_SESSION_MISSING,
};
use crate::runtime;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, Stderr, Stdin, Stdout};
use tokio::net::UnixStream;

pub async fn run(
    id: String,
    attachment_id: Option<String>,
    command: Vec<String>,
    nobuffer: bool,
) -> Result<i32> {
    let mut input = tokio::io::stdin();
    let mut output = tokio::io::stdout();
    let line = read_line(&mut input)
        .await?
        .context("protocol client closed before the opening JSON message")?;
    let hello: ClientHello = match serde_json::from_slice(&line) {
        Ok(hello) => hello,
        Err(error) => {
            write_error_line(
                &mut output,
                &format!("invalid opening JSON message: {error}"),
            )
            .await?;
            return Ok(1);
        }
    };
    if matches!(hello.action, Some(ClientAction::StdinEof)) {
        let Some(offset) = hello.stdin_eof else {
            write_error_line(&mut output, "stdin-eof requires stdin_eof").await?;
            return Ok(1);
        };
        return stdin_eof_control(&id, offset, output).await;
    }
    if hello.stdin_eof.is_some_and(|eof| eof < hello.stdin_start) {
        write_error_line(&mut output, "stdin_eof is before stdin_start").await?;
        return Ok(1);
    }
    let creating = hello.offsets.is_none();
    let offsets = hello.offsets.unwrap_or_default();
    let session_dir = runtime::session_dir(&id)?;
    let attachment_id = attachment_id.unwrap_or_else(generated_attachment_id);

    let connection = if creating {
        match start_broker(&id, &session_dir, &command, nobuffer).await {
            Ok(connection) => connection,
            Err(error) => {
                write_error_line(&mut output, &error.to_string()).await?;
                return Ok(1);
            }
        }
    } else {
        if !session_dir.is_dir() {
            write_typed_error_line(&mut output, "session does not exist", ERROR_SESSION_MISSING)
                .await?;
            return Ok(1);
        }
        match connect_existing(&session_dir).await {
            Ok(connection) => connection,
            Err(error) => {
                write_error_line(&mut output, &error.to_string()).await?;
                return Ok(1);
            }
        }
    };

    proxy_attachment(
        input,
        output,
        tokio::io::stderr(),
        connection,
        AttachmentOpening {
            offsets,
            stdin_start: hello.stdin_start,
            stdin_eof: hello.stdin_eof,
            attachment_id,
        },
    )
    .await
}

fn generated_attachment_id() -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("local-{}-{nanos}-{sequence}", std::process::id())
}

async fn start_broker(
    id: &str,
    session_dir: &Path,
    command: &[String],
    nobuffer: bool,
) -> Result<UnixStream> {
    match fs::create_dir(session_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            bail!("session {id:?} already exists")
        }
        Err(error) => return Err(error.into()),
    }
    fs::set_permissions(session_dir, fs::Permissions::from_mode(0o700))?;
    let log_path = session_dir.join("broker.log");
    let log = fs::File::create(&log_path)?;
    let executable = std::env::current_exe().context("cannot locate the pipekeep executable")?;
    let mut broker = Command::new(executable);
    broker
        .arg("__broker")
        .arg("--id")
        .arg(id)
        .arg("--session-dir")
        .arg(session_dir);
    if nobuffer {
        broker.arg("--nobuffer");
    }
    broker
        .arg("--")
        .args(command)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    unsafe {
        broker.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = match broker.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = fs::remove_dir_all(session_dir);
            return Err(error.into());
        }
    };

    let socket = runtime::socket_path(session_dir);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match UnixStream::connect(&socket).await {
            Ok(connection) => return Ok(connection),
            Err(_) if tokio::time::Instant::now() < deadline => {}
            Err(error) => {
                let _ = child.kill();
                let detail = fs::read_to_string(&log_path).unwrap_or_default();
                let _ = fs::remove_dir_all(session_dir);
                if detail.trim().is_empty() {
                    return Err(error).context("session broker did not become ready");
                }
                bail!("session broker failed: {}", detail.trim());
            }
        }
        if let Some(status) = child.try_wait()? {
            let detail = fs::read_to_string(&log_path).unwrap_or_default();
            let _ = fs::remove_dir_all(session_dir);
            if detail.trim().is_empty() {
                bail!("session broker exited during startup ({status})");
            }
            bail!("session broker failed: {}", detail.trim());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn connect_existing(session_dir: &Path) -> Result<UnixStream> {
    if !session_dir.is_dir() {
        bail!("session does not exist");
    }
    let socket = runtime::socket_path(session_dir);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match UnixStream::connect(&socket).await {
            Ok(connection) => return Ok(connection),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(error).context("cannot connect to session broker"),
        }
    }
}

struct AttachmentOpening {
    offsets: OutputOffsets,
    stdin_start: u64,
    stdin_eof: Option<u64>,
    attachment_id: String,
}

async fn proxy_attachment(
    external_input: Stdin,
    mut external_output: Stdout,
    external_error: Stderr,
    mut broker: UnixStream,
    opening: AttachmentOpening,
) -> Result<i32> {
    write_json_line(
        &mut broker,
        &serde_json::json!({
            "action": "attach",
            "offsets": opening.offsets,
            "stdin_start": opening.stdin_start,
            "stdin_eof": opening.stdin_eof,
            "attachment_id": opening.attachment_id,
        }),
    )
    .await?;

    let response = read_line(&mut broker)
        .await?
        .context("broker closed before the opening response")?;
    if serde_json::from_slice::<ErrorResponse>(&response)
        .ok()
        .and_then(|value| value.error)
        .is_some()
    {
        external_output.write_all(&response).await?;
        external_output.write_all(b"\n").await?;
        external_output.flush().await?;
        return Ok(1);
    }
    let hello: ServerHello = serde_json::from_slice(&response)
        .context("broker returned an invalid attachment header")?;
    write_json_line(&mut external_output, &hello).await?;

    // After the handshake, stdout and stderr belong to the command. A
    // pipekeep diagnostic written to either stream would be counted by the
    // public client as delivered command bytes and poison its resume offsets,
    // so an internal failure closes the attachment silently; the client sees
    // the transport end and reattaches at real byte positions.
    Ok(relay_streams(
        external_input,
        external_output,
        external_error,
        broker,
        hello,
        opening.stdin_eof,
    )
    .await
    .unwrap_or(1))
}

async fn relay_streams(
    external_input: Stdin,
    mut external_output: Stdout,
    mut external_error: Stderr,
    broker: UnixStream,
    hello: ServerHello,
    stdin_eof: Option<u64>,
) -> Result<i32> {
    let stdin_position = hello.offsets.stdin;
    let stdin_already_eof = hello.stdin_eof;
    let mut stdout_position = hello.offsets.stdout;
    let mut stderr_position = hello.offsets.stderr;
    let (mut broker_read, mut broker_write) = broker.into_split();
    let mut input = tokio::spawn(async move {
        let result = forward_raw_stdin(
            external_input,
            &mut broker_write,
            stdin_position,
            stdin_eof,
            stdin_already_eof,
        )
        .await;
        let _ = broker_write.shutdown().await;
        result
    });
    loop {
        tokio::select! {
            result = &mut input => {
                result??;
                return Ok(0);
            }
            frame = read_frame(&mut broker_read) => {
                match frame? {
                    Some(Frame::StdoutData { offset, data }) => {
                        write_raw_output(
                            &mut external_output,
                            &mut stdout_position,
                            offset,
                            &data,
                        ).await?;
                    }
                    Some(Frame::StderrData { offset, data }) => {
                        write_raw_output(
                            &mut external_error,
                            &mut stderr_position,
                            offset,
                            &data,
                        ).await?;
                    }
                    Some(Frame::StdinPosition { .. }) => {}
                    Some(Frame::Exit(result)) => {
                        input.abort();
                        external_output.flush().await?;
                        external_error.flush().await?;
                        return Ok(result.process_code());
                    }
                    Some(_) => bail!("broker sent a client-to-server frame"),
                    None => {
                        input.abort();
                        return Ok(0);
                    }
                }
            }
        }
    }
}

async fn forward_raw_stdin<W: AsyncWrite + Unpin>(
    mut input: Stdin,
    broker: &mut W,
    mut position: u64,
    eof: Option<u64>,
    already_eof: bool,
) -> Result<()> {
    if already_eof {
        return std::future::pending::<Result<()>>().await;
    }
    if eof.is_some_and(|end| position > end) {
        bail!("broker stdin position is beyond the declared EOF");
    }
    let mut buffer = vec![0_u8; 32 * 1024];
    loop {
        if eof == Some(position) {
            return std::future::pending::<Result<()>>().await;
        }
        let limit = eof
            .map(|end| (end - position) as usize)
            .unwrap_or(buffer.len())
            .min(buffer.len());
        let count = input.read(&mut buffer[..limit]).await?;
        if count == 0 {
            return Ok(());
        }
        write_frame(
            broker,
            &Frame::StdinData {
                offset: position,
                data: buffer[..count].to_vec(),
            },
        )
        .await?;
        position += count as u64;
    }
}

async fn write_raw_output<W: AsyncWrite + Unpin>(
    output: &mut W,
    position: &mut u64,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    if offset > *position {
        bail!("broker output contains a gap at byte {position}");
    }
    let skip = position.saturating_sub(offset) as usize;
    if skip >= data.len() {
        return Ok(());
    }
    output.write_all(&data[skip..]).await?;
    output.flush().await?;
    *position += (data.len() - skip) as u64;
    Ok(())
}

async fn stdin_eof_control(id: &str, offset: u64, mut output: Stdout) -> Result<i32> {
    let session_dir = runtime::session_dir(id)?;
    if !session_dir.is_dir() {
        write_typed_error_line(&mut output, "session does not exist", ERROR_SESSION_MISSING)
            .await?;
        return Ok(1);
    }
    let mut connection = match connect_existing(&session_dir).await {
        Ok(connection) => connection,
        Err(error) => {
            write_error_line(&mut output, &error.to_string()).await?;
            return Ok(1);
        }
    };
    write_json_line(
        &mut connection,
        &serde_json::json!({"action": "stdin-eof", "offset": offset}),
    )
    .await?;
    let response = read_line(&mut connection)
        .await?
        .context("broker closed before its stdin EOF response")?;
    output.write_all(&response).await?;
    output.write_all(b"\n").await?;
    output.flush().await?;
    Ok(
        if serde_json::from_slice::<ErrorResponse>(&response)
            .ok()
            .and_then(|value| value.error)
            .is_some()
        {
            1
        } else {
            0
        },
    )
}

#[derive(Deserialize)]
struct ErrorResponse {
    #[serde(default)]
    error: Option<String>,
}

pub async fn pid(id: &str) -> Result<i32> {
    let response = control_request(id, "pid").await?;
    let pid = response
        .get("pid")
        .and_then(serde_json::Value::as_i64)
        .context("broker returned an invalid PID response")?;
    println!("{pid}");
    Ok(0)
}

pub async fn cancel(id: &str) -> Result<i32> {
    let response = control_request(id, "cancel").await?;
    let result: ExitResult = serde_json::from_value(
        response
            .get("exit")
            .cloned()
            .context("broker returned an invalid cancellation response")?,
    )?;
    let outcome: CancelOutcome = serde_json::from_value(
        response
            .get("outcome")
            .cloned()
            .context("broker returned no cancellation outcome")?,
    )?;
    println!(
        "{}",
        serde_json::json!({"exit": result, "outcome": outcome})
    );
    Ok(result.process_code())
}

pub async fn detach(id: &str, attachment_id: &str) -> Result<i32> {
    let session_dir = runtime::session_dir(id)?;
    if !session_dir.is_dir() {
        println!(
            "{}",
            serde_json::json!({
                "outcome": ERROR_SESSION_MISSING,
                "error": "session does not exist",
                "code": ERROR_SESSION_MISSING,
            })
        );
        return Ok(1);
    }
    let mut connection = connect_existing(&session_dir).await?;
    write_json_line(
        &mut connection,
        &serde_json::json!({"action": "detach", "attachment_id": attachment_id}),
    )
    .await?;
    let line = read_line(&mut connection)
        .await?
        .context("broker closed before its detach response")?;
    let response: serde_json::Value = serde_json::from_slice(&line)?;
    println!("{response}");
    if response
        .get("error")
        .and_then(serde_json::Value::as_str)
        .is_some()
    {
        Ok(1)
    } else {
        Ok(0)
    }
}

async fn control_request(id: &str, action: &str) -> Result<serde_json::Value> {
    let session_dir = runtime::session_dir(id)?;
    let mut connection = connect_existing(&session_dir).await?;
    write_json_line(&mut connection, &serde_json::json!({"action": action})).await?;
    let line = read_line(&mut connection)
        .await?
        .context("broker closed before its control response")?;
    let response: serde_json::Value = serde_json::from_slice(&line)?;
    if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        bail!("{error}");
    }
    Ok(response)
}

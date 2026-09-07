use crate::protocol::{
    checked_end, read_frame, read_line, write_error_line, write_frame, write_json_line,
    BrokerRequest, CancelOutcome, ClientAction, ClientHello, Direction, ExitResult, Frame,
    OutputOffsets, ServerHello, FRAMED_ATTACHMENT_V1,
};
use crate::runtime;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, Stderr, Stdin, Stdout};
use tokio::net::UnixStream;

struct AttachmentOpen {
    offsets: OutputOffsets,
    stdin_start: u64,
    stdin_eof: Option<u64>,
    force: bool,
    framed: bool,
}

pub async fn run(
    id: String,
    command: Vec<String>,
    nobuffer: bool,
    force: bool,
    group_pidfd: bool,
    framed: bool,
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
    let force = force || hello.force;
    if group_pidfd && (hello.offsets.is_some() || hello.action.is_some()) {
        write_error_line(
            &mut output,
            "--group-pidfd is only for new sessions; existing sessions cannot be retrofitted",
        )
        .await?;
        return Ok(1);
    }
    if framed && hello.action.is_some() {
        write_error_line(
            &mut output,
            "--framed is for data attachments; use the ordinary EOF control",
        )
        .await?;
        return Ok(1);
    }
    if matches!(hello.action, Some(ClientAction::StdinEof)) {
        if force {
            write_error_line(&mut output, "force is only supported for data attachments").await?;
            return Ok(1);
        }
        let Some(offset) = hello.stdin_eof else {
            write_error_line(&mut output, "stdin-eof requires stdin_eof").await?;
            return Ok(1);
        };
        return stdin_eof_control(&id, offset, output).await;
    }
    let creating = hello.offsets.is_none();
    if creating && force {
        write_error_line(
            &mut output,
            "force requires attach-only offsets and cannot create a session",
        )
        .await?;
        return Ok(1);
    }
    if !force && hello.stdin_eof.is_some_and(|eof| eof < hello.stdin_start) {
        write_error_line(&mut output, "stdin_eof is before stdin_start").await?;
        return Ok(1);
    }
    let offsets = hello.offsets.unwrap_or_default();
    let session_dir = runtime::session_dir(&id)?;

    let connection = if creating {
        match start_broker(&id, &session_dir, &command, nobuffer, group_pidfd).await {
            Ok(connection) => connection,
            Err(error) => {
                write_error_line(&mut output, &error.to_string()).await?;
                return Ok(1);
            }
        }
    } else {
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
        AttachmentOpen {
            offsets,
            stdin_start: hello.stdin_start,
            stdin_eof: hello.stdin_eof,
            force,
            framed,
        },
    )
    .await
}

async fn start_broker(
    id: &str,
    session_dir: &Path,
    command: &[String],
    nobuffer: bool,
    group_pidfd: bool,
) -> Result<UnixStream> {
    if group_pidfd {
        // Reject a known incompatible inherited policy before session
        // allocation or broker dispatch. Only the broker can probe its group.
        crate::group_pidfd::GroupPidfd::check_sigchld_policy().map_err(|error| {
            anyhow::anyhow!("group pidfd unsupported; workload not started: {error}")
        })?;
    }
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
    if group_pidfd {
        broker.arg("--group-pidfd");
    }
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
                if group_pidfd {
                    // Dispatch may already have happened. Do not kill the
                    // authority owner or remove its session on a ready timeout.
                    bail!("session broker readiness unknown after dispatch; session retained; workload may have started; query the same session, do not retry creation");
                }
                let _ = child.kill();
                let detail = fs::read_to_string(&log_path).unwrap_or_default();
                let _ = fs::remove_dir_all(session_dir);
                if detail.trim().is_empty() {
                    return Err(error).context("session broker did not become ready");
                }
                bail!("session broker failed: {}", detail.trim());
            }
        }
        let status = child.try_wait().map_err(|error| {
            if group_pidfd {
                // Even ECHILD is not observed exit or pre-dispatch rejection.
                // Preserve the broker/session; never infer cleanup authority.
                anyhow::anyhow!("session broker wait failed after dispatch: {error}; session retained; workload may have started; query the same session, do not retry creation")
            } else {
                error.into()
            }
        })?;
        if let Some(status) = status {
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

async fn proxy_attachment(
    external_input: Stdin,
    mut external_output: Stdout,
    external_error: Stderr,
    mut broker: UnixStream,
    opening: AttachmentOpen,
) -> Result<i32> {
    let request = if opening.framed {
        BrokerRequest::AttachFramedV1 {
            offsets: opening.offsets,
            stdin_start: opening.stdin_start,
            stdin_eof: opening.stdin_eof,
            force: opening.force,
        }
    } else {
        BrokerRequest::Attach {
            offsets: opening.offsets,
            stdin_start: opening.stdin_start,
            stdin_eof: opening.stdin_eof,
            force: opening.force,
        }
    };
    write_json_line(&mut broker, &request).await?;

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
    if opening.framed
        && (hello.attachment.as_deref() != Some(FRAMED_ATTACHMENT_V1) || hello.nobuffer)
    {
        bail!(
            "broker did not confirm buffered framed-v1 attachment; no fallback; session retained"
        );
    }
    if opening.framed {
        if let Some(exit) = &hello.exit {
            exit.validate()?;
        }
    }
    write_json_line(&mut external_output, &hello).await?;

    // After the handshake, raw streams belong to the command and framed
    // stdout belongs to the encoder. Never inject diagnostics into either
    // wire format: failure detaches, and clients recover the same session.
    Ok(relay_streams(
        external_input,
        external_output,
        external_error,
        broker,
        hello,
        opening.stdin_eof,
        opening.framed,
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
    framed: bool,
) -> Result<i32> {
    let mut stdin_position = hello.offsets.stdin;
    let mut stdout_position = hello.offsets.stdout;
    let mut stderr_position = hello.offsets.stderr;
    let (mut broker_read, mut broker_write) = broker.into_split();
    // Both futures live in this scope. On ANY completion or error their
    // pending reads/writes are dropped; there is no detached forwarding task.
    let input = async {
        if framed {
            let mut input = external_input;
            while let Some(frame) = read_frame(&mut input, Direction::Input).await? {
                write_frame(&mut broker_write, &frame).await?;
            }
            Ok(1) // transport end only; never synthesize a StdinEof frame
        } else {
            forward_raw_stdin(
                external_input,
                &mut broker_write,
                hello.offsets.stdin,
                stdin_eof,
                hello.stdin_eof,
            )
            .await?;
            Ok(0)
        }
    };
    let output = async {
        loop {
            let Some(frame) = read_frame(&mut broker_read, Direction::Output).await? else {
                return Ok(if framed { 1 } else { 0 });
            };
            if framed {
                match &frame {
                    Frame::StdoutData { offset, data } | Frame::StderrData { offset, data } => {
                        let position = if matches!(&frame, Frame::StdoutData { .. }) {
                            &mut stdout_position
                        } else {
                            &mut stderr_position
                        };
                        if *offset != *position {
                            bail!("broker output is not contiguous");
                        }
                        *position = checked_end(*offset, data.len())?;
                    }
                    Frame::StdinPosition { offset } => {
                        if *offset < stdin_position {
                            bail!("broker stdin receipt regressed");
                        }
                        stdin_position = *offset;
                    }
                    _ => {}
                }
                write_frame(&mut external_output, &frame).await?;
                if let Frame::Exit(result) = frame {
                    return Ok(result.process_code());
                }
            } else {
                match frame {
                    Frame::StdoutData { offset, data } => {
                        write_raw_output(&mut external_output, &mut stdout_position, offset, &data)
                            .await?;
                    }
                    Frame::StderrData { offset, data } => {
                        write_raw_output(&mut external_error, &mut stderr_position, offset, &data)
                            .await?;
                    }
                    Frame::StdinPosition { .. } => {}
                    Frame::Exit(result) => {
                        external_output.flush().await?;
                        external_error.flush().await?;
                        return Ok(result.process_code());
                    }
                    _ => bail!("broker sent a client-to-server frame"),
                }
            }
        }
    };
    tokio::select! {
        result = input => result,
        result = output => result,
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
            .map(|end| (end - position).min(buffer.len() as u64) as usize)
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
        position = checked_end(position, count)?;
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
    let skip = usize::try_from(position.saturating_sub(offset)).unwrap_or(usize::MAX);
    if skip >= data.len() {
        return Ok(());
    }
    output.write_all(&data[skip..]).await?;
    output.flush().await?;
    *position = checked_end(*position, data.len() - skip)?;
    Ok(())
}

async fn stdin_eof_control(id: &str, offset: u64, mut output: Stdout) -> Result<i32> {
    let session_dir = runtime::session_dir(id)?;
    let mut connection = match connect_existing(&session_dir).await {
        Ok(connection) => connection,
        Err(error) => {
            write_error_line(&mut output, &error.to_string()).await?;
            return Ok(1);
        }
    };
    write_json_line(&mut connection, &BrokerRequest::StdinEof { offset }).await?;
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

pub async fn session(id: &str) -> Result<i32> {
    println!("{}", control_request(id, "session").await?);
    Ok(0)
}

pub async fn cancel(id: &str, require_group_pidfd: bool) -> Result<i32> {
    let response = control_request(
        id,
        if require_group_pidfd {
            "cancel-group-pidfd"
        } else {
            "cancel"
        },
    )
    .await?;
    if require_group_pidfd
        && !response["session"]["capabilities"]
            .as_array()
            .is_some_and(|caps| {
                caps.iter()
                    .any(|cap| cap == crate::protocol::GROUP_PIDFD_CAPABILITY)
            })
    {
        bail!("broker did not verify group-pidfd-cancel-v1 for this session");
    }
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
    let _ = outcome; // Validate existing wire outcome before forwarding additive facts.
    println!("{response}");
    Ok(result.process_code())
}

async fn control_request(id: &str, action: &str) -> Result<serde_json::Value> {
    let session_dir = runtime::session_dir(id)?;
    let mut connection = connect_existing(&session_dir).await?;
    let request = match action {
        "pid" => BrokerRequest::Pid,
        "cancel" => BrokerRequest::Cancel,
        "cancel-group-pidfd" => BrokerRequest::CancelGroupPidfd,
        "session" => BrokerRequest::Session,
        _ => bail!("unsupported control request {action:?}"),
    };
    write_json_line(&mut connection, &request).await?;
    let line = read_line(&mut connection)
        .await?
        .context("broker closed before its control response")?;
    let response: serde_json::Value = serde_json::from_slice(&line)?;
    if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        bail!("{error}");
    }
    Ok(response)
}

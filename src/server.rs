use crate::protocol::{
    read_line, write_error_line, write_json_line, ClientHello, ExitResult, OutputOffsets,
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
use tokio::io::{AsyncWriteExt, Stdin, Stdout};
use tokio::net::UnixStream;

pub async fn run(id: String, command: Vec<String>, nobuffer: bool) -> Result<i32> {
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
    let creating = hello.offsets.is_none();
    let offsets = hello.offsets.unwrap_or_default();
    let session_dir = runtime::session_dir(&id)?;

    let connection = if creating {
        match start_broker(&id, &session_dir, &command, nobuffer).await {
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

    proxy_attachment(input, output, connection, offsets).await?;
    Ok(0)
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
    let executable = std::env::current_exe().context("cannot locate the rpipe executable")?;
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

async fn proxy_attachment(
    mut external_input: Stdin,
    mut external_output: Stdout,
    mut broker: UnixStream,
    offsets: OutputOffsets,
) -> Result<()> {
    write_json_line(
        &mut broker,
        &serde_json::json!({"action": "attach", "offsets": offsets}),
    )
    .await?;

    let response = read_line(&mut broker)
        .await?
        .context("broker closed before the opening response")?;
    external_output.write_all(&response).await?;
    external_output.write_all(b"\n").await?;
    external_output.flush().await?;
    if serde_json::from_slice::<ErrorResponse>(&response)
        .ok()
        .and_then(|value| value.error)
        .is_some()
    {
        return Ok(());
    }

    let (mut broker_read, mut broker_write) = broker.into_split();
    let input = tokio::spawn(async move {
        let result = tokio::io::copy(&mut external_input, &mut broker_write).await;
        let _ = broker_write.shutdown().await;
        result
    });
    let output = tokio::spawn(async move {
        let result = tokio::io::copy(&mut broker_read, &mut external_output).await;
        let _ = external_output.flush().await;
        result
    });

    // Broker closure commonly makes the input copy see EPIPE just before the
    // final output/exit bytes are drained. Output owns attachment completion;
    // input errors merely describe the same disconnect and must not truncate
    // those final bytes or leak into the command's stderr.
    let result = output.await?;
    input.abort();
    result?;
    Ok(())
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
    Ok(result.process_code())
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

use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

use crate::util::{wait_for_file, wait_for_group_gone, Cleanup};

pub(crate) fn verify_cancellation(
    pipekeep: &Path,
    runtime: &Path,
    scripts: &Path,
    cleanup: &mut Cleanup,
) -> Result<()> {
    let id = format!("acceptance-cancel-{}", std::process::id());
    cleanup.track(id.clone());
    let script = scripts.join("cancel.sh");
    let child_pid_file = scripts.join("cancel-child.pid");
    fs::write(
        &script,
        format!(
            r#"trap '' TERM
(trap '' TERM; echo "$$" > '{}'; while :; do sleep 1; done) &
printf ready
while :; do sleep 1; done
"#,
            child_pid_file.display()
        ),
    )?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let mut first = spawn_direct_attach(
        pipekeep,
        runtime,
        &id,
        &script,
        &[("PIPEKEEP_CANCEL_GRACE_SECS", "0")],
        false,
    )?;
    write_request(&mut first, &serde_json::json!({"stdin_eof": 0}))?;
    let mut stdout = first.stdout.take().context("cancel attach has no stdout")?;
    let header = read_json_line(&mut stdout)?;
    ensure_no_error(&header)?;
    let mut ready = [0_u8; 5];
    stdout.read_exact(&mut ready)?;
    if ready != *b"ready" {
        bail!("cancel workload did not report ready");
    }
    wait_for_file(&child_pid_file, Duration::from_secs(5))?;
    let pid_output = Command::new(pipekeep)
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .args(["pid", "--id", &id])
        .output()?;
    if !pid_output.status.success() {
        bail!(
            "pid failed: {}",
            String::from_utf8_lossy(&pid_output.stderr)
        );
    }
    let pgid: i32 = String::from_utf8(pid_output.stdout)?
        .trim()
        .parse()
        .context("pid output was not an integer")?;

    let cancel = Command::new(pipekeep)
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .env("PIPEKEEP_CANCEL_GRACE_SECS", "0")
        .args(["cancel", "--id", &id])
        .output()?;
    let line = String::from_utf8(cancel.stdout)?;
    let lines: Vec<&str> = line.lines().collect();
    if lines.len() != 1 {
        bail!("cancel did not print one JSON line: {line:?}");
    }
    let response: Value = serde_json::from_str(lines[0])?;
    if response.get("outcome").and_then(Value::as_str) != Some("cancel_won") {
        bail!("unexpected cancel outcome: {response}");
    }
    if response
        .get("exit")
        .and_then(|exit| exit.get("signal"))
        .and_then(Value::as_i64)
        != Some(9)
    {
        bail!("cancel did not report authoritative SIGKILL: {response}");
    }
    if cancel.status.code() != Some(137) {
        bail!("cancel process status was {}, expected 137", cancel.status);
    }
    wait_for_group_gone(pgid, Duration::from_secs(5))?;
    let _ = first.wait();
    println!("ok: cancellation JSON and process-group termination verified");
    Ok(())
}

pub(crate) fn verify_negative_cases(
    pipekeep: &Path,
    runtime: &Path,
    scripts: &Path,
    cleanup: &mut Cleanup,
) -> Result<()> {
    verify_concurrent_attachment(pipekeep, runtime, scripts, cleanup)?;
    verify_missing_session(pipekeep, runtime, scripts)?;
    verify_ahead_offset(pipekeep, runtime, scripts, cleanup)?;
    verify_stale_nobuffer_replay(pipekeep, runtime, scripts, cleanup)?;
    verify_replay_expiry(pipekeep, runtime, scripts, cleanup)?;
    println!("ok: negative cases verified");
    Ok(())
}

fn verify_concurrent_attachment(
    pipekeep: &Path,
    runtime: &Path,
    scripts: &Path,
    cleanup: &mut Cleanup,
) -> Result<()> {
    let id = format!("acceptance-concurrent-{}", std::process::id());
    cleanup.track(id.clone());
    let release = scripts.join("concurrent.release");
    let script = scripts.join("concurrent.sh");
    fs::write(
        &script,
        format!(
            "printf held; while [ ! -e '{}' ]; do sleep 0.05; done\n",
            release.display()
        ),
    )?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let mut first = spawn_direct_attach(pipekeep, runtime, &id, &script, &[], false)?;
    write_request(&mut first, &serde_json::json!({"stdin_eof": 0}))?;
    let mut stdout = first.stdout.take().context("first attach has no stdout")?;
    let header = read_json_line(&mut stdout)?;
    ensure_no_error(&header)?;
    let mut held = [0_u8; 4];
    stdout.read_exact(&mut held)?;
    if held != *b"held" {
        bail!("concurrent holder did not start");
    }

    let output = direct_attach_output(
        pipekeep,
        runtime,
        &id,
        &script,
        &serde_json::json!({"offsets": {"stdout": 0, "stderr": 0}}),
        &[],
        false,
    )?;
    expect_error(&output, "already has an attached client")?;
    fs::write(release, b"")?;
    let status = first.wait()?;
    if !status.success() {
        bail!("concurrent holder exited with {status}");
    }
    Ok(())
}

fn verify_missing_session(pipekeep: &Path, runtime: &Path, scripts: &Path) -> Result<()> {
    let script = scripts.join("missing-unused.sh");
    fs::write(&script, "exit 99\n")?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let output = direct_attach_output(
        pipekeep,
        runtime,
        "acceptance-missing-session",
        &script,
        &serde_json::json!({"offsets": {"stdout": 0, "stderr": 0}}),
        &[],
        false,
    )?;
    expect_error(&output, "session does not exist")
}

fn verify_ahead_offset(
    pipekeep: &Path,
    runtime: &Path,
    scripts: &Path,
    cleanup: &mut Cleanup,
) -> Result<()> {
    let id = format!("acceptance-ahead-{}", std::process::id());
    cleanup.track(id.clone());
    let script = scripts.join("ahead.sh");
    fs::write(&script, "printf abc; printf err >&2\n")?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let output = direct_attach_output(
        pipekeep,
        runtime,
        &id,
        &script,
        &serde_json::json!({"stdin_eof": 0}),
        &[("PIPEKEEP_SESSION_TTL_SECS", "30")],
        false,
    )?;
    expect_success(&output)?;
    let output = direct_attach_output(
        pipekeep,
        runtime,
        &id,
        &script,
        &serde_json::json!({"offsets": {"stdout": 999, "stderr": 0}}),
        &[],
        false,
    )?;
    expect_error(&output, "requested output offset is ahead of the stream")
}

fn verify_stale_nobuffer_replay(
    pipekeep: &Path,
    runtime: &Path,
    scripts: &Path,
    cleanup: &mut Cleanup,
) -> Result<()> {
    let id = format!("acceptance-stale-{}", std::process::id());
    cleanup.track(id.clone());
    let gate = scripts.join("stale.gate");
    let done = scripts.join("stale.done");
    let script = scripts.join("stale.sh");
    fs::write(
        &script,
        format!(
            "while [ ! -e '{}' ]; do sleep 0.05; done; printf stale-out; printf stale-err >&2; touch '{}'\n",
            gate.display(),
            done.display()
        ),
    )?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;

    let mut first = spawn_direct_attach(pipekeep, runtime, &id, &script, &[], true)?;
    write_request(&mut first, &serde_json::json!({"stdin_eof": 0}))?;
    let mut stdout = first.stdout.take().context("stale attach has no stdout")?;
    let header = read_json_line(&mut stdout)?;
    ensure_no_error(&header)?;
    first.kill()?;
    let _ = first.wait();

    fs::write(gate, b"")?;
    wait_for_file(&done, Duration::from_secs(5))?;
    let output = direct_attach_output(
        pipekeep,
        runtime,
        &id,
        &script,
        &serde_json::json!({"offsets": {"stdout": 0, "stderr": 0}}),
        &[],
        true,
    )?;
    expect_success(&output)?;
    let (header, body) = split_header(&output.stdout)?;
    assert_json_u64(header.get("offsets").context("no offsets")?, "stdout", 9)?;
    assert_json_u64(header.get("offsets").context("no offsets")?, "stderr", 9)?;
    if !body.is_empty() || !output.stderr.is_empty() {
        bail!("stale nobuffer replay unexpectedly returned discarded bytes");
    }
    Ok(())
}

fn verify_replay_expiry(
    pipekeep: &Path,
    runtime: &Path,
    scripts: &Path,
    cleanup: &mut Cleanup,
) -> Result<()> {
    let id = format!("acceptance-expiry-{}", std::process::id());
    cleanup.track(id.clone());
    let script = scripts.join("expiry.sh");
    fs::write(&script, "printf ttl\n")?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let output = direct_attach_output(
        pipekeep,
        runtime,
        &id,
        &script,
        &serde_json::json!({"stdin_eof": 0}),
        &[("PIPEKEEP_SESSION_TTL_SECS", "1")],
        false,
    )?;
    expect_success(&output)?;
    thread::sleep(Duration::from_secs(2));
    let output = direct_attach_output(
        pipekeep,
        runtime,
        &id,
        &script,
        &serde_json::json!({"offsets": {"stdout": 0, "stderr": 0}}),
        &[],
        false,
    )?;
    expect_error(&output, "session does not exist")
}

fn spawn_direct_attach(
    pipekeep: &Path,
    runtime: &Path,
    id: &str,
    script: &Path,
    envs: &[(&str, &str)],
    nobuffer: bool,
) -> Result<Child> {
    let mut command = Command::new(pipekeep);
    command.env("PIPEKEEP_RUNTIME_DIR", runtime);
    command.env("PIPEKEEP_SESSION_TTL_SECS", "30");
    for (key, value) in envs {
        command.env(key, value);
    }
    command.args(["--id", id]);
    if nobuffer {
        command.arg("--nobuffer");
    }
    command
        .arg("--")
        .arg("/bin/sh")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("cannot spawn direct attach for {id}"))
}

fn write_request(child: &mut Child, request: &Value) -> Result<()> {
    let mut stdin = child.stdin.take().context("child has no stdin")?;
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    stdin.write_all(&bytes)?;
    Ok(())
}

fn direct_attach_output(
    pipekeep: &Path,
    runtime: &Path,
    id: &str,
    script: &Path,
    request: &Value,
    envs: &[(&str, &str)],
    nobuffer: bool,
) -> Result<std::process::Output> {
    let mut child = spawn_direct_attach(pipekeep, runtime, id, script, envs, nobuffer)?;
    write_request(&mut child, request)?;
    child.wait_with_output().context("direct attach failed")
}

fn expect_success(output: &std::process::Output) -> Result<()> {
    if !output.status.success() {
        bail!(
            "expected success, got {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let (header, _) = split_header(&output.stdout)?;
    ensure_no_error(&header)
}

fn expect_error(output: &std::process::Output, expected: &str) -> Result<()> {
    if output.status.success() {
        bail!(
            "expected error containing {expected:?}, got success\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let (header, _) = split_header(&output.stdout)?;
    let error = header
        .get("error")
        .and_then(Value::as_str)
        .context("error response did not include an error field")?;
    if !error.contains(expected) {
        bail!("expected error containing {expected:?}, got {error:?}");
    }
    Ok(())
}

fn ensure_no_error(header: &Value) -> Result<()> {
    if let Some(error) = header.get("error").and_then(Value::as_str) {
        bail!("unexpected error response: {error}");
    }
    Ok(())
}

fn split_header(stdout: &[u8]) -> Result<(Value, &[u8])> {
    let end = stdout
        .iter()
        .position(|byte| *byte == b'\n')
        .context("stdout did not contain a JSON header")?;
    let header: Value = serde_json::from_slice(&stdout[..end])?;
    let body = &stdout[end + 1..];
    Ok((header, body))
}

fn read_json_line(reader: &mut impl Read) -> Result<Value> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte)?;
        if byte[0] == b'\n' {
            return serde_json::from_slice(&line).context("invalid JSON line");
        }
        line.push(byte[0]);
        if line.len() > 64 * 1024 {
            bail!("JSON line is too large");
        }
    }
}

fn assert_json_u64(object: &Value, key: &str, expected: u64) -> Result<()> {
    let actual = object
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing integer field {key:?} in {object}"))?;
    if actual != expected {
        bail!("expected {key} offset {expected}, got {actual}");
    }
    Ok(())
}

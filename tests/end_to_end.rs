use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

fn binary() -> PathBuf {
    assert_cmd::cargo::cargo_bin("pipekeep")
}

fn runtime_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("pipekeep-{label}-"))
        .tempdir_in("/tmp")
        .unwrap()
}

fn attachment(runtime: &Path, id: &str, shell: &str) -> std::process::Child {
    Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .args(["--id", id, "--", "/bin/sh", "-c", shell])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn forced_attachment(runtime: &Path, id: &str, shell: &str) -> std::process::Child {
    Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .args(["--id", id, "--force", "--", "/bin/sh", "-c", shell])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn request(value: serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn write_request(child: &mut Child, value: serde_json::Value) {
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&request(value))
        .unwrap();
    child.stdin.as_mut().unwrap().flush().unwrap();
}

fn read_json_line<R: Read>(reader: &mut R) -> serde_json::Value {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    serde_json::from_slice(&line).unwrap()
}

fn wait_exited(child: &mut Child, label: &str) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "{label} did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_displaced(child: &mut Child, label: &str) {
    let _ = wait_exited(child, label);
}

fn session_socket(runtime: &Path) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for entry in fs::read_dir(runtime).unwrap() {
            let socket = entry.unwrap().path().join("broker.sock");
            if socket.exists() {
                return socket;
            }
        }
        assert!(Instant::now() < deadline, "broker socket did not appear");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn pid_text(runtime: &Path, id: &str) -> String {
    let output = Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .args(["pid", "--id", id])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

fn split_header(stdout: &[u8]) -> (serde_json::Value, &[u8]) {
    let end = stdout.iter().position(|byte| *byte == b'\n').unwrap();
    (
        serde_json::from_slice(&stdout[..end]).unwrap(),
        &stdout[end + 1..],
    )
}

// A session-creating attachment whose broker inherits an explicit
// cancellation grace period; the TTL keeps the settled session around for
// the control requests that follow.
fn cancellable_attachment(
    runtime: &Path,
    id: &str,
    shell: &str,
    grace_secs: &str,
) -> std::process::Child {
    Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .env("PIPEKEEP_CANCEL_GRACE_SECS", grace_secs)
        .args(["--id", id, "--", "/bin/sh", "-c", shell])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn cancel_json(runtime: &Path, id: &str) -> (std::process::ExitStatus, serde_json::Value) {
    let output = Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .args(["cancel", "--id", id])
        .output()
        .unwrap();
    assert!(
        output.stderr.is_empty(),
        "cancel stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        text.lines().count(),
        1,
        "cancel must print one machine-readable line: {text:?}"
    );
    (output.status, serde_json::from_str(&text).unwrap())
}

fn outer_command(runtime: &Path, id: &str, shell: &str) -> Command {
    let binary = binary();
    let mut command = Command::new(&binary);
    command
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", "2")
        .arg("--")
        .arg(&binary)
        .arg("--id")
        .arg(id)
        .arg("--")
        .arg("/bin/sh")
        .arg("-c")
        .arg(shell);
    command
}

#[test]
fn remote_protocol_exposes_raw_standard_streams() {
    let runtime = runtime_dir("raw-protocol");
    let binary = binary();
    let input = b"line one\n\0line two";
    let mut child = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "2")
        .args(["--id", "raw-protocol", "--", "/bin/sh", "-c"])
        .arg("cat; printf raw-error >&2; exit 9")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let request = serde_json::json!({"stdin_eof": input.len()});
    let mut wire_input = serde_json::to_vec(&request).unwrap();
    wire_input.push(b'\n');
    wire_input.extend_from_slice(input);
    child.stdin.take().unwrap().write_all(&wire_input).unwrap();
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(9));
    let header_end = output
        .stdout
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap();
    let header: serde_json::Value = serde_json::from_slice(&output.stdout[..header_end]).unwrap();
    assert_eq!(header["offsets"]["stdin"], 0);
    assert_eq!(&output.stdout[header_end + 1..], input);
    assert_eq!(output.stderr, b"raw-error");
}

#[test]
fn stdin_eof_control_is_sticky_and_idempotent() {
    let runtime = runtime_dir("stdin-eof-control");
    let binary = binary();
    let mut attachment = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "2")
        .args([
            "--id",
            "stdin-eof-control",
            "--",
            "/bin/sh",
            "-c",
            "cat; printf done",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut attachment_input = attachment.stdin.take().unwrap();
    let mut attachment_output = attachment.stdout.take().unwrap();
    attachment_input.write_all(b"{}\nabc").unwrap();
    attachment_input.flush().unwrap();

    let mut response = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        attachment_output.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        response.push(byte[0]);
    }
    let header: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(header["offsets"]["stdin"], 0);

    for attempt in 0..2 {
        let mut control = Command::new(&binary)
            .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
            .args([
                "--id",
                "stdin-eof-control",
                "--",
                "/bin/sh",
                "-c",
                "exit 99",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let request = serde_json::json!({"action":"stdin-eof", "stdin_eof":3});
        let mut request = serde_json::to_vec(&request).unwrap();
        request.push(b'\n');
        control.stdin.take().unwrap().write_all(&request).unwrap();
        let output = control.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "control attempt {attempt}, status: {}, stdout: {}, stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(response["stdin_eof_at"], 3);
    }

    let mut raw_output = Vec::new();
    attachment_output.read_to_end(&mut raw_output).unwrap();
    let status = attachment.wait().unwrap();
    drop(attachment_input);
    assert!(status.success());
    assert_eq!(raw_output, b"abcdone");
}

#[test]
fn carries_stdin_stdout_stderr_and_exit_status() {
    let runtime = runtime_dir("streams");
    let mut child = outer_command(
        runtime.path(),
        "streams",
        "cat; printf 'remote error' >&2; exit 7",
    )
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"stream input")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(7),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"stream input");
    assert_eq!(output.stderr, b"remote error");
}

#[test]
fn reconnects_after_transport_process_is_killed() {
    let runtime = runtime_dir("resume");
    let binary = binary();
    let script = runtime.path().join("transport.sh");
    let flag = runtime.path().join("first-attempt");
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
if mkdir '{}' 2>/dev/null; then
  '{}' --id resume -- /bin/sh -c 'printf first; sleep 1; printf second' &
  child=$!
  (sleep 0.2; kill "$child" 2>/dev/null) &
  wait "$child"
else
  exec '{}' --id resume -- /bin/sh -c 'printf first; sleep 1; printf second'
fi
"#,
            flag.display(),
            binary.display(),
            binary.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();

    let output = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "2")
        .arg("--")
        .arg(&script)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"firstsecond");
}

#[test]
fn pid_and_cancel_control_the_command_group() {
    let runtime = runtime_dir("cancel");
    let binary = binary();
    let mut outer = outer_command(runtime.path(), "cancel-me", "printf R; sleep 30")
        .env("PIPEKEEP_CANCEL_GRACE_SECS", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut byte = [0_u8; 1];
    outer
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut byte)
        .unwrap();
    assert_eq!(&byte, b"R");

    let pid = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .args(["pid", "--id", "cancel-me"])
        .output()
        .unwrap();
    assert!(pid.status.success());
    assert!(String::from_utf8(pid.stdout)
        .unwrap()
        .trim()
        .parse::<u32>()
        .is_ok());

    let (canceled, response) = cancel_json(runtime.path(), "cancel-me");
    // The leader accepted SIGTERM before escalation, so its authoritative
    // result is 128 + SIGTERM rather than being relabeled as canceled.
    assert_eq!(canceled.code(), Some(143));
    assert_eq!(response["outcome"], "cancel_won");
    assert_eq!(response["exit"]["signal"], 15);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = outer.try_wait().unwrap() {
            assert_eq!(status.code(), Some(143));
            break;
        }
        assert!(Instant::now() < deadline, "outer pipekeep did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn cancel_after_natural_completion_reports_already_exited() {
    let runtime = runtime_dir("cancel-exited");
    let mut child =
        cancellable_attachment(runtime.path(), "cancel-exited", "printf done; exit 5", "5");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"stdin_eof\":0}\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    // The attachment saw the terminal result, so the leader is reaped and
    // the group settled before the cancellation below signals anything.
    assert_eq!(output.status.code(), Some(5));
    let (_, body) = split_header(&output.stdout);
    assert_eq!(body, b"done");

    let (status, response) = cancel_json(runtime.path(), "cancel-exited");
    assert_eq!(response["outcome"], "already_exited");
    // The retained natural result passes through unchanged.
    assert_eq!(response["exit"]["code"], 5);
    assert!(response["exit"].get("signal").is_none());
    assert_eq!(status.code(), Some(5));
}

#[test]
fn term_delivered_cancellation_reports_cancel_won() {
    let runtime = runtime_dir("cancel-term");
    let mut child =
        cancellable_attachment(runtime.path(), "cancel-term", "printf R; sleep 30", "5");
    let mut input = child.stdin.take().unwrap();
    input.write_all(b"{}\n").unwrap();
    input.flush().unwrap();
    let mut output_pipe = child.stdout.take().unwrap();
    read_json_line(&mut output_pipe);
    let mut byte = [0_u8; 1];
    output_pipe.read_exact(&mut byte).unwrap();
    assert_eq!(&byte, b"R");

    let (status, response) = cancel_json(runtime.path(), "cancel-term");
    assert_eq!(response["outcome"], "cancel_won");
    assert_eq!(response["exit"]["signal"], 15);
    assert!(response["exit"].get("code").is_none());
    assert_eq!(status.code(), Some(143));

    // The attached client receives the same retained result.
    assert_eq!(child.wait().unwrap().code(), Some(143));
    drop(input);
}

#[test]
fn kill_escalation_after_ignored_term_reports_cancel_won() {
    let runtime = runtime_dir("cancel-kill");
    // The leader ignores TERM and keeps its group alive past the grace
    // period, forcing the KILL escalation.
    let mut child = cancellable_attachment(
        runtime.path(),
        "cancel-kill",
        "trap '' TERM; printf R; while :; do sleep 1; done",
        "1",
    );
    let mut input = child.stdin.take().unwrap();
    input.write_all(b"{}\n").unwrap();
    input.flush().unwrap();
    let mut output_pipe = child.stdout.take().unwrap();
    read_json_line(&mut output_pipe);
    let mut byte = [0_u8; 1];
    output_pipe.read_exact(&mut byte).unwrap();
    assert_eq!(&byte, b"R");

    let (status, response) = cancel_json(runtime.path(), "cancel-kill");
    assert_eq!(response["outcome"], "cancel_won");
    assert_eq!(response["exit"]["signal"], 9);
    assert!(response["exit"].get("code").is_none());
    assert_eq!(status.code(), Some(137));

    assert_eq!(child.wait().unwrap().code(), Some(137));
    drop(input);
}

#[test]
fn cancel_of_settled_group_never_fabricates_a_win() {
    let runtime = runtime_dir("cancel-gone");
    let mut child =
        cancellable_attachment(runtime.path(), "cancel-gone", "printf R; sleep 30", "5");
    let mut input = child.stdin.take().unwrap();
    input.write_all(b"{}\n").unwrap();
    input.flush().unwrap();
    let mut output_pipe = child.stdout.take().unwrap();
    read_json_line(&mut output_pipe);
    let mut byte = [0_u8; 1];
    output_pipe.read_exact(&mut byte).unwrap();
    assert_eq!(&byte, b"R");

    let (status, first) = cancel_json(runtime.path(), "cancel-gone");
    assert_eq!(first["outcome"], "cancel_won");
    assert_eq!(status.code(), Some(143));
    assert_eq!(child.wait().unwrap().code(), Some(143));
    drop(input);

    // The recorded process group is now fully settled and reaped, so this
    // cancellation's signal attempt reaches nobody. The outcome must resolve
    // from that failed attempt — already_exited, with the retained
    // signal-termination result untouched rather than relabeled.
    let (status, second) = cancel_json(runtime.path(), "cancel-gone");
    assert_eq!(second["outcome"], "already_exited");
    assert_eq!(second["exit"]["signal"], 15);
    assert!(second["exit"].get("code").is_none());
    assert_eq!(status.code(), Some(143));
}

#[test]
fn nobuffer_mode_preserves_live_streams() {
    let runtime = runtime_dir("nobuffer");
    let binary = binary();
    let mut child = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "2")
        .args(["--nobuffer", "--"])
        .arg(&binary)
        .args([
            "--id",
            "nobuffer",
            "--nobuffer",
            "--",
            "/bin/sh",
            "-c",
            "cat; printf live-error >&2",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"live-input")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"live-input");
    assert_eq!(output.stderr, b"live-error");
}

#[test]
fn version_reports_package_version_and_revision() {
    let output = Command::new(binary()).arg("--version").output().unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let expected_prefix = format!("pipekeep {} (", env!("CARGO_PKG_VERSION"));
    assert!(
        text.starts_with(&expected_prefix) && text.trim_end().ends_with(')'),
        "unexpected version line: {text:?}"
    );
    let revision = &text.trim_end()[expected_prefix.len()..text.trim_end().len() - 1];
    assert!(!revision.is_empty());
    assert!(!revision.contains(char::is_whitespace));
}

#[test]
fn capabilities_probe_is_machine_readable_and_strict() {
    let output = Command::new(binary())
        .args(["capabilities", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(text.lines().count(), 1, "probe must be one compact line");
    let probe: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(probe["name"], "pipekeep");
    assert_eq!(probe["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(probe["protocol"], 1);
    let revision = probe["revision"].as_str().unwrap();
    assert!(!revision.is_empty());
    let capabilities: Vec<&str> = probe["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    for expected in [
        "raw-public-streams",
        "absolute-resume-offsets",
        "sticky-stdin-eof",
        "separate-stdout-stderr",
        "process-group-cancel",
        "cancel-outcome",
        "terminal-replay",
        "nobuffer",
        "forced-attach-takeover",
    ] {
        assert!(capabilities.contains(&expected), "missing {expected}");
    }

    for malformed in [vec!["capabilities"], vec!["capabilities", "--json", "x"]] {
        let output = Command::new(binary()).args(&malformed).output().unwrap();
        assert_eq!(output.status.code(), Some(1), "args: {malformed:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--json"), "stderr: {stderr}");
    }
}

#[test]
fn replays_buffered_output_at_explicit_offsets_after_exit() {
    let runtime = runtime_dir("replay");
    let mut first = attachment(runtime.path(), "replay", "printf abcdef; printf uvwxyz >&2");
    first
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"stdin_eof\":0}\n")
        .unwrap();
    let output = first.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdin"], 0);
    assert_eq!(body, b"abcdef");
    assert_eq!(output.stderr, b"uvwxyz");

    // Terminal replay from explicit mid-stream offsets after the command and
    // its first attachment have both finished.
    let mut second = attachment(runtime.path(), "replay", "unused-on-attach");
    second
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"offsets\":{\"stdout\":3,\"stderr\":4}}\n")
        .unwrap();
    let output = second.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdout"], 3);
    assert_eq!(header["offsets"]["stderr"], 4);
    assert_eq!(header["stdin_eof"], true);
    assert_eq!(header["stdout_eof"], 6);
    assert_eq!(header["stderr_eof"], 6);
    assert_eq!(header["exit"]["code"], 0);
    assert_eq!(body, b"def");
    assert_eq!(output.stderr, b"yz");

    // Full replay from zero is still available.
    let mut third = attachment(runtime.path(), "replay", "unused-on-attach");
    third
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"offsets\":{\"stdout\":0,\"stderr\":0}}\n")
        .unwrap();
    let output = third.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (_, body) = split_header(&output.stdout);
    assert_eq!(body, b"abcdef");
    assert_eq!(output.stderr, b"uvwxyz");
}

#[test]
fn resumes_stdin_at_broker_position_with_sticky_eof() {
    let runtime = runtime_dir("stdin-resume");
    let mut first = attachment(runtime.path(), "stdin-resume", "cat");
    let mut input = first.stdin.take().unwrap();
    let mut output_pipe = first.stdout.take().unwrap();
    input.write_all(b"{}\nabc").unwrap();
    input.flush().unwrap();
    let header = read_json_line(&mut output_pipe);
    assert_eq!(header["offsets"]["stdin"], 0);
    // The echo confirms the broker accepted the bytes before the attachment
    // is lost, so the resume position below is deterministic.
    let mut echoed = [0_u8; 3];
    output_pipe.read_exact(&mut echoed).unwrap();
    assert_eq!(&echoed, b"abc");
    drop(input);
    assert!(first.wait().unwrap().success());

    let mut second = attachment(runtime.path(), "stdin-resume", "unused-on-attach");
    let mut input = second.stdin.take().unwrap();
    input
        .write_all(b"{\"offsets\":{\"stdout\":3,\"stderr\":0},\"stdin_eof\":6}\ndef")
        .unwrap();
    input.flush().unwrap();
    let output = second.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdin"], 3);
    assert_eq!(header["offsets"]["stdout"], 3);
    assert_eq!(body, b"def");
    drop(input);

    // Redeclaring the same absolute EOF is idempotent even after exit.
    let mut redeclare = attachment(runtime.path(), "stdin-resume", "unused-on-attach");
    redeclare
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"action\":\"stdin-eof\",\"stdin_eof\":6}\n")
        .unwrap();
    let output = redeclare.wait_with_output().unwrap();
    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["stdin_eof_at"], 6);
    assert_eq!(response["stdin_eof"], true);

    // A conflicting EOF position is rejected, not silently adopted.
    let mut conflict = attachment(runtime.path(), "stdin-resume", "unused-on-attach");
    conflict
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"action\":\"stdin-eof\",\"stdin_eof\":7}\n")
        .unwrap();
    let output = conflict.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("already declared"));
}

#[test]
fn rejects_second_concurrent_data_attachment() {
    let runtime = runtime_dir("exclusive");
    let mut first = attachment(runtime.path(), "exclusive", "cat");
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    let header = read_json_line(&mut first_output);
    assert!(header.get("error").is_none(), "header: {header}");

    let mut second = attachment(runtime.path(), "exclusive", "unused-on-attach");
    let mut second_input = second.stdin.take().unwrap();
    second_input
        .write_all(b"{\"offsets\":{\"stdout\":0,\"stderr\":0}}\n")
        .unwrap();
    second_input.flush().unwrap();
    let output = second.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        response["error"]
            .as_str()
            .unwrap()
            .contains("already has an attached client"),
        "response: {response}"
    );

    // A control declaration is still allowed alongside the data attachment
    // and cleanly ends the session command.
    let mut control = attachment(runtime.path(), "exclusive", "unused-on-attach");
    control
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"action\":\"stdin-eof\",\"stdin_eof\":0}\n")
        .unwrap();
    assert!(control.wait_with_output().unwrap().status.success());
    assert_eq!(first.wait().unwrap().code(), Some(0));
}

#[test]
fn forced_attachment_replaces_current_without_relaunching_command() {
    let runtime = runtime_dir("force-replace");
    let launch_log = runtime.path().join("launches");
    let shell = format!(
        "printf launch >> '{}'; printf out1; printf err1 >&2; cat; printf out2; printf err2 >&2",
        launch_log.display()
    );
    let mut first = attachment(runtime.path(), "force-replace", &shell);
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    let mut first_error = first.stderr.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    let header = read_json_line(&mut first_output);
    assert_eq!(header["offsets"]["stdout"], 0);
    let mut stdout_prefix = [0_u8; 4];
    first_output.read_exact(&mut stdout_prefix).unwrap();
    assert_eq!(&stdout_prefix, b"out1");
    let mut stderr_prefix = [0_u8; 4];
    first_error.read_exact(&mut stderr_prefix).unwrap();
    assert_eq!(&stderr_prefix, b"err1");
    let pid_before = pid_text(runtime.path(), "force-replace");

    let mut second = forced_attachment(runtime.path(), "force-replace", "unused-on-attach");
    let mut second_input = second.stdin.take().unwrap();
    second_input
        .write_all(&request(
            serde_json::json!({"offsets":{"stdout":4,"stderr":4},"stdin_eof":3}),
        ))
        .unwrap();
    second_input.write_all(b"abc").unwrap();
    drop(second_input);
    let second_output = second.wait_with_output().unwrap();

    wait_displaced(&mut first, "displaced first");
    drop(first_input);
    assert_eq!(pid_text(runtime.path(), "force-replace"), pid_before);
    assert_eq!(fs::read_to_string(&launch_log).unwrap(), "launch");
    assert_eq!(second_output.status.code(), Some(0));
    let (header, body) = split_header(&second_output.stdout);
    assert_eq!(header["offsets"]["stdout"], 4);
    assert_eq!(header["offsets"]["stderr"], 4);
    assert_eq!(body, b"abcout2");
    assert_eq!(second_output.stderr, b"err2");
}

#[test]
fn forced_ahead_offset_divests_then_leaves_unattached_for_recovery() {
    let runtime = runtime_dir("force-ahead");
    let mut first = attachment(
        runtime.path(),
        "force-ahead",
        "printf abc; printf err >&2; sleep 0.2; printf done; printf fin >&2",
    );
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    let mut first_error = first.stderr.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);
    let mut stdout_prefix = [0_u8; 3];
    first_output.read_exact(&mut stdout_prefix).unwrap();
    assert_eq!(&stdout_prefix, b"abc");
    let mut stderr_prefix = [0_u8; 3];
    first_error.read_exact(&mut stderr_prefix).unwrap();
    assert_eq!(&stderr_prefix, b"err");

    let mut failed = forced_attachment(runtime.path(), "force-ahead", "unused-on-attach");
    write_request(
        &mut failed,
        serde_json::json!({"offsets":{"stdout":99,"stderr":0}}),
    );
    let failed_output = failed.wait_with_output().unwrap();
    assert_eq!(failed_output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&failed_output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("requested output offset is ahead"));
    wait_displaced(&mut first, "divested first");
    drop(first_input);

    let mut recovered = forced_attachment(runtime.path(), "force-ahead", "unused-on-attach");
    write_request(
        &mut recovered,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0},"stdin_eof":0}),
    );
    let output = recovered.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdout"], 0);
    assert_eq!(body, b"abcdone");
    assert_eq!(output.stderr, b"errfin");
}

#[test]
fn forced_invalid_stdin_eof_divests_without_mutating_stdin_state() {
    let runtime = runtime_dir("force-stdin-eof");
    let mut first = attachment(runtime.path(), "force-stdin-eof", "cat");
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\nabc").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);
    let mut echoed = [0_u8; 3];
    first_output.read_exact(&mut echoed).unwrap();
    assert_eq!(&echoed, b"abc");

    let mut failed = forced_attachment(runtime.path(), "force-stdin-eof", "unused-on-attach");
    write_request(
        &mut failed,
        serde_json::json!({"offsets":{"stdout":3,"stderr":0},"stdin_eof":2}),
    );
    let failed_output = failed.wait_with_output().unwrap();
    assert_eq!(failed_output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&failed_output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("behind accepted byte 3"));
    wait_displaced(&mut first, "invalid eof displaced first");
    drop(first_input);

    let mut recovered = forced_attachment(runtime.path(), "force-stdin-eof", "unused-on-attach");
    let mut input = recovered.stdin.take().unwrap();
    input
        .write_all(&request(
            serde_json::json!({"offsets":{"stdout":3,"stderr":0},"stdin_eof":6}),
        ))
        .unwrap();
    input.write_all(b"def").unwrap();
    drop(input);
    let output = recovered.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdin"], 3);
    assert_eq!(body, b"def");
}

#[test]
fn forced_frontend_inconsistent_stdin_offsets_take_over_before_broker_rejects() {
    let runtime = runtime_dir("force-frontend-stdin-range");
    let mut first = attachment(runtime.path(), "force-frontend-stdin-range", "cat");
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\nabcdef").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);
    let mut echoed = [0_u8; 6];
    first_output.read_exact(&mut echoed).unwrap();
    assert_eq!(&echoed, b"abcdef");

    let mut failed = forced_attachment(
        runtime.path(),
        "force-frontend-stdin-range",
        "unused-on-attach",
    );
    write_request(
        &mut failed,
        serde_json::json!({
            "offsets":{"stdout":6,"stderr":0},
            "stdin_start":6,
            "stdin_eof":5
        }),
    );
    let failed_output = failed.wait_with_output().unwrap();
    assert_eq!(failed_output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&failed_output.stdout).unwrap();
    assert!(
        response["error"]
            .as_str()
            .unwrap()
            .contains("behind accepted byte 6"),
        "response: {response}"
    );
    wait_displaced(&mut first, "frontend range displaced first");
    drop(first_input);

    let mut recovered = forced_attachment(
        runtime.path(),
        "force-frontend-stdin-range",
        "unused-on-attach",
    );
    let mut recovered_input = recovered.stdin.take().unwrap();
    recovered_input
        .write_all(&request(serde_json::json!({
            "offsets":{"stdout":6,"stderr":0},
            "stdin_start":6,
            "stdin_eof":9
        })))
        .unwrap();
    recovered_input.write_all(b"ghi").unwrap();
    drop(recovered_input);
    let output = recovered.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdin"], 6);
    assert_eq!(body, b"ghi");
}

#[test]
fn forced_nobuffer_opening_failure_divests_and_does_not_restore() {
    let runtime = runtime_dir("force-nobuffer-gap");
    let release = runtime.path().join("release");
    let mut first = Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .args([
            "--id",
            "force-nobuffer-gap",
            "--nobuffer",
            "--",
            "/bin/sh",
            "-c",
            &format!(
                "while [ ! -e '{}' ]; do sleep 0.02; done; printf abc; sleep 0.3",
                release.display()
            ),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);
    fs::write(&release, b"").unwrap();
    let mut prefix = [0_u8; 3];
    first_output.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"abc");

    let mut failed = forced_attachment(runtime.path(), "force-nobuffer-gap", "unused-on-attach");
    write_request(
        &mut failed,
        serde_json::json!({"offsets":{"stdout":99,"stderr":0}}),
    );
    let failed_output = failed.wait_with_output().unwrap();
    assert_eq!(failed_output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&failed_output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("requested output offset is ahead"));
    wait_displaced(&mut first, "nobuffer displaced first");
    drop(first_input);

    let mut recovered = forced_attachment(runtime.path(), "force-nobuffer-gap", "unused-on-attach");
    write_request(
        &mut recovered,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0},"stdin_eof":0}),
    );
    let output = recovered.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["nobuffer"], true);
    assert_eq!(header["offsets"]["stdout"], 3);
    assert_eq!(body, b"");
}

#[test]
fn displaced_cleanup_cannot_clear_replacement() {
    let runtime = runtime_dir("force-cleanup");
    let mut first = attachment(runtime.path(), "force-cleanup", "cat; printf done");
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);

    let mut second = forced_attachment(runtime.path(), "force-cleanup", "unused-on-attach");
    let mut second_input = second.stdin.take().unwrap();
    second_input
        .write_all(&request(
            serde_json::json!({"offsets":{"stdout":0,"stderr":0},"stdin_eof":1}),
        ))
        .unwrap();
    second_input.flush().unwrap();
    let mut second_output = second.stdout.take().unwrap();
    let header = read_json_line(&mut second_output);
    assert_eq!(header["offsets"]["stdout"], 0);
    wait_displaced(&mut first, "cleanup first");
    drop(first_input);

    second_input.write_all(b"z").unwrap();
    drop(second_input);
    let mut body = Vec::new();
    second_output.read_to_end(&mut body).unwrap();
    let status = second.wait().unwrap();
    assert_eq!(status.code(), Some(0));
    assert_eq!(body, b"zdone");
}

#[test]
fn concurrent_forced_attempts_leave_one_winner_and_no_stale_clear() {
    let runtime = runtime_dir("force-concurrent");
    let release = runtime.path().join("release");
    let mut first = attachment(
        runtime.path(),
        "force-concurrent",
        &format!(
            "while [ ! -e '{}' ]; do sleep 0.02; done; printf done",
            release.display()
        ),
    );
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);

    let mut second = forced_attachment(runtime.path(), "force-concurrent", "unused-on-attach");
    write_request(
        &mut second,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0},"stdin_eof":0}),
    );
    let mut second_output = second.stdout.take().unwrap();
    let second_header = read_json_line(&mut second_output);
    assert_eq!(second_header["offsets"]["stdout"], 0);
    wait_displaced(&mut first, "concurrent displaced first");
    drop(first_input);

    let mut third = forced_attachment(runtime.path(), "force-concurrent", "unused-on-attach");
    write_request(
        &mut third,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0},"stdin_eof":0}),
    );
    drop(third.stdin.take());
    let mut third_output = third.stdout.take().unwrap();
    let third_header = read_json_line(&mut third_output);
    assert_eq!(third_header["offsets"]["stdout"], 0);

    wait_displaced(&mut second, "concurrent displaced second");
    fs::write(&release, b"").unwrap();
    let mut body = Vec::new();
    third_output.read_to_end(&mut body).unwrap();
    let third_status = third.wait().unwrap();
    assert_eq!(third_status.code(), Some(0));
    assert_eq!(body, b"done");
}

#[test]
fn delayed_stdin_from_displaced_attachment_is_ignored() {
    let runtime = runtime_dir("force-stale-stdin");
    let mut first = attachment(runtime.path(), "force-stale-stdin", "cat");
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);

    let mut second = forced_attachment(runtime.path(), "force-stale-stdin", "unused-on-attach");
    let mut second_input = second.stdin.take().unwrap();
    second_input
        .write_all(&request(
            serde_json::json!({"offsets":{"stdout":0,"stderr":0},"stdin_eof":1}),
        ))
        .unwrap();
    second_input.flush().unwrap();
    let mut second_output = second.stdout.take().unwrap();
    read_json_line(&mut second_output);
    let _ = first_input.write_all(b"A");
    second_input.write_all(b"B").unwrap();
    drop(second_input);

    let mut body = Vec::new();
    second_output.read_to_end(&mut body).unwrap();
    let status = second.wait().unwrap();
    assert_eq!(status.code(), Some(0));
    assert_eq!(body, b"B");
    wait_displaced(&mut first, "stale stdin first");
}

#[test]
fn forced_replacement_during_output_preserves_contiguous_replay() {
    let runtime = runtime_dir("force-output");
    let mut first = attachment(
        runtime.path(),
        "force-output",
        "for c in a b c d e f g h i j; do printf \"$c\"; sleep 0.02; done",
    );
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);
    let mut prefix = [0_u8; 3];
    first_output.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"abc");

    let mut second = forced_attachment(runtime.path(), "force-output", "unused-on-attach");
    write_request(
        &mut second,
        serde_json::json!({"offsets":{"stdout":3,"stderr":0},"stdin_eof":0}),
    );
    let output = second.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    wait_displaced(&mut first, "output displaced first");
    drop(first_input);
    let (_, body) = split_header(&output.stdout);
    let mut combined = prefix.to_vec();
    combined.extend_from_slice(body);
    assert_eq!(combined, b"abcdefghij");
}

#[test]
fn forced_replacement_after_workload_exit_replays_terminal_state() {
    let runtime = runtime_dir("force-after-exit");
    let mut first = attachment(
        runtime.path(),
        "force-after-exit",
        "printf out; printf err >&2; exit 7",
    );
    first
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"stdin_eof\":0}\n")
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));

    let mut second = forced_attachment(runtime.path(), "force-after-exit", "unused-on-attach");
    write_request(
        &mut second,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0}}),
    );
    let output = second.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(7));
    let (header, body) = split_header(&output.stdout);
    assert_eq!(header["exit"]["code"], 7);
    assert_eq!(body, b"out");
    assert_eq!(output.stderr, b"err");
    let _ = first.wait();
}

#[test]
fn malformed_forced_broker_request_does_not_take_over() {
    let runtime = runtime_dir("force-malformed");
    let mut first = attachment(runtime.path(), "force-malformed", "cat");
    let mut first_input = first.stdin.take().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    read_json_line(&mut first_output);

    let socket = session_socket(runtime.path());
    let mut raw = UnixStream::connect(socket).unwrap();
    raw.write_all(b"{\"action\":\"attach\",\"force\":true,\"offsets\"\n")
        .unwrap();
    drop(raw);

    let mut second = attachment(runtime.path(), "force-malformed", "unused-on-attach");
    write_request(
        &mut second,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0}}),
    );
    let output = second.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("already has an attached client"));

    let mut control = attachment(runtime.path(), "force-malformed", "unused-on-attach");
    control
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"action\":\"stdin-eof\",\"stdin_eof\":0}\n")
        .unwrap();
    assert!(control.wait_with_output().unwrap().status.success());
    assert_eq!(first.wait().unwrap().code(), Some(0));
    drop(first_input);
}

#[test]
fn forced_missing_session_remains_missing() {
    let runtime = runtime_dir("force-missing");
    let mut child = forced_attachment(runtime.path(), "force-missing", "unused-on-attach");
    write_request(
        &mut child,
        serde_json::json!({"offsets":{"stdout":0,"stderr":0}}),
    );
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("session does not exist"));
    assert_eq!(fs::read_dir(runtime.path()).unwrap().count(), 0);
}

#[test]
fn force_create_is_rejected_before_launch_or_mutation() {
    let runtime = runtime_dir("force-create");
    let launched = runtime.path().join("launched");
    let shell = format!("touch '{}'", launched.display());
    let mut child = forced_attachment(runtime.path(), "force-create", &shell);
    write_request(&mut child, serde_json::json!({}));
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("cannot create"));
    assert!(!launched.exists());
    assert_eq!(fs::read_dir(runtime.path()).unwrap().count(), 0);
}

#[test]
fn removes_completed_session_after_short_ttl() {
    let runtime = runtime_dir("ttl");
    let mut child = Command::new(binary())
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "1")
        .args(["--id", "ttl", "--", "/bin/sh", "-c", "printf done"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"stdin_eof\":0}\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let (_, body) = split_header(&output.stdout);
    assert_eq!(body, b"done");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let sessions = fs::read_dir(runtime.path()).unwrap().count();
        if sessions == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "session directory survived the TTL"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut late = attachment(runtime.path(), "ttl", "unused-on-attach");
    late.stdin
        .take()
        .unwrap()
        .write_all(b"{\"offsets\":{\"stdout\":0,\"stderr\":0}}\n")
        .unwrap();
    let output = late.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("session does not exist"));
}

// In --nobuffer mode a live attachment relays output through a small fixed
// broker-side queue. When the client falls behind and the queue overruns, the
// broker emits a forward-offset chunk and the short-lived proxy must close
// the attachment WITHOUT writing a pipekeep diagnostic to its public stderr:
// after the handshake that stream carries native command stderr, and any
// injected diagnostic would be counted by the outer wrapper as delivered
// command bytes and poison the next absolute stderr resume offset. The flood
// is written while this test reads nothing, so far more data passes through
// the broker than the live queue plus every transport buffer can absorb and
// the overrun is deterministic, not timing-dependent.
#[test]
fn nobuffer_live_queue_overrun_closes_attachment_without_public_diagnostics() {
    let runtime = runtime_dir("live-gap");
    let binary = binary();
    const FLOOD: usize = 32 * 1024 * 1024;
    let attached = runtime.path().join("attached");
    let sync = runtime.path().join("sync");
    let done = runtime.path().join("done");
    let finish = runtime.path().join("finish");
    // In nobuffer mode output produced before the attachment subscribes is
    // discarded, so the command holds every write until the test confirms the
    // attachment (and later the stderr delivery) through flag files. It also
    // stays alive until the gap has been observed: at command exit the broker
    // resolves outstanding gaps at the terminal boundary instead, which is
    // not the path under test.
    let script = format!(
        "while [ ! -e '{}' ]; do sleep 0.05; done; printf real-stderr >&2; \
         while [ ! -e '{}' ]; do sleep 0.05; done; \
         head -c {FLOOD} /dev/zero; touch '{}'; \
         while [ ! -e '{}' ]; do sleep 0.05; done",
        attached.display(),
        sync.display(),
        done.display(),
        finish.display()
    );
    let mut first = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .args([
            "--id",
            "live-gap",
            "--nobuffer",
            "--",
            "/bin/sh",
            "-c",
            &script,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Keep stdin open for the whole attachment so the proxy can only end
    // through its output path, never through a benign local stdin EOF.
    let mut first_input = first.stdin.take().unwrap();
    first_input.write_all(b"{}\n").unwrap();
    first_input.flush().unwrap();
    let mut first_output = first.stdout.take().unwrap();
    let mut first_error = first.stderr.take().unwrap();
    let header = read_json_line(&mut first_output);
    assert_eq!(header["offsets"]["stdout"], 0);
    assert_eq!(header["offsets"]["stderr"], 0);
    fs::write(&attached, b"").unwrap();

    // Genuine command stderr passes through unchanged while the stream flows.
    let mut real = [0_u8; 11];
    first_error.read_exact(&mut real).unwrap();
    assert_eq!(&real, b"real-stderr");

    // Release the flood and wait until the command has written all of it.
    fs::write(&sync, b"").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done.exists() {
        assert!(
            Instant::now() < deadline,
            "command did not finish the flood"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut body = Vec::new();
    first_output.read_to_end(&mut body).unwrap();
    let mut late_error = Vec::new();
    first_error.read_to_end(&mut late_error).unwrap();
    let status = first.wait().unwrap();
    drop(first_input);

    // The attachment failed on the queue gap rather than skipping bytes...
    assert_eq!(status.code(), Some(1));
    assert!(body.len() < FLOOD, "no overrun: {} delivered", body.len());
    // ...and the public streams carried only native command bytes: the
    // delivered stdout prefix is intact and stderr received no diagnostic.
    assert!(body.iter().all(|byte| *byte == 0));
    assert_eq!(
        late_error,
        b"",
        "public stderr was contaminated: {}",
        String::from_utf8_lossy(&late_error)
    );

    // A reconnect using only actually delivered byte counts advances to the
    // live positions at its opening handshake and replays the retained exit.
    // Terminal state is asynchronous to the finish flag, so attach again
    // until the broker reports it.
    fs::write(&finish, b"").unwrap();
    let request = format!(
        "{{\"offsets\":{{\"stdout\":{},\"stderr\":11}}}}\n",
        body.len()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let output = loop {
        let mut second = attachment(runtime.path(), "live-gap", "unused-on-attach");
        second
            .stdin
            .take()
            .unwrap()
            .write_all(request.as_bytes())
            .unwrap();
        let output = second.wait_with_output().unwrap();
        let (header, _) = split_header(&output.stdout);
        if header.get("exit").is_some() {
            break output;
        }
        assert!(Instant::now() < deadline, "command never became terminal");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (header, rest) = split_header(&output.stdout);
    assert_eq!(header["offsets"]["stdout"], FLOOD);
    assert_eq!(header["offsets"]["stderr"], 11);
    assert_eq!(header["stdout_eof"], FLOOD);
    assert_eq!(header["stderr_eof"], 11);
    assert_eq!(header["exit"]["code"], 0);
    assert_eq!(rest, b"");
    assert_eq!(output.stderr, b"");
}

// The outer wrapper reads the transport command's merged stderr. Diagnostics
// written by the transport itself are indistinguishable from remote command
// stderr and advance the outer stderr position, so exact stderr resume
// accounting is invalid after such a failure. The transport script forwards
// exactly the remote command's four stderr bytes, injects its own diagnostic,
// and dies; the resume attempt then requests a position beyond the stream.
#[test]
fn transport_stderr_diagnostics_break_exact_stderr_resume() {
    let runtime = runtime_dir("transport-stderr");
    let binary = binary();
    let script = runtime.path().join("transport.sh");
    let flag = runtime.path().join("first-attempt");
    let fifo = runtime.path().join("stderr-tap");
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
if mkdir '{flag}' 2>/dev/null; then
  mkfifo '{fifo}'
  # A background command's stdin defaults to /dev/null; pass the real
  # transport stdin through explicitly so the handshake reaches the inner.
  exec 3<&0
  '{binary}' --id transport-stderr -- /bin/sh -c 'printf AAAA >&2; sleep 30' 0<&3 2>'{fifo}' &
  inner=$!
  {{ dd bs=1 count=4 2>/dev/null; printf 'TRANSPORT-DIAGNOSTIC'; }} <'{fifo}' >&2
  kill "$inner" 2>/dev/null
  wait "$inner" 2>/dev/null
  exit 255
else
  exec '{binary}' --id transport-stderr -- /bin/sh -c 'printf AAAA >&2; sleep 30'
fi
"#,
            flag = flag.display(),
            fifo = fifo.display(),
            binary = binary.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();

    let output = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_SESSION_TTL_SECS", "2")
        .arg("--")
        .arg(&script)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    // Remote stderr and the transport's own diagnostic arrive merged.
    assert!(
        stderr.contains("AAAATRANSPORT-DIAGNOSTIC"),
        "stderr: {stderr}"
    );
    // The 20 diagnostic bytes were counted as delivered remote stderr, so the
    // resume asks for byte 24 of a 4-byte stream and is refused.
    assert!(
        stderr.contains("requested output offset is ahead of the stream"),
        "stderr: {stderr}"
    );

    let canceled = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_CANCEL_GRACE_SECS", "0")
        .args(["cancel", "--id", "transport-stderr"])
        .status()
        .unwrap();
    assert!(canceled.code().is_some());
}

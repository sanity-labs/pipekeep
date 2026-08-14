use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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

fn split_header(stdout: &[u8]) -> (serde_json::Value, &[u8]) {
    let end = stdout.iter().position(|byte| *byte == b'\n').unwrap();
    (
        serde_json::from_slice(&stdout[..end]).unwrap(),
        &stdout[end + 1..],
    )
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

    let canceled = Command::new(&binary)
        .env("PIPEKEEP_RUNTIME_DIR", runtime.path())
        .env("PIPEKEEP_CANCEL_GRACE_SECS", "0")
        .args(["cancel", "--id", "cancel-me"])
        .status()
        .unwrap();
    // The leader accepted SIGTERM before escalation, so its authoritative
    // result is 128 + SIGTERM rather than being relabeled as canceled.
    assert_eq!(canceled.code(), Some(143));

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
        "terminal-replay",
        "nobuffer",
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

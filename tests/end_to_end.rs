use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn binary() -> PathBuf {
    assert_cmd::cargo::cargo_bin("rpipe")
}

fn runtime_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("rpipe-{label}-"))
        .tempdir_in("/tmp")
        .unwrap()
}

fn outer_command(runtime: &Path, id: &str, shell: &str) -> Command {
    let binary = binary();
    let mut command = Command::new(&binary);
    command
        .env("RPIPE_RUNTIME_DIR", runtime)
        .env("RPIPE_SESSION_TTL_SECS", "2")
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
if mkdir '{}'; then
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
        .env("RPIPE_RUNTIME_DIR", runtime.path())
        .env("RPIPE_SESSION_TTL_SECS", "2")
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
        .env("RPIPE_CANCEL_GRACE_SECS", "0")
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
        .env("RPIPE_RUNTIME_DIR", runtime.path())
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
        .env("RPIPE_RUNTIME_DIR", runtime.path())
        .env("RPIPE_CANCEL_GRACE_SECS", "0")
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
        assert!(Instant::now() < deadline, "outer rpipe did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn nobuffer_mode_preserves_live_streams() {
    let runtime = runtime_dir("nobuffer");
    let binary = binary();
    let mut child = Command::new(&binary)
        .env("RPIPE_RUNTIME_DIR", runtime.path())
        .env("RPIPE_SESSION_TTL_SECS", "2")
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

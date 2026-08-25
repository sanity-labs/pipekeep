use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::cli::ControllerArgs;
use crate::util::{assert_json_u64, read_json_line, read_offset, write_offset};

pub(crate) fn run_controller(args: ControllerArgs) -> Result<()> {
    fs::create_dir_all(&args.state)?;
    let stdout_offset = read_offset(&args.state.join("stdout.offset"))?;
    let stderr_offset = read_offset(&args.state.join("stderr.offset"))?;

    let mut child = Command::new(&args.pipekeep)
        .env("PIPEKEEP_RUNTIME_DIR", &args.runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", args.ttl_secs.to_string())
        .args(["--id", &args.id, "--"])
        .arg("/bin/sh")
        .arg(&args.workload)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("cannot start {}", args.pipekeep.display()))?;

    let request = if args.create {
        serde_json::json!({"stdin_eof": 0})
    } else {
        serde_json::json!({
            "offsets": {"stdout": stdout_offset, "stderr": stderr_offset},
            "stdin_eof": 0,
        })
    };
    {
        let mut stdin = child.stdin.take().context("pipekeep child has no stdin")?;
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        stdin.write_all(&bytes)?;
    }

    let mut stdout = child
        .stdout
        .take()
        .context("pipekeep child has no stdout")?;
    let header = read_json_line(&mut stdout)?;
    if let Some(error) = header.get("error").and_then(Value::as_str) {
        let status = child.wait()?;
        bail!("attachment failed with {status}: {error}");
    }
    let offsets = header
        .get("offsets")
        .context("attachment header has no offsets")?;
    assert_json_u64(offsets, "stdout", stdout_offset)?;
    assert_json_u64(offsets, "stderr", stderr_offset)?;

    let running = Arc::new(AtomicBool::new(true));
    let stdout_counter = Arc::new(AtomicU64::new(stdout_offset));
    let stderr_counter = Arc::new(AtomicU64::new(stderr_offset));
    let stdout_thread = spawn_stream_recorder(
        stdout,
        args.state.join("stdout.log"),
        args.state.join("stdout.offset"),
        stdout_counter.clone(),
        running.clone(),
    );
    let stderr = child
        .stderr
        .take()
        .context("pipekeep child has no stderr")?;
    let stderr_thread = spawn_stream_recorder(
        stderr,
        args.state.join("stderr.log"),
        args.state.join("stderr.offset"),
        stderr_counter.clone(),
        running.clone(),
    );

    let start = Instant::now();
    let killed = loop {
        if let Some(status) = child.try_wait()? {
            break finish_controller_child(
                status,
                false,
                args.expect_exit,
                running,
                stdout_thread,
                stderr_thread,
            );
        }
        if args
            .kill_after_ms
            .is_some_and(|ms| start.elapsed() >= Duration::from_millis(ms))
        {
            child.kill().context("failed to kill attachment proxy")?;
            let status = child.wait()?;
            break finish_controller_child(
                status,
                true,
                args.expect_exit,
                running,
                stdout_thread,
                stderr_thread,
            );
        }
        thread::sleep(Duration::from_millis(10));
    }?;
    println!(
        "{}",
        serde_json::json!({
            "killed_proxy": killed,
            "stdout_offset": read_offset(&args.state.join("stdout.offset"))?,
            "stderr_offset": read_offset(&args.state.join("stderr.offset"))?,
        })
    );
    Ok(())
}

fn finish_controller_child(
    status: ExitStatus,
    killed: bool,
    expect_exit: Option<i32>,
    running: Arc<AtomicBool>,
    stdout_thread: thread::JoinHandle<Result<()>>,
    stderr_thread: thread::JoinHandle<Result<()>>,
) -> Result<bool> {
    running.store(false, Ordering::Release);
    join_thread(stdout_thread)?;
    join_thread(stderr_thread)?;
    if let Some(expected) = expect_exit {
        if status.code() != Some(expected) {
            bail!("expected terminal exit {expected}, got {status}");
        }
    } else if !killed {
        bail!("attachment ended before the requested disconnect point: {status}");
    }
    Ok(killed)
}

fn spawn_stream_recorder<R: Read + Send + 'static>(
    mut reader: R,
    log_path: PathBuf,
    offset_path: PathBuf,
    counter: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("cannot open {}", log_path.display()))?;
        let mut buffer = [0_u8; 8192];
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(count) => count,
                Err(error) => {
                    if !running.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    return Err(error).context("stream read failed");
                }
            };
            log.write_all(&buffer[..count])?;
            log.flush()?;
            let offset = counter.fetch_add(count as u64, Ordering::AcqRel) + count as u64;
            write_offset(&offset_path, offset)?;
        }
    })
}

fn join_thread(handle: thread::JoinHandle<Result<()>>) -> Result<()> {
    handle
        .join()
        .map_err(|_| anyhow::anyhow!("stream recorder panicked"))?
}

pub(crate) struct ControllerRun<'a> {
    pub(crate) pipekeep: &'a Path,
    pub(crate) runtime: &'a Path,
    pub(crate) state: &'a Path,
    pub(crate) workload: &'a Path,
    pub(crate) id: &'a str,
    pub(crate) create: bool,
    pub(crate) kill_after_ms: Option<u64>,
    pub(crate) expect_exit: Option<i32>,
    pub(crate) ttl_secs: u64,
}

pub(crate) fn run_controller_process(run: ControllerRun<'_>) -> Result<()> {
    let mut command = Command::new(env::current_exe()?);
    command
        .arg("__controller")
        .arg("--pipekeep")
        .arg(run.pipekeep)
        .arg("--runtime")
        .arg(run.runtime)
        .arg("--id")
        .arg(run.id)
        .arg("--state")
        .arg(run.state)
        .arg("--workload")
        .arg(run.workload)
        .arg("--ttl-secs")
        .arg(run.ttl_secs.to_string());
    if run.create {
        command.arg("--create");
    } else {
        command.arg("--resume");
    }
    if let Some(ms) = run.kill_after_ms {
        command.arg("--kill-after-ms").arg(ms.to_string());
    }
    if let Some(code) = run.expect_exit {
        command.arg("--expect-exit").arg(code.to_string());
    }
    let output = command.output().context("cannot run controller process")?;
    if !output.status.success() {
        bail!(
            "controller exited with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    print!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

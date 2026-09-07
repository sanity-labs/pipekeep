use anyhow::{bail, Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::cli::{Mode, StdinTransportArgs, StdinWorkloadArgs};
use crate::util::{assert_file_eq, join_thread, Cleanup, Lcg};

pub(crate) const STDIN_WORKLOAD_EXIT: i32 = 43;
const STDIN_RECORD_SIZE: usize = 64;
const STDIN_RECORDS: usize = 8192;
const UNSET_OFFSET: u64 = u64::MAX;

pub(crate) fn verify_buffered_stdin_replay(
    pipekeep: &Path,
    runtime: &Path,
    root: &Path,
    mode: Mode,
    seed: u64,
    cleanup: &mut Cleanup,
) -> Result<()> {
    let replay_root = root.join("stdin-replay");
    fs::create_dir_all(&replay_root)?;
    let id = format!("acceptance-stdin-{}-{seed}", std::process::id());
    cleanup.track(id.clone());

    let input_len = STDIN_RECORDS * STDIN_RECORD_SIZE;
    let input = deterministic_stdin_input(input_len);
    let digest = sha256_hex(&input);
    let mut outer = spawn_outer_stdin_replay(pipekeep, runtime, &id, &replay_root, input_len)?;
    let outer_pid = outer.id();

    let stdout_path = replay_root.join("outer.stdout");
    let stderr_path = replay_root.join("outer.stderr");
    let stdout_thread = spawn_file_recorder(
        outer
            .stdout
            .take()
            .context("outer stdin replay has no stdout")?,
        stdout_path.clone(),
    );
    let stderr_thread = spawn_file_recorder(
        outer
            .stderr
            .take()
            .context("outer stdin replay has no stderr")?,
        stderr_path.clone(),
    );

    let producer_done = replay_root.join("producer.done");
    let producer_input = input.clone();
    let mut producer_stdin = outer
        .stdin
        .take()
        .context("outer stdin replay has no stdin")?;
    let producer_done_thread = producer_done.clone();
    let producer_thread = thread::spawn(move || -> Result<()> {
        for chunk in producer_input.chunks(2048) {
            producer_stdin.write_all(chunk)?;
            producer_stdin.flush()?;
            thread::sleep(Duration::from_millis(1));
        }
        drop(producer_stdin);
        fs::write(producer_done_thread, b"done\n")?;
        Ok(())
    });

    let progress_path = replay_root.join("workload.progress");
    let mut killed = Vec::new();
    let first_pid = wait_for_data_transport_pid(&replay_root, None, Duration::from_secs(10))?;
    let first_prefix =
        wait_for_stdin_progress_between(&progress_path, 1, input_len, Duration::from_secs(20))?;
    kill_transport_after_prefix(&mut outer, outer_pid, first_pid, first_prefix, input_len)?;
    killed.push(first_pid);

    let target_deaths = match mode {
        Mode::Smoke | Mode::Chaos => 3,
    };
    let mut rng = Lcg::new(seed ^ 0xa5a5_5a5a_d15c_a11e);
    let mut last_pid = first_pid;
    let mut post_eof_loss = false;
    while killed.len() < target_deaths {
        if read_stdin_progress(&progress_path)? >= input_len as u64 {
            break;
        }
        let pid =
            wait_for_data_transport_pid(&replay_root, Some(last_pid), Duration::from_secs(20))?;
        last_pid = pid;
        let delay = 15 + (rng.next_u64() % 55);
        thread::sleep(Duration::from_millis(delay));
        let accepted = read_stdin_progress(&progress_path)?;
        if accepted >= input_len as u64 {
            break;
        }
        let accepted = if accepted > 0 {
            accepted
        } else {
            wait_for_stdin_progress_between(&progress_path, 1, input_len, Duration::from_secs(20))?
        };
        let producer_was_done = producer_done.exists();
        kill_transport_after_prefix(&mut outer, outer_pid, pid, accepted, input_len)?;
        post_eof_loss |= producer_was_done;
        killed.push(pid);
    }

    join_thread(producer_thread).context("finite stdin producer failed")?;
    if !post_eof_loss && read_stdin_progress(&progress_path)? < input_len as u64 {
        let pid =
            wait_for_data_transport_pid(&replay_root, Some(last_pid), Duration::from_secs(20))?;
        last_pid = pid;
        thread::sleep(Duration::from_millis(25));
        let accepted = read_stdin_progress(&progress_path)?;
        if accepted >= input_len as u64 {
            // The finite EOF already reached the workload before another
            // post-EOF transport loss could be synchronized.
        } else {
            let accepted = if accepted > 0 {
                accepted
            } else {
                wait_for_stdin_progress_between(
                    &progress_path,
                    1,
                    input_len,
                    Duration::from_secs(20),
                )?
            };
            if accepted < input_len as u64 {
                kill_transport_after_prefix(&mut outer, outer_pid, pid, accepted, input_len)?;
                post_eof_loss = true;
                killed.push(pid);
            }
        }
    }
    let _ = last_pid;
    if !post_eof_loss {
        bail!("stdin replay did not lose a transport after finite producer EOF");
    }

    let status = wait_child_timeout(&mut outer, Duration::from_secs(40))
        .context("outer stdin replay did not finish")?;
    if status.code() != Some(STDIN_WORKLOAD_EXIT) {
        bail!(
            "stdin replay outer exit was {}, expected {}",
            status,
            STDIN_WORKLOAD_EXIT
        );
    }
    join_thread(stdout_thread)?;
    join_thread(stderr_thread)?;

    assert_file_eq(
        &replay_root.join("accepted.bin"),
        &input,
        "buffered stdin replay accepted input",
    )?;
    let expected_stdout =
        format!("stdin-replay stdout len={input_len} records={STDIN_RECORDS} sha256={digest}\n");
    let expected_stderr =
        format!("stdin-replay stderr len={input_len} records={STDIN_RECORDS} sha256={digest}\n");
    assert_file_eq(
        &stdout_path,
        expected_stdout.as_bytes(),
        "stdin replay stdout",
    )?;
    assert_file_eq(
        &stderr_path,
        expected_stderr.as_bytes(),
        "stdin replay stderr",
    )?;
    validate_stdin_replay_events(&replay_root, input_len, &killed)?;

    println!(
        "ok: buffered stdin replay verified len={input_len} sha256={digest} transport_deaths={} seed={} outer_pid={outer_pid}",
        killed.len(),
        seed
    );
    Ok(())
}

fn spawn_outer_stdin_replay(
    pipekeep: &Path,
    runtime: &Path,
    id: &str,
    replay_root: &Path,
    input_len: usize,
) -> Result<Child> {
    let mut command = Command::new(pipekeep);
    command
        .env("PIPEKEEP_RUNTIME_DIR", runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .arg("--")
        .arg(env::current_exe()?)
        .arg("__stdin_transport")
        .arg("--pipekeep")
        .arg(pipekeep)
        .arg("--runtime")
        .arg(runtime)
        .arg("--id")
        .arg(id)
        .arg("--root")
        .arg(replay_root)
        .arg("--input-len")
        .arg(input_len.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("cannot spawn outer stdin replay for {id}"))
}

pub(crate) fn run_stdin_transport(args: StdinTransportArgs) -> Result<i32> {
    fs::create_dir_all(&args.root)?;
    let pid = std::process::id();
    fs::write(args.root.join("transport.current.pid"), format!("{pid}\n"))?;
    append_stdin_event(
        &args.root,
        &serde_json::json!({
            "event": "transport_start",
            "pid": pid,
            "ppid": unsafe { libc::getppid() },
        }),
    )?;

    let mut child = Command::new(&args.pipekeep)
        .env("PIPEKEEP_RUNTIME_DIR", &args.runtime)
        .env("PIPEKEEP_SESSION_TTL_SECS", "30")
        .args(["--id", &args.id, "--"])
        .arg(env::current_exe()?)
        .arg("__stdin_workload")
        .arg("--root")
        .arg(&args.root)
        .arg("--input-len")
        .arg(args.input_len.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("cannot spawn inner pipekeep transport for {}", args.id))?;

    let child_stdin = child.stdin.take().context("inner pipekeep has no stdin")?;
    let child_stdout = child
        .stdout
        .take()
        .context("inner pipekeep has no stdout")?;
    let child_stderr = child
        .stderr
        .take()
        .context("inner pipekeep has no stderr")?;
    let server_stdin = Arc::new(AtomicU64::new(UNSET_OFFSET));
    let data_attachment = Arc::new(AtomicBool::new(false));

    let stdin_root = args.root.clone();
    let stdin_server_stdin = server_stdin.clone();
    let stdin_data_attachment = data_attachment.clone();
    let input_len = args.input_len;
    let stdin_thread = thread::spawn(move || {
        proxy_outer_to_inner_stdin(
            std::io::stdin(),
            child_stdin,
            stdin_root,
            pid,
            stdin_server_stdin,
            stdin_data_attachment,
            input_len,
        )
    });

    let stdout_root = args.root.clone();
    let stdout_server_stdin = server_stdin.clone();
    let stdout_data_attachment = data_attachment.clone();
    let stdout_thread = thread::spawn(move || {
        proxy_inner_stdout(
            child_stdout,
            std::io::stdout(),
            stdout_root,
            pid,
            stdout_server_stdin,
            stdout_data_attachment,
        )
    });

    let stderr_root = args.root.clone();
    let stderr_thread = thread::spawn(move || {
        copy_transport_stream(child_stderr, std::io::stderr(), stderr_root, pid, "stderr")
    });

    let status = child.wait()?;
    let _ = join_thread(stdout_thread);
    let _ = join_thread(stderr_thread);
    let _ = stdin_thread;
    append_stdin_event(
        &args.root,
        &serde_json::json!({
            "event": "transport_exit",
            "pid": pid,
            "code": status.code(),
            "signal": status.signal(),
        }),
    )?;
    Ok(process_code(status))
}

fn proxy_outer_to_inner_stdin<R: Read, W: Write>(
    mut input: R,
    mut output: W,
    root: PathBuf,
    pid: u32,
    server_stdin: Arc<AtomicU64>,
    data_attachment: Arc<AtomicBool>,
    input_len: usize,
) -> Result<()> {
    let Some(line) = read_line_including_newline(&mut input)? else {
        return Ok(());
    };
    output.write_all(&line)?;
    output.flush()?;
    let hello: Value = serde_json::from_slice(line.trim_ascii_end())?;
    let action = hello
        .get("action")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if action.is_none() {
        data_attachment.store(true, Ordering::Release);
    }
    append_stdin_event(
        &root,
        &serde_json::json!({
            "event": "client_hello",
            "pid": pid,
            "action": action,
            "stdin_start": hello.get("stdin_start").and_then(Value::as_u64).unwrap_or(0),
            "stdin_eof": hello.get("stdin_eof").and_then(Value::as_u64),
        }),
    )?;

    let mut position = UNSET_OFFSET;
    let mut logged_first_chunk = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            return Ok(());
        }
        if position == UNSET_OFFSET {
            position = wait_for_server_stdin_offset(&server_stdin, Duration::from_secs(5))?;
        }
        let valid = raw_chunk_matches_input(&buffer[..count], position, input_len);
        if !logged_first_chunk {
            append_stdin_event(
                &root,
                &serde_json::json!({
                    "event": "raw_stdin_chunk",
                    "pid": pid,
                    "offset": position,
                    "bytes": count,
                    "valid": valid,
                }),
            )?;
            logged_first_chunk = true;
        }
        if !valid {
            append_stdin_event(
                &root,
                &serde_json::json!({
                    "event": "raw_stdin_mismatch",
                    "pid": pid,
                    "offset": position,
                    "bytes": count,
                }),
            )?;
        }
        output.write_all(&buffer[..count])?;
        output.flush()?;
        position += count as u64;
    }
}

fn proxy_inner_stdout<R: Read, W: Write>(
    mut input: R,
    mut output: W,
    root: PathBuf,
    pid: u32,
    server_stdin: Arc<AtomicU64>,
    data_attachment: Arc<AtomicBool>,
) -> Result<()> {
    let Some(line) = read_line_including_newline(&mut input)? else {
        return Ok(());
    };
    output.write_all(&line)?;
    output.flush()?;
    let hello: Value = serde_json::from_slice(line.trim_ascii_end())?;
    if let Some(error) = hello.get("error").and_then(Value::as_str) {
        append_stdin_event(
            &root,
            &serde_json::json!({
                "event": "server_error",
                "pid": pid,
                "error": error,
            }),
        )?;
    } else {
        let stdin = hello
            .get("offsets")
            .and_then(|offsets| offsets.get("stdin"))
            .and_then(Value::as_u64)
            .context("server hello has no stdin offset")?;
        if data_attachment.load(Ordering::Acquire) {
            fs::write(root.join("transport.data.pid"), format!("{pid}\n"))?;
        }
        server_stdin.store(stdin, Ordering::Release);
        append_stdin_event(
            &root,
            &serde_json::json!({
                "event": "server_hello",
                "pid": pid,
                "stdin": stdin,
                "stdout": hello.get("offsets").and_then(|offsets| offsets.get("stdout")).and_then(Value::as_u64),
                "stderr": hello.get("offsets").and_then(|offsets| offsets.get("stderr")).and_then(Value::as_u64),
                "stdin_eof": hello.get("stdin_eof").and_then(Value::as_bool).unwrap_or(false),
                "exit": hello.get("exit").is_some(),
            }),
        )?;
    }
    copy_raw(input, output)
}

fn copy_transport_stream<R: Read, W: Write>(
    input: R,
    output: W,
    root: PathBuf,
    pid: u32,
    stream: &str,
) -> Result<()> {
    let result = copy_raw(input, output);
    if let Err(error) = &result {
        append_stdin_event(
            &root,
            &serde_json::json!({
                "event": "transport_copy_error",
                "pid": pid,
                "stream": stream,
                "error": error.to_string(),
            }),
        )?;
    }
    result
}

fn copy_raw<R: Read, W: Write>(mut input: R, mut output: W) -> Result<()> {
    let mut buffer = [0_u8; 8192];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            return Ok(());
        }
        output.write_all(&buffer[..count])?;
        output.flush()?;
    }
}

pub(crate) fn run_stdin_workload(args: StdinWorkloadArgs) -> Result<()> {
    fs::create_dir_all(&args.root)?;
    let expected = deterministic_stdin_input(args.input_len);
    let mut accepted = Vec::with_capacity(args.input_len);
    let accepted_path = args.root.join("accepted.bin");
    let mut accepted_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&accepted_path)?;
    write_stdin_progress(&args.root.join("workload.progress"), 0)?;

    let mut stdin = std::io::stdin();
    let mut buffer = [0_u8; 1024];
    loop {
        let count = stdin.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let start = accepted.len();
        let end = start + count;
        if end > expected.len() {
            fs::write(
                args.root.join("workload.failure"),
                format!("received bytes beyond expected EOF at {start}\n"),
            )?;
            bail!("stdin workload received bytes beyond expected EOF at {start}");
        }
        if buffer[..count] != expected[start..end] {
            fs::write(
                args.root.join("workload.failure"),
                format!("stdin mismatch at accepted offset {start}\n"),
            )?;
            bail!("stdin workload mismatch at accepted offset {start}");
        }
        accepted_file.write_all(&buffer[..count])?;
        accepted_file.flush()?;
        accepted.extend_from_slice(&buffer[..count]);
        write_stdin_progress(&args.root.join("workload.progress"), accepted.len() as u64)?;
        thread::sleep(Duration::from_millis(2));
    }

    if accepted.len() != expected.len() {
        fs::write(
            args.root.join("workload.failure"),
            format!(
                "stdin ended at {} bytes, expected {}\n",
                accepted.len(),
                expected.len()
            ),
        )?;
        bail!(
            "stdin workload ended at {} bytes, expected {}",
            accepted.len(),
            expected.len()
        );
    }
    accepted_file.sync_all()?;
    let digest = sha256_hex(&accepted);
    fs::write(
        args.root.join("workload.complete"),
        format!(
            "len={} records={} sha256={digest}\n",
            accepted.len(),
            accepted.len() / STDIN_RECORD_SIZE
        ),
    )?;
    println!(
        "stdin-replay stdout len={} records={} sha256={digest}",
        accepted.len(),
        accepted.len() / STDIN_RECORD_SIZE
    );
    eprintln!(
        "stdin-replay stderr len={} records={} sha256={digest}",
        accepted.len(),
        accepted.len() / STDIN_RECORD_SIZE
    );
    Ok(())
}

fn spawn_file_recorder<R: Read + Send + 'static>(
    mut reader: R,
    path: PathBuf,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let mut file = File::create(&path)
            .with_context(|| format!("cannot create recorder file {}", path.display()))?;
        let mut buffer = [0_u8; 8192];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                return Ok(());
            }
            file.write_all(&buffer[..count])?;
            file.flush()?;
        }
    })
}

fn deterministic_stdin_input(len: usize) -> Vec<u8> {
    (0..len).map(deterministic_stdin_byte).collect()
}

fn deterministic_stdin_byte(index: usize) -> u8 {
    let record = index / STDIN_RECORD_SIZE;
    let within = index % STDIN_RECORD_SIZE;
    match within {
        0 => b'P',
        1 => b'K',
        2 => b'I',
        3 => b'N',
        4..=11 => (record as u64).to_le_bytes()[within - 4],
        _ => {
            let mixed = (record as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .rotate_left((within % 31) as u32)
                ^ (within as u64).wrapping_mul(0xd1b5_4a32_d192_ed03);
            (mixed >> ((within % 8) * 8)) as u8
        }
    }
}

fn raw_chunk_matches_input(chunk: &[u8], offset: u64, input_len: usize) -> bool {
    let Ok(start) = usize::try_from(offset) else {
        return false;
    };
    let Some(end) = start.checked_add(chunk.len()) else {
        return false;
    };
    if end > input_len {
        return false;
    }
    chunk
        .iter()
        .enumerate()
        .all(|(index, byte)| *byte == deterministic_stdin_byte(start + index))
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut text = String::with_capacity(digest.len() * 2);
    for byte in digest {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

fn read_line_including_newline(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        match reader.read(&mut byte)? {
            0 if line.is_empty() => return Ok(None),
            0 => bail!("stream ended in the opening JSON line"),
            _ => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    return Ok(Some(line));
                }
                if line.len() > 64 * 1024 {
                    bail!("opening JSON line is too large");
                }
            }
        }
    }
}

fn wait_for_server_stdin_offset(offset: &AtomicU64, timeout: Duration) -> Result<u64> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let value = offset.load(Ordering::Acquire);
        if value != UNSET_OFFSET {
            return Ok(value);
        }
        thread::sleep(Duration::from_millis(5));
    }
    bail!("timed out waiting for server stdin offset")
}

fn append_stdin_event(root: &Path, event: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("transport.events.jsonl"))?;
    let mut bytes = serde_json::to_vec(event)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.flush()?;
    Ok(())
}

fn wait_for_data_transport_pid(
    root: &Path,
    previous: Option<u32>,
    timeout: Duration,
) -> Result<u32> {
    let path = root.join("transport.data.pid");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(&path) {
            if let Ok(pid) = text.trim().parse::<u32>() {
                if Some(pid) != previous && process_exists(pid) {
                    return Ok(pid);
                }
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    bail!("timed out waiting for a restarted stdin transport")
}

fn kill_transport_after_prefix(
    outer: &mut Child,
    outer_pid: u32,
    transport_pid: u32,
    accepted: u64,
    input_len: usize,
) -> Result<()> {
    if accepted == 0 || accepted >= input_len as u64 {
        bail!(
            "refusing to kill transport {transport_pid}: accepted prefix {accepted} is not proper for {input_len} bytes"
        );
    }
    signal_process(transport_pid, libc::SIGKILL)
        .with_context(|| format!("failed to kill transport {transport_pid}"))?;
    thread::sleep(Duration::from_millis(30));
    if outer.id() != outer_pid {
        bail!(
            "outer process id changed from {outer_pid} to {}",
            outer.id()
        );
    }
    if let Some(status) = outer.try_wait()? {
        bail!("outer process exited after transport kill with {status}");
    }
    Ok(())
}

fn wait_for_stdin_progress_between(
    path: &Path,
    min_inclusive: u64,
    max_exclusive: usize,
    timeout: Duration,
) -> Result<u64> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let value = read_stdin_progress(path)?;
        if value >= min_inclusive && value < max_exclusive as u64 {
            return Ok(value);
        }
        thread::sleep(Duration::from_millis(20));
    }
    bail!(
        "timed out waiting for stdin progress in [{min_inclusive}, {max_exclusive}) at {}",
        path.display()
    )
}

fn read_stdin_progress(path: &Path) -> Result<u64> {
    match fs::read_to_string(path) {
        Ok(text) => text
            .trim()
            .parse()
            .with_context(|| format!("invalid stdin progress in {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn write_stdin_progress(path: &Path, offset: u64) -> Result<()> {
    let temp = path.with_extension("progress.tmp");
    fs::write(&temp, format!("{offset}\n"))?;
    fs::rename(temp, path)?;
    Ok(())
}

fn wait_child_timeout(child: &mut Child, timeout: Duration) -> Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    bail!("timed out waiting for child {}", child.id())
}

fn signal_process(pid: u32, signal: i32) -> Result<()> {
    let result = unsafe { libc::kill(pid as i32, signal) };
    if result == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn process_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as i32, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn process_code(status: ExitStatus) -> i32 {
    status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1))
}

fn validate_stdin_replay_events(root: &Path, input_len: usize, killed_pids: &[u32]) -> Result<()> {
    if killed_pids.len() < 2 {
        bail!("stdin replay killed only {} transports", killed_pids.len());
    }
    let events_path = root.join("transport.events.jsonl");
    let text = fs::read_to_string(&events_path)
        .with_context(|| format!("cannot read {}", events_path.display()))?;
    let mut client_by_pid: HashMap<u64, Value> = HashMap::new();
    let mut server_by_pid: HashMap<u64, Value> = HashMap::new();
    let mut raw_by_pid: HashMap<u64, Value> = HashMap::new();
    let mut data_pids = Vec::new();
    for line in text.lines() {
        let event: Value = serde_json::from_str(line)?;
        let pid = event.get("pid").and_then(Value::as_u64).unwrap_or(0);
        match event.get("event").and_then(Value::as_str) {
            Some("client_hello") => {
                if event.get("action").is_none_or(Value::is_null) && !data_pids.contains(&pid) {
                    data_pids.push(pid);
                }
                client_by_pid.insert(pid, event);
            }
            Some("server_hello") => {
                server_by_pid.insert(pid, event);
            }
            Some("raw_stdin_chunk") => {
                if event.get("valid").and_then(Value::as_bool) != Some(true) {
                    bail!("transport saw raw stdin bytes that did not match the framed input");
                }
                raw_by_pid.insert(pid, event);
            }
            Some("raw_stdin_mismatch") => {
                bail!("transport logged a raw stdin mismatch: {event}");
            }
            _ => {}
        }
    }
    if data_pids.len() < killed_pids.len() + 1 {
        bail!(
            "stdin replay did not restart data transports repeatedly: data_pids={} killed={}",
            data_pids.len(),
            killed_pids.len()
        );
    }

    let mut positive_resume = false;
    let mut suffix_replay = false;
    let mut sticky_eof_resume = false;
    for (pid, server) in &server_by_pid {
        let Some(client) = client_by_pid.get(pid) else {
            continue;
        };
        if !client.get("action").is_none_or(Value::is_null) {
            continue;
        }
        let stdin = server.get("stdin").and_then(Value::as_u64).unwrap_or(0);
        if stdin > 0 && stdin < input_len as u64 {
            positive_resume = true;
            if let Some(raw) = raw_by_pid.get(pid) {
                if raw.get("offset").and_then(Value::as_u64) == Some(stdin) {
                    suffix_replay = true;
                }
            }
        }
        if client.get("stdin_eof").and_then(Value::as_u64) == Some(input_len as u64)
            && stdin < input_len as u64
        {
            sticky_eof_resume = true;
        }
    }
    if !positive_resume {
        bail!("no resumed inner broker hello reported a nonzero proper stdin prefix");
    }
    if !suffix_replay {
        bail!("no replayed stdin chunk started at the broker-reported suffix offset");
    }
    if !sticky_eof_resume {
        bail!("no reconnect carried finite-producer EOF to a broker missing stdin suffix");
    }
    Ok(())
}

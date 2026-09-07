use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) const WORKLOAD_EXIT: i32 = 42;

pub(crate) fn write_workload(
    scripts: &Path,
    root: &Path,
    counts: (usize, usize),
) -> Result<PathBuf> {
    let path = scripts.join("workload.sh");
    let script = format!(
        r#"if ! mkdir '{root}/launch.once' 2>/dev/null; then
  echo duplicate >> '{root}/duplicate-launch'
  exit 97
fi
echo "$$" > '{root}/workload.pid'
i=0
while [ "$i" -lt {pre} ]; do
  printf 'stdout:%03d:acceptance\n' "$i"
  printf 'stderr:%03d:acceptance\n' "$i" >&2
  i=$((i + 1))
  sleep 0.015
done
touch '{root}/prelude.done'
while [ ! -e '{root}/finish.gate' ]; do sleep 0.02; done
while [ "$i" -lt {total} ]; do
  printf 'stdout:%03d:acceptance\n' "$i"
  printf 'stderr:%03d:acceptance\n' "$i" >&2
  i=$((i + 1))
  sleep 0.015
done
touch '{root}/workload.complete'
exit {exit_code}
"#,
        root = root.display(),
        pre = counts.0,
        total = counts.0 + counts.1,
        exit_code = WORKLOAD_EXIT
    );
    fs::write(&path, script)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    Ok(path)
}

pub(crate) fn expected_stream(name: &str, counts: (usize, usize)) -> String {
    let mut output = String::new();
    for index in 0..(counts.0 + counts.1) {
        output.push_str(&format!("{name}:{index:03}:acceptance\n"));
    }
    output
}

pub(crate) fn init_state(state: &Path) -> Result<()> {
    write_offset(&state.join("stdout.offset"), 0)?;
    write_offset(&state.join("stderr.offset"), 0)?;
    File::create(state.join("stdout.log"))?;
    File::create(state.join("stderr.log"))?;
    Ok(())
}

pub(crate) fn read_offset(path: &Path) -> Result<u64> {
    match fs::read_to_string(path) {
        Ok(text) => text
            .trim()
            .parse()
            .with_context(|| format!("invalid offset in {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn write_offset(path: &Path, offset: u64) -> Result<()> {
    let temp = path.with_extension("offset.tmp");
    fs::write(&temp, format!("{offset}\n"))?;
    fs::rename(temp, path)?;
    Ok(())
}

pub(crate) fn assert_file_eq(path: &Path, expected: &[u8], label: &str) -> Result<()> {
    let actual = fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    if actual != expected {
        bail!(
            "{label} mismatch: expected {} bytes, got {} bytes",
            expected.len(),
            actual.len()
        );
    }
    Ok(())
}

pub(crate) fn assert_launch_once(root: &Path) -> Result<()> {
    if !root.join("launch.once").is_dir() {
        bail!("workload launch marker was not created");
    }
    if root.join("duplicate-launch").exists() {
        bail!("workload launched more than once");
    }
    Ok(())
}

pub(crate) fn wait_for_file(path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    bail!("timed out waiting for {}", path.display())
}

pub(crate) fn wait_for_group_gone(pgid: i32, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let result = unsafe { libc::kill(-pgid, 0) };
        if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    bail!("process group {pgid} still exists")
}

pub(crate) fn read_json_line(reader: &mut impl Read) -> Result<Value> {
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

pub(crate) fn assert_json_u64(object: &Value, key: &str, expected: u64) -> Result<()> {
    let actual = object
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing integer field {key:?} in {object}"))?;
    if actual != expected {
        bail!("expected {key} offset {expected}, got {actual}");
    }
    Ok(())
}

pub(crate) fn join_thread(handle: thread::JoinHandle<Result<()>>) -> Result<()> {
    handle
        .join()
        .map_err(|_| anyhow::anyhow!("stream recorder panicked"))?
}

pub(crate) fn locate_pipekeep(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return absolute(path);
    }
    if let Some(path) = env::var_os("PIPEKEEP_BIN") {
        return absolute(Path::new(&path));
    }
    let exe = env::current_exe()?;
    if let Some(parent) = exe.parent() {
        let sibling = parent.join("pipekeep");
        if sibling.exists() {
            return absolute(&sibling);
        }
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsStr::new("cargo").to_os_string());
    let status = Command::new(cargo)
        .current_dir(&manifest_dir)
        .args(["build", "--quiet", "--bin", "pipekeep"])
        .status()
        .context("failed to invoke cargo build --bin pipekeep")?;
    if !status.success() {
        bail!("cargo build --bin pipekeep failed with {status}");
    }
    let sibling = exe
        .parent()
        .context("current executable has no parent")?
        .join("pipekeep");
    absolute(&sibling)
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

pub(crate) fn default_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1)
}

pub(crate) struct Lcg(u64);

impl Lcg {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

pub(crate) struct Cleanup {
    pipekeep: PathBuf,
    runtime: PathBuf,
    sessions: Vec<String>,
    active: bool,
}

impl Cleanup {
    pub(crate) fn new(pipekeep: PathBuf, runtime: PathBuf) -> Self {
        Self {
            pipekeep,
            runtime,
            sessions: Vec::new(),
            active: true,
        }
    }

    pub(crate) fn track(&mut self, id: String) {
        self.sessions.push(id);
    }

    pub(crate) fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        for id in &self.sessions {
            let _ = Command::new(&self.pipekeep)
                .env("PIPEKEEP_RUNTIME_DIR", &self.runtime)
                .env("PIPEKEEP_CANCEL_GRACE_SECS", "0")
                .args(["cancel", "--id", id])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

pub(crate) struct HarnessTemp {
    dir: tempfile::TempDir,
    retain: bool,
    success: bool,
}

impl HarnessTemp {
    pub(crate) fn new(retain: bool) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("pipekeep-acceptance-")
            .tempdir_in("/tmp")?;
        Ok(Self {
            dir,
            retain,
            success: false,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    pub(crate) fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for HarnessTemp {
    fn drop(&mut self) {
        if self.retain || !self.success {
            let path = self.dir.path().to_path_buf();
            let dir = std::mem::replace(
                &mut self.dir,
                tempfile::Builder::new()
                    .prefix("pipekeep-acceptance-empty-")
                    .tempdir_in("/tmp")
                    .expect("create replacement tempdir"),
            );
            let _ = dir.keep();
            eprintln!("retained acceptance temp dir: {}", path.display());
        }
    }
}

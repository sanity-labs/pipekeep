use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub fn root_dir() -> Result<PathBuf> {
    let root = if let Some(value) = std::env::var_os("PIPEKEEP_RUNTIME_DIR") {
        PathBuf::from(value)
    } else if let Some(value) = std::env::var_os("XDG_RUNTIME_DIR") {
        PathBuf::from(value).join("pipekeep")
    } else {
        std::env::temp_dir().join(format!("pipekeep-{}", unsafe { libc::geteuid() }))
    };
    fs::create_dir_all(&root)
        .with_context(|| format!("cannot create runtime directory {}", root.display()))?;
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
    Ok(root)
}

pub fn session_dir(id: &str) -> Result<PathBuf> {
    if id.is_empty() {
        bail!("session ID may not be empty");
    }
    if id.len() > 1024 {
        bail!("session ID is too long");
    }
    let digest = Sha256::digest(id.as_bytes());
    let name = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(root_dir()?.join(name))
}

pub fn socket_path(session_dir: &Path) -> PathBuf {
    session_dir.join("broker.sock")
}

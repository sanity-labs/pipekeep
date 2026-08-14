use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=PIPEKEEP_BUILD_REV");
    let revision = std::env::var("PIPEKEEP_BUILD_REV")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(git_revision)
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=PIPEKEEP_BUILD_REVISION={revision}");
}

fn git_revision() -> Option<String> {
    // HEAD covers detached checkouts; the reflog also moves when a commit
    // advances the branch that a symbolic HEAD points at.
    for name in ["HEAD", "logs/HEAD"] {
        if let Some(path) = git_path(name) {
            if std::path::Path::new(&path).exists() {
                println!("cargo:rerun-if-changed={path}");
            }
        }
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let revision = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!revision.is_empty()).then_some(revision)
}

fn git_path(name: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", name])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!path.is_empty()).then_some(path)
}

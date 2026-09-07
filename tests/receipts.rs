// Bounded Linux fixtures use exact subprocess/pidfd ownership and fail closed.
#[cfg(target_os = "linux")]
#[test]
fn public_framed_receipts_matrix() {
    let output = std::process::Command::new("python3")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/receipts.py"))
        .arg(env!("CARGO_BIN_EXE_pipekeep"))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("Python is required for the public receipts matrix");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "public receipts matrix failed");
}

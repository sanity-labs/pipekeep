// Mandatory local gate: unavailable Python/group pidfds are failures, never skips.
#[cfg(target_os = "linux")]
#[test]
fn finite_output_matrix() {
    let result = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/output_bounds.py"
        ))
        .arg(env!("CARGO_BIN_EXE_pipekeep"))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("finite output matrix requires Python");
    println!("{}", String::from_utf8_lossy(&result.stdout));
    eprintln!("{}", String::from_utf8_lossy(&result.stderr));
    assert!(
        result.status.success(),
        "mandatory finite output matrix failed"
    );
}

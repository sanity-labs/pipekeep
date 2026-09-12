// Mandatory repair gate: real acquisition fault, frontend boundaries and I/O pins.
#[cfg(target_os = "linux")]
#[test]
fn finite_output_boundary_repairs() {
    let result = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/output_bounds_repair.py"
        ))
        .arg(env!("CARGO_BIN_EXE_pipekeep"))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("finite output repair gate requires Python");
    println!("{}", String::from_utf8_lossy(&result.stdout));
    eprintln!("{}", String::from_utf8_lossy(&result.stderr));
    assert!(result.status.success(), "finite output repair gate failed");
}

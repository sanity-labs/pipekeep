// Real broker binaries and Linux syscalls, with an exact-owned subreaper harness.
// A capable gate MUST set PIPEKEEP_REQUIRE_GROUP_PIDFD_TESTS=1: unsupported
// runtimes (or missing Python) then fail rather than silently skipping coverage.
#[cfg(target_os = "linux")]
#[test]
fn real_group_pidfd_broker_matrix() {
    let required = std::env::var_os("PIPEKEEP_REQUIRE_GROUP_PIDFD_TESTS").is_some();
    let output = std::process::Command::new("python3")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/group_pidfd.py"))
        .arg(env!("CARGO_BIN_EXE_pipekeep"))
        .output();
    match output {
        Err(error) if !required && error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("UNSUPPORTED group-pidfd harness: python3 unavailable");
        }
        Err(error) => panic!("cannot run required group-pidfd harness: {error}"),
        Ok(output) => {
            println!("{}", String::from_utf8_lossy(&output.stdout));
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            assert!(
                output.status.success(),
                "group-pidfd matrix failed: {}",
                output.status
            );
            if required {
                assert!(String::from_utf8_lossy(&output.stdout).contains("CAPABLE MATRIX PASSED"));
            }
        }
    }
}

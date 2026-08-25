use anyhow::{Context, Result};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use crate::cases::{verify_cancellation, verify_negative_cases};
use crate::cli::{Mode, Options};
use crate::controller::{run_controller_process, ControllerRun};
use crate::stdin_replay::verify_buffered_stdin_replay;
use crate::util::{
    assert_file_eq, assert_launch_once, default_seed, expected_stream, init_state, locate_pipekeep,
    wait_for_file, write_workload, Cleanup, HarnessTemp, Lcg, WORKLOAD_EXIT,
};

pub(crate) fn run_orchestrator(mode: Mode, options: Options) -> Result<()> {
    let pipekeep = locate_pipekeep(options.pipekeep.as_deref())?;
    let seed = match mode {
        Mode::Smoke => options.seed.unwrap_or(0),
        Mode::Chaos => options.seed.unwrap_or_else(default_seed),
    };
    let cycles = if mode == Mode::Smoke {
        3
    } else {
        options.cycles
    };

    let mut temp = HarnessTemp::new(options.retain_temp)?;
    let root = temp.path().to_path_buf();
    let runtime = root.join("runtime");
    let state = root.join("state");
    let scripts = root.join("scripts");
    fs::create_dir_all(&runtime)?;
    fs::create_dir_all(&state)?;
    fs::create_dir_all(&scripts)?;
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700))?;

    init_state(&state)?;
    let output_counts = output_counts(mode, cycles);
    let workload = write_workload(&scripts, &root, output_counts)?;
    let expected_stdout = expected_stream("stdout", output_counts);
    let expected_stderr = expected_stream("stderr", output_counts);
    let session_id = format!("acceptance-main-{}-{seed}", std::process::id());

    println!(
        "pipekeep acceptance {:?}: seed={seed} cycles={cycles} temp={}",
        mode,
        root.display()
    );

    let mut cleanup = Cleanup::new(pipekeep.clone(), runtime.clone());
    cleanup.track(session_id.clone());

    match mode {
        Mode::Smoke => {
            run_smoke_cycles(&pipekeep, &runtime, &state, &workload, &session_id, &root)?
        }
        Mode::Chaos => run_chaos_cycles(
            &pipekeep,
            &runtime,
            &state,
            &workload,
            &session_id,
            &root,
            seed,
            cycles,
        )?,
    }

    wait_for_file(&root.join("workload.complete"), Duration::from_secs(20))
        .context("workload did not complete while detached")?;

    run_controller_process(ControllerRun {
        pipekeep: &pipekeep,
        runtime: &runtime,
        state: &state,
        workload: &workload,
        id: &session_id,
        create: false,
        kill_after_ms: None,
        expect_exit: Some(WORKLOAD_EXIT),
        ttl_secs: 3,
    })
    .context("final terminal replay failed")?;

    assert_file_eq(
        &state.join("stdout.log"),
        expected_stdout.as_bytes(),
        "stdout",
    )?;
    assert_file_eq(
        &state.join("stderr.log"),
        expected_stderr.as_bytes(),
        "stderr",
    )?;
    assert_launch_once(&root)?;

    verify_buffered_stdin_replay(&pipekeep, &runtime, &root, mode, seed, &mut cleanup)?;
    verify_cancellation(&pipekeep, &runtime, &scripts, &mut cleanup)?;
    verify_negative_cases(&pipekeep, &runtime, &scripts, &mut cleanup)?;

    cleanup.disarm();
    temp.mark_success();
    println!(
        "ok: stdout={} bytes stderr={} bytes exit={} seed={} cycles={}",
        expected_stdout.len(),
        expected_stderr.len(),
        WORKLOAD_EXIT,
        seed,
        cycles
    );
    Ok(())
}

fn run_smoke_cycles(
    pipekeep: &Path,
    runtime: &Path,
    state: &Path,
    workload: &Path,
    session_id: &str,
    root: &Path,
) -> Result<()> {
    for (index, delay) in [180_u64, 220, 160].into_iter().enumerate() {
        run_controller_process(ControllerRun {
            pipekeep,
            runtime,
            state,
            workload,
            id: session_id,
            create: index == 0,
            kill_after_ms: Some(delay),
            expect_exit: None,
            ttl_secs: 30,
        })
        .with_context(|| format!("smoke disconnect cycle {index} failed"))?;
        assert_launch_once(root)?;
    }
    fs::write(root.join("finish.gate"), b"")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_chaos_cycles(
    pipekeep: &Path,
    runtime: &Path,
    state: &Path,
    workload: &Path,
    session_id: &str,
    root: &Path,
    seed: u64,
    cycles: usize,
) -> Result<()> {
    let mut rng = Lcg::new(seed);
    for cycle in 0..cycles {
        if cycle == cycles / 2 {
            fs::write(root.join("finish.gate"), b"")?;
        }
        let delay = 35 + (rng.next_u64() % 145);
        run_controller_process(ControllerRun {
            pipekeep,
            runtime,
            state,
            workload,
            id: session_id,
            create: cycle == 0,
            kill_after_ms: Some(delay),
            expect_exit: None,
            ttl_secs: 30,
        })
        .with_context(|| format!("chaos cycle {cycle} failed after {delay}ms"))?;
        assert_launch_once(root)?;
    }
    fs::write(root.join("finish.gate"), b"")?;
    Ok(())
}

fn output_counts(mode: Mode, cycles: usize) -> (usize, usize) {
    match mode {
        Mode::Smoke => (30, 50),
        Mode::Chaos => {
            let each = cycles.saturating_mul(12).max(60);
            (each, each)
        }
    }
}

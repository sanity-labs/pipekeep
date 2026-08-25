use anyhow::{bail, Context, Result};
use std::env;
use std::path::PathBuf;

use crate::controller::run_controller;
use crate::orchestrator::run_orchestrator;
use crate::stdin_replay::{run_stdin_transport, run_stdin_workload, STDIN_WORKLOAD_EXIT};

pub(crate) fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("smoke") => run_orchestrator(Mode::Smoke, Options::parse(args)?),
        Some("chaos") => {
            let options = Options::parse(args)?;
            run_orchestrator(Mode::Chaos, options)
        }
        Some("__controller") => run_controller(ControllerArgs::parse(args)?),
        Some("__stdin_transport") => {
            let code = run_stdin_transport(StdinTransportArgs::parse(args)?)?;
            std::process::exit(code);
        }
        Some("__stdin_workload") => {
            run_stdin_workload(StdinWorkloadArgs::parse(args)?)?;
            std::process::exit(STDIN_WORKLOAD_EXIT);
        }
        Some("--help") | Some("-h") | None => {
            print_help();
            Ok(())
        }
        Some(other) => bail!("unknown command {other:?}; use smoke, chaos, or --help"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mode {
    Smoke,
    Chaos,
}

#[derive(Debug)]
pub(crate) struct Options {
    pub(crate) pipekeep: Option<PathBuf>,
    pub(crate) retain_temp: bool,
    pub(crate) seed: Option<u64>,
    pub(crate) cycles: usize,
}

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut pipekeep = None;
        let mut retain_temp = false;
        let mut seed = None;
        let mut cycles = 12_usize;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--pipekeep" => {
                    pipekeep = Some(PathBuf::from(
                        args.next().context("--pipekeep requires a path")?,
                    ));
                }
                "--retain-temp" => retain_temp = true,
                "--seed" => {
                    seed = Some(
                        args.next()
                            .context("--seed requires an integer")?
                            .parse()
                            .context("--seed must be an unsigned integer")?,
                    );
                }
                "--cycles" => {
                    cycles = args
                        .next()
                        .context("--cycles requires an integer")?
                        .parse()
                        .context("--cycles must be a positive integer")?;
                    if cycles == 0 {
                        bail!("--cycles must be greater than zero");
                    }
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => bail!("unknown option {other:?}"),
            }
        }
        Ok(Self {
            pipekeep,
            retain_temp,
            seed,
            cycles,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ControllerArgs {
    pub(crate) pipekeep: PathBuf,
    pub(crate) runtime: PathBuf,
    pub(crate) id: String,
    pub(crate) state: PathBuf,
    pub(crate) workload: PathBuf,
    pub(crate) create: bool,
    pub(crate) kill_after_ms: Option<u64>,
    pub(crate) expect_exit: Option<i32>,
    pub(crate) ttl_secs: u64,
}

impl ControllerArgs {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut pipekeep = None;
        let mut runtime = None;
        let mut id = None;
        let mut state = None;
        let mut workload = None;
        let mut create = false;
        let mut kill_after_ms = None;
        let mut expect_exit = None;
        let mut ttl_secs = 30_u64;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--pipekeep" => pipekeep = Some(PathBuf::from(next_arg(&mut args, "--pipekeep")?)),
                "--runtime" => runtime = Some(PathBuf::from(next_arg(&mut args, "--runtime")?)),
                "--id" => id = Some(next_arg(&mut args, "--id")?),
                "--state" => state = Some(PathBuf::from(next_arg(&mut args, "--state")?)),
                "--workload" => {
                    workload = Some(PathBuf::from(next_arg(&mut args, "--workload")?));
                }
                "--create" => create = true,
                "--resume" => create = false,
                "--kill-after-ms" => {
                    kill_after_ms = Some(
                        next_arg(&mut args, "--kill-after-ms")?
                            .parse()
                            .context("--kill-after-ms must be an integer")?,
                    );
                }
                "--expect-exit" => {
                    expect_exit = Some(
                        next_arg(&mut args, "--expect-exit")?
                            .parse()
                            .context("--expect-exit must be an integer")?,
                    );
                }
                "--ttl-secs" => {
                    ttl_secs = next_arg(&mut args, "--ttl-secs")?
                        .parse()
                        .context("--ttl-secs must be an integer")?;
                }
                other => bail!("unknown controller option {other:?}"),
            }
        }
        Ok(Self {
            pipekeep: pipekeep.context("--pipekeep is required")?,
            runtime: runtime.context("--runtime is required")?,
            id: id.context("--id is required")?,
            state: state.context("--state is required")?,
            workload: workload.context("--workload is required")?,
            create,
            kill_after_ms,
            expect_exit,
            ttl_secs,
        })
    }
}

#[derive(Debug)]
pub(crate) struct StdinTransportArgs {
    pub(crate) pipekeep: PathBuf,
    pub(crate) runtime: PathBuf,
    pub(crate) id: String,
    pub(crate) root: PathBuf,
    pub(crate) input_len: usize,
}

impl StdinTransportArgs {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut pipekeep = None;
        let mut runtime = None;
        let mut id = None;
        let mut root = None;
        let mut input_len = None;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--pipekeep" => pipekeep = Some(PathBuf::from(next_arg(&mut args, "--pipekeep")?)),
                "--runtime" => runtime = Some(PathBuf::from(next_arg(&mut args, "--runtime")?)),
                "--id" => id = Some(next_arg(&mut args, "--id")?),
                "--root" => root = Some(PathBuf::from(next_arg(&mut args, "--root")?)),
                "--input-len" => {
                    input_len = Some(
                        next_arg(&mut args, "--input-len")?
                            .parse()
                            .context("--input-len must be an integer")?,
                    );
                }
                other => bail!("unknown stdin transport option {other:?}"),
            }
        }
        Ok(Self {
            pipekeep: pipekeep.context("--pipekeep is required")?,
            runtime: runtime.context("--runtime is required")?,
            id: id.context("--id is required")?,
            root: root.context("--root is required")?,
            input_len: input_len.context("--input-len is required")?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct StdinWorkloadArgs {
    pub(crate) root: PathBuf,
    pub(crate) input_len: usize,
}

impl StdinWorkloadArgs {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut root = None;
        let mut input_len = None;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--root" => root = Some(PathBuf::from(next_arg(&mut args, "--root")?)),
                "--input-len" => {
                    input_len = Some(
                        next_arg(&mut args, "--input-len")?
                            .parse()
                            .context("--input-len must be an integer")?,
                    );
                }
                other => bail!("unknown stdin workload option {other:?}"),
            }
        }
        Ok(Self {
            root: root.context("--root is required")?,
            input_len: input_len.context("--input-len is required")?,
        })
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("{flag} requires a value"))
}

fn print_help() {
    println!(
        r#"pipekeep-acceptance

Usage:
  cargo run --bin pipekeep-acceptance -- smoke [--retain-temp] [--pipekeep PATH]
  cargo run --bin pipekeep-acceptance -- chaos [--cycles N] [--seed N] [--retain-temp] [--pipekeep PATH]

The smoke is deterministic. Chaos reports and accepts a reproducible seed while
randomizing bounded attachment-kill timings. Both modes use only this binary,
the pipekeep CLI, /bin/sh, files, signals, and temporary directories.
"#
    );
}

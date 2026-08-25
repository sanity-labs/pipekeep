#[path = "pipekeep_acceptance/cases.rs"]
mod cases;
#[path = "pipekeep_acceptance/cli.rs"]
mod cli;
#[path = "pipekeep_acceptance/controller.rs"]
mod controller;
#[path = "pipekeep_acceptance/orchestrator.rs"]
mod orchestrator;
#[path = "pipekeep_acceptance/stdin_replay.rs"]
mod stdin_replay;
#[path = "pipekeep_acceptance/util.rs"]
mod util;

use anyhow::Result;

fn main() {
    if let Err(error) = real_main() {
        eprintln!("pipekeep-acceptance: {error:#}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<()> {
    cli::run()
}

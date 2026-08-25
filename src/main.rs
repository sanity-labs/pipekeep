mod broker;
mod cli;
mod outer;
mod protocol;
mod runtime;
mod server;

use anyhow::Result;
use cli::Mode;

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("pipekeep: {error:#}");
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> Result<i32> {
    match cli::parse(std::env::args().skip(1).collect())? {
        Mode::Outer { command, nobuffer } => outer::run(command, nobuffer).await,
        Mode::Server {
            id,
            command,
            nobuffer,
            force,
        } => server::run(id, command, nobuffer, force).await,
        Mode::Cancel { id } => server::cancel(&id).await,
        Mode::Pid { id } => server::pid(&id).await,
        Mode::Broker {
            id,
            session_dir,
            command,
            nobuffer,
        } => broker::run(id, session_dir, command, nobuffer).await,
        Mode::Help => {
            print!("{}", cli::HELP);
            Ok(0)
        }
        Mode::Version => {
            println!(
                "pipekeep {} ({})",
                env!("CARGO_PKG_VERSION"),
                env!("PIPEKEEP_BUILD_REVISION")
            );
            Ok(0)
        }
        Mode::Capabilities => {
            println!("{}", capabilities());
            Ok(0)
        }
    }
}

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "name": "pipekeep",
        "version": env!("CARGO_PKG_VERSION"),
        "revision": env!("PIPEKEEP_BUILD_REVISION"),
        "protocol": protocol::ATTACHMENT_PROTOCOL_VERSION,
        "capabilities": [
            "raw-public-streams",
            "absolute-resume-offsets",
            "sticky-stdin-eof",
            "separate-stdout-stderr",
            "process-group-cancel",
            "cancel-outcome",
            "terminal-replay",
            "nobuffer",
            "forced-attach-takeover",
        ],
    })
}

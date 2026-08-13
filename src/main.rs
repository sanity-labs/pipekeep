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
            eprintln!("rpipe: {error:#}");
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
        } => server::run(id, command, nobuffer).await,
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
            println!("rpipe {}", env!("CARGO_PKG_VERSION"));
            Ok(0)
        }
    }
}

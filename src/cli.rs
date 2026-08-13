use anyhow::{bail, Result};
use std::path::PathBuf;

#[derive(Debug)]
pub enum Mode {
    Outer {
        command: Vec<String>,
        nobuffer: bool,
    },
    Server {
        id: String,
        command: Vec<String>,
        nobuffer: bool,
    },
    Cancel {
        id: String,
    },
    Pid {
        id: String,
    },
    Broker {
        id: String,
        session_dir: PathBuf,
        command: Vec<String>,
        nobuffer: bool,
    },
    Help,
    Version,
}

pub fn parse(mut args: Vec<String>) -> Result<Mode> {
    if args.is_empty() {
        return Ok(Mode::Help);
    }

    match args[0].as_str() {
        "--help" | "-h" => return Ok(Mode::Help),
        "--version" | "-V" => return Ok(Mode::Version),
        "cancel" => return parse_control(&args[1..], true),
        "pid" => return parse_control(&args[1..], false),
        "__broker" => return parse_broker(&args[1..]),
        _ => {}
    }

    let mut id = None;
    let mut nobuffer = false;
    let index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--id" => {
                let value = args.get(index + 1).cloned();
                let Some(value) = value else {
                    bail!("--id requires a value");
                };
                id = Some(value);
                args.drain(index..=index + 1);
            }
            "--nobuffer" => {
                nobuffer = true;
                args.remove(index);
            }
            "--" => {
                args.remove(index);
                break;
            }
            _ if id.is_none() => break,
            other => bail!("unknown server option {other:?}; put the command after --"),
        }
    }

    if args.is_empty() {
        bail!("a command is required");
    }
    if let Some(id) = id {
        Ok(Mode::Server {
            id,
            command: args,
            nobuffer,
        })
    } else {
        Ok(Mode::Outer {
            command: args,
            nobuffer,
        })
    }
}

fn parse_control(args: &[String], cancel: bool) -> Result<Mode> {
    if args.len() != 2 || args[0] != "--id" {
        bail!("expected --id ID");
    }
    if cancel {
        Ok(Mode::Cancel {
            id: args[1].clone(),
        })
    } else {
        Ok(Mode::Pid {
            id: args[1].clone(),
        })
    }
}

fn parse_broker(args: &[String]) -> Result<Mode> {
    let mut id = None;
    let mut session_dir = None;
    let mut nobuffer = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--id" => {
                id = args.get(index + 1).cloned();
                index += 2;
            }
            "--session-dir" => {
                session_dir = args.get(index + 1).map(PathBuf::from);
                index += 2;
            }
            "--nobuffer" => {
                nobuffer = true;
                index += 1;
            }
            "--" => {
                index += 1;
                break;
            }
            other => bail!("unknown broker option {other:?}"),
        }
    }
    let command = args[index..].to_vec();
    if command.is_empty() {
        bail!("broker command is missing");
    }
    Ok(Mode::Broker {
        id: id.ok_or_else(|| anyhow::anyhow!("broker ID is missing"))?,
        session_dir: session_dir
            .ok_or_else(|| anyhow::anyhow!("broker session directory is missing"))?,
        command,
        nobuffer,
    })
}

pub const HELP: &str = r#"rpipe: resumable process pipes

Usage:
  rpipe [--nobuffer] [--] TRANSPORT [ARG...]
  rpipe --id ID [--nobuffer] -- COMMAND [ARG...]
  rpipe cancel --id ID
  rpipe pid --id ID

The outer form reruns TRANSPORT after a disconnection. The --id form creates
or attaches to a detached process session using the opening protocol message.
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_outer_and_server() {
        assert!(matches!(
            parse(vec!["--".into(), "ssh".into(), "host".into()]).unwrap(),
            Mode::Outer { .. }
        ));
        assert!(matches!(
            parse(vec!["--id".into(), "x".into(), "--".into(), "cat".into()]).unwrap(),
            Mode::Server { .. }
        ));
    }
}

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
        force: bool,
        group_pidfd: bool,
        framed: bool,
    },
    Cancel {
        id: String,
        require_group_pidfd: bool,
    },
    Session {
        id: String,
    },
    Pid {
        id: String,
    },
    Broker {
        id: String,
        group_pidfd: bool,
        session_dir: PathBuf,
        command: Vec<String>,
        nobuffer: bool,
    },
    Help,
    Version,
    Capabilities,
}

pub fn parse(mut args: Vec<String>) -> Result<Mode> {
    if args.is_empty() {
        return Ok(Mode::Help);
    }

    match args[0].as_str() {
        "--help" | "-h" => return Ok(Mode::Help),
        "--version" | "-V" => return Ok(Mode::Version),
        "cancel" => return parse_control(&args[1..], true),
        "session" => return parse_session(&args[1..]),
        "pid" => return parse_control(&args[1..], false),
        "capabilities" => return parse_capabilities(&args[1..]),
        "__broker" => return parse_broker(&args[1..]),
        _ => {}
    }

    let mut id = None;
    let mut nobuffer = false;
    let mut force = false;
    let mut group_pidfd = false;
    let mut framed = false;
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
            "--group-pidfd" => {
                group_pidfd = true;
                args.remove(index);
            }
            "--framed" => {
                // Old parsers interpret an unknown leading token as an outer
                // transport command. Require the strict server-option context
                // so every valid framed invocation is rejected by old parsers.
                if id.is_none() {
                    bail!("--framed must follow --id ID");
                }
                framed = true;
                args.remove(index);
            }
            "--force" => {
                force = true;
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

    if framed && (nobuffer || id.is_none()) {
        bail!("--framed requires --id and buffered sessions (no --nobuffer)");
    }
    if args.is_empty() {
        bail!("a command is required");
    }
    if let Some(id) = id {
        Ok(Mode::Server {
            id,
            command: args,
            nobuffer,
            force,
            group_pidfd,
            framed,
        })
    } else {
        if group_pidfd {
            bail!("--group-pidfd requires --id and new-session creation");
        }
        if force {
            bail!("--force requires --id and only applies to attach-only sessions");
        }
        Ok(Mode::Outer {
            command: args,
            nobuffer,
        })
    }
}

fn parse_session(args: &[String]) -> Result<Mode> {
    if args.len() != 2 || args[0] != "--id" {
        bail!("expected --id ID");
    }
    Ok(Mode::Session {
        id: args[1].clone(),
    })
}

fn parse_control(args: &[String], cancel: bool) -> Result<Mode> {
    let mut args = args.to_vec();
    let require_group_pidfd = cancel && args.iter().any(|s| s == "--require-group-pidfd");
    if require_group_pidfd {
        args.retain(|s| s != "--require-group-pidfd");
    }
    if args.len() != 2 || args[0] != "--id" {
        bail!("expected --id ID [--require-group-pidfd for cancel]");
    }
    if cancel {
        Ok(Mode::Cancel {
            id: args[1].clone(),
            require_group_pidfd,
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
    let mut group_pidfd = false;
    let mut nobuffer = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--id" => {
                id = args.get(index + 1).cloned();
                index += 2;
            }
            "--group-pidfd" => {
                group_pidfd = true;
                index += 1;
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
        group_pidfd,
        id: id.ok_or_else(|| anyhow::anyhow!("broker ID is missing"))?,
        session_dir: session_dir
            .ok_or_else(|| anyhow::anyhow!("broker session directory is missing"))?,
        command,
        nobuffer,
    })
}

fn parse_capabilities(args: &[String]) -> Result<Mode> {
    match args {
        [] => bail!("capabilities requires --json; only machine-readable output is provided"),
        [flag] if flag == "--json" => Ok(Mode::Capabilities),
        _ => bail!("capabilities accepts exactly one argument: --json"),
    }
}

pub const HELP: &str = r#"pipekeep: resumable process pipes

Usage:
  pipekeep [--nobuffer] [--] TRANSPORT [ARG...]
  pipekeep --id ID [--nobuffer] [--force] [--group-pidfd] [--framed] -- COMMAND [ARG...]
  pipekeep cancel --id ID [--require-group-pidfd]
  pipekeep session --id ID
  pipekeep pid --id ID
  pipekeep capabilities --json
  pipekeep --version

The outer form reruns TRANSPORT after a disconnection. The --id form creates
or attaches to a detached process session using the opening protocol message.
With --id, --force applies only to attach-only opening requests and supersedes
an existing data attachment without restarting the command.
--framed requires buffering and carries typed frames after a versioned opening.
`capabilities --json` prints a machine-readable compatibility probe.
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_requires_strict_server_option_context_and_buffering() {
        let args = |values: &[&str]| values.iter().map(|value| (*value).to_owned()).collect();
        assert!(matches!(
            parse(args(&["--id", "x", "--framed", "--", "cat"])).unwrap(),
            Mode::Server { framed: true, .. }
        ));
        assert!(parse(args(&["--framed", "--id", "x", "--", "cat"])).is_err());
        assert!(parse(args(&["--id", "x", "--framed", "--nobuffer", "--", "cat"])).is_err());
        assert!(parse(args(&["--id", "x", "--nobuffer", "--framed", "--", "cat"])).is_err());
    }

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
        assert!(matches!(
            parse(vec![
                "--id".into(),
                "x".into(),
                "--force".into(),
                "--".into(),
                "cat".into()
            ])
            .unwrap(),
            Mode::Server { force: true, .. }
        ));
        assert!(parse(vec!["--force".into(), "--".into(), "ssh".into()]).is_err());
    }

    #[test]
    fn parses_capabilities_probe_strictly() {
        assert!(matches!(
            parse(vec!["capabilities".into(), "--json".into()]).unwrap(),
            Mode::Capabilities
        ));
        assert!(parse(vec!["capabilities".into()]).is_err());
        assert!(parse(vec!["capabilities".into(), "--text".into()]).is_err());
        assert!(parse(vec!["capabilities".into(), "--json".into(), "extra".into()]).is_err());
    }
}

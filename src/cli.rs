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
        output_bounded: bool,
        output_limit: Option<u64>,
    },
    Cancel {
        id: String,
        require_group_pidfd: bool,
        output_bounded: bool,
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
        output_limit: Option<u64>,
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
    let mut output_bounded = false;
    let mut group_before_id = false;
    let mut output_limit = None;
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
                group_before_id |= id.is_none();
                group_pidfd = true;
                args.remove(index);
            }
            "--output-bounded" => {
                if id.is_none() {
                    bail!("--output-bounded must follow --id ID");
                }
                output_bounded = true;
                framed = true;
                args.remove(index);
            }
            "--output-limit" => {
                if id.is_none() {
                    bail!("--output-limit must follow --id ID");
                }
                if output_limit.is_some() {
                    bail!("duplicate --output-limit");
                }
                output_limit = Some(parse_limit(args.get(index + 1))?);
                args.drain(index..=index + 1);
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
    if output_bounded && group_before_id {
        bail!("finite output requires --id ID before --group-pidfd");
    }
    if output_limit.is_some() && (!output_bounded || !group_pidfd || nobuffer) {
        bail!("--output-limit requires --output-bounded, --group-pidfd and buffering");
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
            output_bounded,
            output_limit,
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
    let output_bounded = cancel && args.iter().any(|s| s == "--output-bounded");
    if output_bounded {
        args.retain(|s| s != "--output-bounded");
    }
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
            output_bounded,
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
    let mut output_limit = None;
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
            "--output-limit" => {
                if output_limit.is_some() {
                    bail!("duplicate --output-limit");
                }
                output_limit = Some(parse_limit(args.get(index + 1))?);
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
    if output_limit.is_some() && (!group_pidfd || nobuffer) {
        bail!("finite output requires buffered group pidfd session");
    }
    Ok(Mode::Broker {
        output_limit,
        group_pidfd,
        id: id.ok_or_else(|| anyhow::anyhow!("broker ID is missing"))?,
        session_dir: session_dir
            .ok_or_else(|| anyhow::anyhow!("broker session directory is missing"))?,
        command,
        nobuffer,
    })
}

fn parse_limit(value: Option<&String>) -> Result<u64> {
    let value = value.ok_or_else(|| anyhow::anyhow!("--output-limit requires decimal bytes"))?;
    if value.is_empty() || !value.bytes().all(|v| v.is_ascii_digit()) {
        bail!("invalid output limit");
    }
    let limit = value.parse::<u64>()?;
    if limit > i64::MAX as u64 {
        bail!("output limit exceeds file offset range");
    }
    Ok(limit)
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
  pipekeep --id ID --output-bounded --group-pidfd --output-limit BYTES -- COMMAND [ARG...]
  pipekeep --id ID --output-bounded [--force] -- COMMAND [ARG...]
  pipekeep cancel --id ID [--require-group-pidfd] [--output-bounded]
  pipekeep session --id ID
  pipekeep pid --id ID
  pipekeep capabilities --json
  pipekeep --version

The outer form reruns TRANSPORT after a disconnection. The --id form creates
or attaches to a detached process session using the opening protocol message.
With --id, --force applies only to attach-only opening requests and supersedes
an existing data attachment without restarting the command.
--framed requires buffering and carries typed frames after a versioned opening.
--output-bounded selects policy-aware framing; creation also requires the checked
shared decimal --output-limit (0..9223372036854775807), --group-pidfd and buffering.
Attach-only openings omit --output-limit and --group-pidfd. Bounded sessions
require output-aware readers and cancellation; use framed stdin EOF intent.
Keep --id ID before new flags, and the workload after -- for old-parser safety.
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
    fn finite_policy_is_checked_and_old_parser_safe() {
        let args = |values: &[&str]| values.iter().map(|value| (*value).to_owned()).collect();
        assert!(parse(args(&[
            "--group-pidfd",
            "--id",
            "x",
            "--output-bounded",
            "--output-limit",
            "0",
            "--",
            "true"
        ]))
        .is_err());
        for limit in ["0", "9223372036854775807"] {
            assert!(matches!(
                parse(args(&[
                    "--id",
                    "x",
                    "--output-bounded",
                    "--group-pidfd",
                    "--output-limit",
                    limit,
                    "--",
                    "true"
                ]))
                .unwrap(),
                Mode::Server {
                    output_limit: Some(_),
                    ..
                }
            ));
        }
        for limit in [
            "-1",
            "+1",
            "",
            "1k",
            "9223372036854775808",
            "18446744073709551616",
        ] {
            assert!(parse(args(&[
                "--id",
                "x",
                "--output-bounded",
                "--group-pidfd",
                "--output-limit",
                limit,
                "--",
                "true"
            ]))
            .is_err());
        }
        assert!(parse(args(&["--output-bounded", "--id", "x", "--", "true"])).is_err());
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

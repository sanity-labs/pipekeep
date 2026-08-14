# pipekeep

`pipekeep` keeps a non-interactive command connected across temporary transport
failures. It preserves ordinary byte-stream stdin and separate stdout/stderr,
without allocating a pseudo-terminal.

```sh
produce |
  pipekeep -- kubectl exec -i my-pod -- \
    pipekeep --id banana -- remote-command |
  consume
```

The first `pipekeep` restarts the opaque transport command after a disconnect.
The second creates a detached broker beside the remote command, or reconnects
to that broker using absolute stream positions. SSH works the same way:

```sh
pipekeep -- ssh host pipekeep --id banana -- command
```

## Build

```sh
cargo build --release
cargo test
```

`pipekeep` currently targets Unix systems. Runtime state is stored under
`$PIPEKEEP_RUNTIME_DIR`, `$XDG_RUNTIME_DIR/pipekeep`, or `/tmp/pipekeep-$UID` (in that
order), with user-only permissions.

## Commands

```text
pipekeep [--nobuffer] [--] TRANSPORT [ARG...]
pipekeep --id ID [--nobuffer] -- COMMAND [ARG...]
pipekeep cancel --id ID
pipekeep pid --id ID
```

`cancel` sends `SIGTERM` to the command process group, waits three seconds by
default, then sends `SIGKILL` if needed. It returns the command's actual final
status. `pid` prints the command leader's PID.

By default, stdin replay uses an unlinked temporary file in the outer process,
and the broker spools stdout/stderr in its private session directory. With
`--nobuffer`, disconnected bytes are discarded and absolute offsets expose
the resulting gaps. Apply `--nobuffer` to both the outer and inner invocation
when input should also use discard semantics.

## Attachment protocol

Each attachment starts with one newline-terminated JSON request on stdin and
one newline-terminated JSON response on stdout. Everything after the response
is transported as ordinary raw stdin, stdout, and stderr. SSH and Kubernetes
Exec preserve those streams; no data framing is exposed to clients.

On reconnect, the client reports the next stdout and stderr bytes it wants:

```json
{"offsets":{"stdout":12312,"stderr":131}}
```

The response reports the next stdin byte the broker needs and the actual output
positions it can provide:

```json
{"offsets":{"stdin":942,"stdout":12312,"stderr":131}}
```

EOF and exit status are sticky broker state. A request's numeric `stdin_eof`
is the absolute input end; `stdin_eof: true` in a response confirms that the
broker has reached it. Response values `stdout_eof` and `stderr_eof` are the
absolute output ends. A retained `exit` contains either `code` or `signal`.
See [design.md](design.md) for the complete reconnection rules.

## Runtime tuning

- `PIPEKEEP_CANCEL_GRACE_SECS`: cancellation grace period (default `3`).
- `PIPEKEEP_SESSION_TTL_SECS`: completed-session replay lifetime (default `300`).

Replay buffers are process-lifetime aids, not durable storage. A broker keeps
completed output and exit state for the TTL so a final transport failure can
still be resumed.

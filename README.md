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

## Wire format

The opening request and response are newline-terminated JSON, as described in
[design.md](design.md). Binary frames then use this fixed header:

```text
+------------+----------------------+-------------------+
| type: u8   | payload length: u32  | payload           |
|            | network byte order   | length bytes      |
+------------+----------------------+-------------------+
```

Data payloads start with an absolute `u64` network-byte-order offset. Frame
types are stdin data (`1`), stdin EOF (`2`), stdout data (`3`), stderr data
(`4`), stdin position (`5`), and exit result (`6`). The exit payload is JSON
containing either `code` or `signal`. Unknown fields in opening JSON messages
are ignored.

## Runtime tuning

- `PIPEKEEP_CANCEL_GRACE_SECS`: cancellation grace period (default `3`).
- `PIPEKEEP_SESSION_TTL_SECS`: completed-session replay lifetime (default `300`).

Replay buffers are process-lifetime aids, not durable storage. A broker keeps
completed output for the TTL so an exit-frame transport failure can still be
resumed.

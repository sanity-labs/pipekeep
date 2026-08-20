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
pipekeep --id ID [--attachment-id ATTACHMENT] [--nobuffer] -- COMMAND [ARG...]
pipekeep detach --id ID --attachment-id ATTACHMENT
pipekeep cancel --id ID
pipekeep pid --id ID
pipekeep capabilities --json
pipekeep --version
```

`cancel` sends `SIGTERM` to the command process group, waits three seconds by
default, then sends `SIGKILL` if needed. It exits with the command's actual
final status and prints one machine-readable JSON line such as
`{"exit":{"signal":15},"outcome":"cancel_won"}`. The `outcome` is
`cancel_won` when the TERM or KILL attempt reached at least one still-live
group member and `already_exited` when the group had already settled before
anything could be signaled; the retained exit result is never relabeled by
the outcome. `pid` prints the command leader's PID.

`--attachment-id` gives a data attachment an opaque identity. Controllers must
generate a fresh, non-reused ID for every attachment so stale detach requests
cannot match a replacement attachment. If omitted, the inner `pipekeep`
generates one for compatibility with older callers. `detach` asks the broker
to release exactly the matching data attachment without
canceling the command or declaring stdin EOF. It prints one JSON line:
`{"outcome":"detached"}` after the broker slot is clear,
`{"outcome":"already_detached"}` when no attachment remains,
`{"outcome":"attachment_mismatch","error":"...","code":"attachment_mismatch"}`
when another attachment owns the slot, or
`{"outcome":"session_missing","error":"session does not exist","code":"session_missing"}`
when the session is absent.

By default, stdin replay uses an unlinked temporary file in the outer process,
and the broker spools stdout/stderr in its private session directory. Both
retentions are unbounded within a session — they grow with the total amount of
data passed through, with no configurable retention limit or backpressure —
which suits controlled experiments with bounded output. With `--nobuffer`,
the inner broker keeps no disconnected output backlog, while the outer wrapper
retains the newest 64 KiB of stdin as a bounded rolling window: a short
detachment can replay that tail without a gap, and older input is discarded as
the window advances. Absolute offsets expose any range no longer retained at
the next reattachment. Apply `--nobuffer` to both the outer and inner
invocation when input should also use bounded-window semantics.

## Compatibility probe

`pipekeep capabilities --json` prints one compact JSON line an upper layer can
use to verify it is talking to a compatible binary:

```json
{"capabilities":["raw-public-streams","absolute-resume-offsets","sticky-stdin-eof","separate-stdout-stderr","process-group-cancel","cancel-outcome","typed-opening-errors","fenced-attachment-detach","terminal-replay","nobuffer"],"name":"pipekeep","protocol":1,"revision":"<source revision>","version":"0.1.0"}
```

`protocol` is the attachment protocol version described below. `revision` is
the build's source revision: exact builds set `PIPEKEEP_BUILD_REV` at compile
time, a Git checkout falls back to its current commit, and `unknown` is used
when neither is available. `pipekeep --version` reports the same revision.
Any other `capabilities` invocation is rejected with an error.

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

Opening failures always keep a human-readable `error`. Missing-session resume
attempts include `code:"session_missing"`; data-attachment contention includes
`code:"session_attached"`. Other failures may remain untyped.

## Runtime tuning

- `PIPEKEEP_CANCEL_GRACE_SECS`: cancellation grace period (default `3`).
- `PIPEKEEP_SESSION_TTL_SECS`: completed-session replay lifetime (default `300`).

Both are read by the detached session broker from its own environment, which
is captured when the session is created. Set them in the environment of the
session-creating remote invocation; later attachments and `cancel` invocations
cannot change them.

Replay buffers are process-lifetime aids, not durable storage. A broker keeps
completed output and exit state for the TTL so a final transport failure can
still be resumed.

## Transport stderr limitation

An opaque transport CLI such as `ssh` or `kubectl` merges its own diagnostics
into the same stderr stream that carries the remote command's stderr, so the
outer `pipekeep` cannot tell them apart. Diagnostics a failing transport writes
after a successful handshake are counted as delivered remote stderr and
invalidate exact stderr resume accounting: the next reattachment either fails
with an offset error (when the miscount points beyond the remote stream) or
silently misses the overcounted remote bytes. Callers that need reliable
recovery should use a transport or API that exposes command stderr separately
from transport errors — the native Kubernetes remote-command API keeps its
protocol error channel separate — or ensure the transport exits without
writing stderr diagnostics (for example `ssh -q`). See
[design.md](design.md) for the full analysis.

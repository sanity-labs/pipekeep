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

## Standalone Acceptance Harness

The repository includes a self-contained acceptance harness that uses only the
compiled `pipekeep` CLI, Rust, `/bin/sh`, files, pipes, and Unix signals. It
does not use Persona code or services, Kubernetes, Postgres, Docker, or a
network dependency.

Run the deterministic smoke with one command:

```sh
cargo run --bin pipekeep-acceptance -- smoke
```

A passing smoke prints controller summaries and ends with lines like:

```text
ok: buffered stdin replay verified len=524288 sha256=05b7fc050ea022110a59b24af26b8f972a4eae005b3024a6b0a3bdc1465b317d transport_deaths=3 seed=0 outer_pid=12345
ok: cancellation JSON and process-group termination verified
ok: negative cases verified
ok: stdout=1760 bytes stderr=1760 bytes exit=42 seed=0 cycles=3
```

The smoke starts a workload once under a detached broker, kills multiple
short-lived attachment proxies while the broker and workload continue, stores
stdout/stderr absolute offsets in files outside each controller process, and
reconnects fresh controller processes to the same session. It then lets the
workload finish while no data attachment is alive, replays the remaining
stdout/stderr exactly once, and observes the authoritative retained exit
status. It also runs a public two-sided stdin replay scenario:
finite deterministic binary producer to one long-lived outer `pipekeep --`
process, a killable local transport process, inner `pipekeep --id`, and a
slow stdin-consuming workload. The harness kills restarted transports while
stdin is in flight, including after producer EOF, then verifies the workload's
exact accepted bytes, deterministic stdout/stderr, and authoritative exit. It
also verifies machine-readable cancellation, process-group termination,
concurrent-attachment rejection, missing-session rejection, ahead-offset
rejection, stale no-buffer replay gap reporting, and replay expiry after the
completed-session TTL.

Seeded chaos mode repeats the same exactness and single-launch assertions with
bounded randomized disconnect timings:

```sh
cargo run --bin pipekeep-acceptance -- chaos --seed 12345 --cycles 12
```

Omit `--seed` to generate and print one, then reuse it to reproduce a failure.
Use `--cycles N` to set the number of forced attachment losses. Use
`--retain-temp` to keep the harness temp directory on success; on failure the
temp directory is retained automatically for diagnostics. If the `pipekeep`
binary is not next to the harness binary, the harness builds it with
`cargo build --bin pipekeep`; `--pipekeep PATH` or `PIPEKEEP_BIN=PATH` can
select an explicit binary.

The harness exercises Pipekeep's intended resumability boundary: attachment or
transport process loss while the detached broker, its session directory, and
the workload's host/container/pod survive. Broker death, container death, pod
replacement, host reboot, or loss of the session directory is outside
Pipekeep's resumability boundary; those failures cannot be resumed by this
protocol. Buffered stdin replay additionally requires the same outer
`pipekeep` process to survive because its default stdin replay buffer is a
process-local unlinked temporary file. Restarting the outer process loses that
buffer and is not claimed as resumable.

## Commands

```text
pipekeep [--nobuffer] [--] TRANSPORT [ARG...]
pipekeep --id ID [--nobuffer] -- COMMAND [ARG...]
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
{"capabilities":["raw-public-streams","absolute-resume-offsets","sticky-stdin-eof","separate-stdout-stderr","process-group-cancel","cancel-outcome","terminal-replay","nobuffer"],"name":"pipekeep","protocol":1,"revision":"<source revision>","version":"0.1.0"}
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

## Runtime tuning

- `PIPEKEEP_CANCEL_GRACE_SECS`: cancellation grace period (default `3`).
- `PIPEKEEP_SESSION_TTL_SECS`: completed-session replay lifetime (default `300`).

Both are read by the detached session broker from its own environment, which
is captured when the session is created. Set them in the environment of the
session-creating remote invocation; later attachments and `cancel` invocations
cannot change them.

Replay buffers are process-lifetime aids, not durable storage. The default
outer stdin buffer is retained only by that outer process. A broker keeps
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

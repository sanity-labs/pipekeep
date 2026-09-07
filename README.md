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
pipekeep --id ID [--nobuffer] [--force] -- COMMAND [ARG...]
pipekeep cancel --id ID
pipekeep pid --id ID
pipekeep capabilities --json
pipekeep --version
```

`cancel` sends `SIGTERM` to the command process group, waits three seconds by
default, then sends `SIGKILL` if needed. It exits with the command's actual
final status when its wait succeeds and prints one machine-readable JSON line such as
`{"exit":{"signal":15},"outcome":"cancel_won"}`. The `outcome` is
`cancel_won` when at least one TERM or KILL syscall succeeded, including
against a zombie-only group. It does not prove a live recipient or which
historical request won after a reply was lost. A later settled call returns
`already_exited`; the retained exit result is never relabeled by the outcome. `pid` prints the command leader's PID.

Ordinary sessions preserve the shipped wait-error fallback: exit 1 lets the
attachment finish and the legacy idle TTL run once both outputs close. That
fallback is not verified command status (for example, inherited SIGCHLD ignore
can cause ECHILD). Opted-in sessions never use this synthetic exit.

For lifetime-safe original-group cancellation, explicitly opt in **when creating
a new session**:

```sh
printf '{"stdin_eof":0}\n' | pipekeep --id job --group-pidfd -- /bin/sh -c 'exit 23'
pipekeep session --id job
pipekeep cancel --id job --require-group-pidfd
```

The opening header and control replies carry a broker-authored `session` fact:

```json
{"capabilities":["group-pidfd-cancel-v1"],"workload_started":true,"cancel_state":"ready"}
```

Verify that fact, not the requested flag or the binary's `capabilities` output.
`session --id` also reports `leader_exit`, output closure flags and any wait/output
failure; `leader_exit` alone is not group settlement. After natural completion,
the guarded cancel above returns actual exit 23 and `already_exited` (CLI status
23). After a successful signal it may return `cancel_won` with the same natural
23. Existing data attachments receive the fact without requesting a retrofit.
`--group-pidfd` on an attach-only request is rejected.

This opt-in requires Linux group pidfd syscalls (upstream 6.9+, or equivalent
verified support). The creating proxy rejects incompatible SIGCHLD policy before
allocating a session directory or dispatching a broker; it need not be a group
leader. Each new broker independently checks default SIGCHLD without
SA_NOCLDWAIT and actual `pidfd_open` and group-scoped signal-zero support on
itself before dispatch. Neither path changes SIGCHLD, and ordinary sessions
gain no policy prerequisite. The broker acquires and verifies its own original-leader pidfd before any
wait adapter. Known unsupported creation fails before workload launch. If
per-child acquisition/verification fails **after launch**, the opening fact has
an empty capability list, `workload_started:true`, `cancel_state:"failed"`, and
`cancel_error`. The broker retains the child wait and available streams within
the session lifetime. Do not retry creation or infer that broker exit killed it.
An opted-in proxy wait error after broker dispatch (including ECHILD), or an
ambiguous readiness timeout, retains the session and reports that the workload
may have started. It does not authorize killing the broker, deleting the session,
or relaunching the workload.

If the same-session query cannot reach a broker, creation may have been rejected
before workload launch, or the broker may have disappeared after dispatch. A
retained directory can then have no broker to expire it. Keep the launch right
consumed; neither a missing socket nor broker death proves workload termination.
Any operator cleanup requires separately established ownership and workload
disposition, not an automatic remove-and-recreate retry.

The private control action `{"action":"cancel-group-pidfd"}` is distinct from
legacy `cancel`. Old brokers reject the unknown action before signaling; new
non-opted brokers also refuse it. The guarded CLI never falls back. An ordinary
`cancel` on an opted-in session uses the same safe operation. Ordinary sessions
retain their existing Unix numeric-group behavior and protocol 1 frames.

One broker-owned operation sends at most one TERM and one KILL, with one grace
deadline. The initiating caller receives the operation's outcome; joined callers
and later settled calls receive `already_exited` plus the same actual exit once
settlement succeeds. Killing/stopping the requesting CLI or losing its socket
does not stop or restart the operation. A lost reply followed by `already_exited`
proves settled state, not historical request attribution. There is no operation
ID or durable historical receipt.

Every active group signal/probe uses the owned original-leader pidfd with
`PIDFD_SIGNAL_PROCESS_GROUP`, including after normal leader reap. Only successful
leader wait **and** original-group ESRCH latch permanent group absence. The
broker forbids further signals before closing the handle under the same lock.
Success additionally requires the actual retained exit and both output EOFs.
Signal success, elapsed time, pidfd readiness, wait failure, output failure and
EPERM/EINVAL/ENOSYS are not settlement.

For opted-in sessions, `PIPEKEEP_CANCEL_SETTLE_SECS` (default 30, minimum 1)
bounds the additional wait after the grace budget; the single operation deadline
is admission + grace + settlement budget. Errors or expiry retain an unresolved
error and permanently stop this operation's signals; later calls return the same
error. This is not a claim that the workload terminated. The idle session TTL
starts afresh after cancellation completion/error and accepted connections reset
it; active operations/connections pin it. Before cancellation, ordinary terminal
exit/output conditions still make the session eligible for idle expiry even if a
same-group child has closed its output and survives. Opted-in grace, settlement
and TTL settings are capped at 86400 seconds each. This is bounded idle retention,
not durable storage or an unconditional wall-clock broker lifetime.

Output/storage failure retention is not bounded here. An opted-in output read
error records failure and leaves that stream unclosed; without cancellation it
cannot become terminal or start idle expiry. Buffer open/write/flush failures
currently stop the drain without recording failure or closing the stream.
Cancellation can fail or time out, but active attachments still pin retention.
Finite spool/retained overflow behavior remains separate work.

The guarantee covers the **original process group only**. A descendant can
escape with setsid/setpgid. Zombies awaiting another parent's reap, uninterruptible
members, permission changes or an escaped output holder can prevent successful
settlement. An escaped child with closed output can survive a successful cancel;
an escaped output holder can make cancellation time out. Broker death/idle expiry
loses authority and may leave workload processes alive. Never reconstruct a
pidfd from `command.pid`, recreate the session, or use a saved numeric fallback.
Engine integration, overflow acceptance and hostile-descendant containment are
separate work.

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

With `--id`, `--force` applies only to attach-only opening requests, meaning
requests that include output `offsets`. It cannot create a session. A forced
attach to an existing broker immediately supersedes the current data
attachment without restarting or signaling the command; if later opening
checks fail, that failed generation is cleared and the broker is left
unattached.

## Compatibility probe

`pipekeep capabilities --json` prints one compact JSON line an upper layer can
use to verify it is talking to a compatible binary:

```json
{"capabilities":["raw-public-streams","absolute-resume-offsets","sticky-stdin-eof","separate-stdout-stderr","process-group-cancel","cancel-outcome","opt-in-group-pidfd-cancel-v1","terminal-replay","nobuffer","forced-attach-takeover"],"name":"pipekeep","protocol":1,"revision":"<source revision>","version":"0.1.0"}
```

`protocol` is the attachment protocol version described below. `revision` is
the build's source revision: exact builds set `PIPEKEEP_BUILD_REV` at compile
time, a Git checkout falls back to its current commit, and `unknown` is used
when neither is available. `pipekeep --version` reports the same revision.
The advertised capabilities describe the invoked binary; an already-running
older broker may ignore the additive `force` field and safely degrade to normal
attachment contention instead of takeover. Any other `capabilities` invocation
is rejected with an error.

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

The Linux real-binary cancellation matrix runs through `cargo test`. Unsupported
runtimes are reported explicitly. A capable local/CI gate must use
`PIPEKEEP_REQUIRE_GROUP_PIDFD_TESTS=1 cargo test --test group_pidfd -- --nocapture`;
that gate fails on unavailable support or a missing Python 3 harness. Fixtures
use individual pidfds for cleanup and a process-local test subreaper. The broker
itself does not become a subreaper.

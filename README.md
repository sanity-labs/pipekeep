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
pipekeep --id ID [--nobuffer] [--force] [--framed] -- COMMAND [ARG...]
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
{"capabilities":["raw-public-streams","framed-v1","absolute-resume-offsets","sticky-stdin-eof","separate-stdout-stderr","process-group-cancel","cancel-outcome","opt-in-group-pidfd-cancel-v1","terminal-replay","nobuffer","forced-attach-takeover"],"name":"pipekeep","protocol":1,"revision":"<source revision>","version":"0.1.0"}
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
one newline-terminated JSON response on stdout. In the default raw mode,
everything after the response is ordinary raw stdin, stdout, and stderr. SSH
and Kubernetes Exec preserve those streams. Explicit `--framed` attachments
use the binary format below; the outer transport form continues to use raw mode.

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

## Public framed attachments

`pipekeep --id ID --framed -- COMMAND [ARG...]` uses the same create/attach JSON
request and absolute resume offsets, followed by typed binary frames in both
directions. Put `--framed` after `--id ID`: this strict server-option context
ensures old frontends reject it as an option rather than interpreting it as an
outer transport command. It requires buffered sessions. `--framed --nobuffer` fails before
launch, and a framed attachment to an existing nobuffer broker fails before
forced replacement or stdin/EOF changes. `--framed` is not an outer-form option.

The actual broker must accept the distinct `attach-framed-v1` request and return
`"attachment":"framed-v1"` in its opening. The public frontend verifies that
marker and buffering before forwarding any input frames or successful opening:

```json
{"attachment":"framed-v1","offsets":{"stdin":942,"stdout":12312,"stderr":131}}
```

A new broker can frame an attachment to its existing raw-created session without
launching the command again. An old running broker rejects the distinct action
before takeover or stdin mutation; a new frontend cannot upgrade that broker.
Old frontends reject `--framed` as an unknown server option before creation.
There is no fallback or retry creation after dispatch. A capability listing is
only a binary description; the broker opening is authoritative.

Frames reuse the broker encoding: one type byte, a four-byte unsigned big-endian
payload length, then the payload. Data payloads start with an eight-byte unsigned
big-endian absolute offset; all remaining bytes are workload bytes.

| Type | Direction | Payload |
| --- | --- | --- |
| 1 | Caller → broker | stdin offset + data |
| 2 | Caller → broker | stdin EOF offset, exactly 8 bytes |
| 3 | Broker → caller | stdout offset + data |
| 4 | Broker → caller | stderr offset + data |
| 5 | Broker → caller | stdin position, exactly 8 bytes |
| 6 | Broker → caller | retained exit JSON, e.g. `{"code":23}` or `{"signal":15}` |

The existing 16 MiB payload ceiling remains (at most 16 MiB minus 8 data bytes
per frame); exit payloads are limited to 256 bytes. Type, direction and length
are checked before payload allocation; offset addition is checked for overflow.
Use small input frames and a bounded replay window, e.g. 4 KiB frames and 64 KiB
of unreceipted input. A caller retains bytes from the last receipt through its
sent position, stops sending when that window is full, and continues reading
both outputs and receipts. On reconnect, discard through the broker's opening
stdin position and replay the remaining bytes with their original offsets.
Output resume positions count only fully received workload data bytes separately
for stdout and stderr. Framing/header bytes never advance stream offsets.

Receipts are continuous, monotonic positions actually written to child stdin,
including partial writes. They do not acknowledge application consumption,
JSONL messages, durable storage, or recovery across broker loss. Intermediate
positions may coalesce: each attachment retains one latest `u64` in a watch
channel, not a receipt queue. A sender services at most one receipt before an
output/terminal turn and alternates ready stdout/stderr. Slow readers still
apply transport backpressure; coalescing bounds receipt storage, not latency.

Only an explicit valid EOF frame, opening `stdin_eof`, or independent ordinary
`stdin-eof` control declares sticky EOF intent. A framed opening additionally
reports `stdin_eof_at` whenever intent is known, including future EOF not yet
reached. A lost EOF response never allows input to reopen. Input after reached
EOF, gaps, data beyond declared EOF, invalid frames and truncated transport
detach the attachment. Transport closure never cancels the command or invents
stdin EOF/command exit. On any proxy failure its input forwarding is dropped;
the broker owns ongoing work and retained actual exit and replay tails.
To continue observing the same attachment after sending a workload EOF frame,
keep public stdin open until the retained Exit frame arrives. Closing public
stdin instead detaches the transport, even after an EOF frame; recover the same
session rather than assuming a terminal result.
After forced takeover, discard the old transport. A frame already in that
transport can finish or tear, but its generation cannot start new input/EOF
mutations or publish a receipt into the replacement attachment.
A torn terminal frame is not a command terminal result: reconnect to the same
session. The process exit code alone is not an authoritative terminal frame.

Framed public stderr carries no workload bytes; workload stderr is type 4 on
public stdout. Raw mode and its stderr/replay behavior remain compatible, with
no public continuous receipts. Raw outer stdin replay and buffered output spool
remain unbounded. Choosing framed mode lets a custom caller bound pending stdin;
it does not establish finite output storage or complete buffering acceptance.
Cancellation, output EOF, command exit and original-group settlement retain their
existing distinct meanings and cancellation implementation.

The Linux public matrix runs with `cargo test --test receipts -- --nocapture`.
It requires Python 3 and pidfds. To require actual old-peer coverage, set
`PIPEKEEP_REQUIRE_RECEIPTS_OLD=1` and `PIPEKEEP_RECEIPTS_OLD_BINARY` to a separate
build of accepted revision `2f1329f8ceef95ac97ee4d2ba72955beb321e1ab`.

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

### Opt-in finite retained output

New buffered sessions can select one shared stdout/stderr byte budget:

```text
pipekeep --id ID --output-bounded --group-pidfd --output-limit BYTES -- COMMAND [ARG...]
pipekeep --id ID --output-bounded [--force] -- COMMAND [ARG...]
pipekeep cancel --id ID --output-bounded
pipekeep session --id ID
```

The first form requires a creating opening (no `offsets`). The second requires
attach-only `offsets` and omits both creation flags. `--output-bounded` selects
framing itself. BYTES is checked decimal in `0..9223372036854775807`; zero permits
empty output. Buffering and verified original-group pidfd support are required.
Always put recognized `--id ID` before new flags, including `--group-pidfd`, and
put the command after `--`. This ordering makes old parsers reject safely.
There is no retrofit, automatic command replay, or renewed creation right after
a lost opening, broker death, or retention expiry.

The binary advertises `opt-in-finite-output-v1`; only an opted-in session with
verified control advertises `finite-output-v1`. Opening uses the distinct broker
action `attach-output-v1` and confirms `attachment:"framed-output-v1"`. Legacy
raw, framed-v1, ordinary EOF controls and ordinary cancellation actions reject
these sessions before force takeover, stdin mutation or signaling. Bounded EOF
intent uses the existing type-2 input frame or an aware attachment opening.
`cancel-output-v1` joins the existing single broker operation. Unlimited and
nobuffer session contracts are unchanged.

Input frames 1/2, output bytes 3/4 and stdin receipts 5 keep their existing
encodings and absolute offsets. Bounded attachments replace type 6 (Exit) with:

| Frame | Payload | Meaning |
| --- | --- | --- |
| 7 | JSON `OutputFact`, at most 4096 bytes | Coalesced current retained facts |
| 8 | JSON `OutputFact`, at most 4096 bytes | End of replay of the available prefix |

An `output` fact also appears in the opening, `session` observation and aware
cancellation response. It records the limit, immutable first resource-stop
reason, per-stream retained/reserved/discarded bytes, typed storage fault,
collection state (`reading`, actual `eof`, or `unconfirmed` with a cause), prefix
completeness, outstanding filesystem work, sealing, actual leader result,
latched original-group absence and typed group-control status. Completeness is
false while I/O is unresolved. A replay end is never evidence of workload pipe
EOF; the old `stdout_eof`, `stderr_eof` and `exit` opening fields are omitted for
bounded attachments. Type 6 is rejected in this mode. An actual exit 0 with
incomplete output yields frontend status 1, while the fact retains actual code 0.
A nonzero code or signal is preserved. A cancellation error keeps its available
facts in the aware JSON response. `session` can observe facts while detached or
while an attachment's replay filesystem operation is blocked.

The opening fixes the finite output policy and limit for that attachment. Later
facts cannot change the limit. Before forwarding replay end, the frontend checks
both retained boundaries against delivered absolute positions, including nonzero
resume offsets. Intermediate progress facts may lead the delivered bytes.
Bounded openings reject the presence of legacy `exit`, `stdout_eof` and
`stderr_eof` fields before forwarding the opening or stdin.

`original_group_absent` is the permanent latch for original-group absence.
`group_control` describes the cancellation operation, and can still be `running`
when retained collection end is emitted, before the owner's next step records
settlement or failure. Consumers must use the absence latch for that group fact.

Reservations cover both streams and pending writes before dispatch to storage.
Only returned successful file-write counts become public offsets. First excess,
not reaching the limit, seals admission and starts/joins TERM/grace/KILL without
waiting for storage, output EOF or a controller. Positive-size reads remain
possible at the exact budget. There is one collector coordinator, two 4096-byte
write slots and two 4096-byte read buffers. With healthy storage, overflow
consumes exactly one excess byte before pausing. A storage fault racing a normal
read can additionally leave at most one already-read 4096-byte chunk to account.

Normal reads pause until actual leader reap plus a later original-group pidfd
ESRCH. Then one fair shared tail drain discards at most 262144 bytes in reads of
at most 4096 bytes, for at most 200 ms, clamped to the original cancellation
finish deadline. Output, joins and redelivery never extend deadlines. Reaching
the byte cap without an EOF-observing read remains unconfirmed. Only a positive
read returning zero establishes EOF. Bound exhaustion closes collectors with a
precise cause and preserves prior group absence and actual exit. Pipe capacities
vary; the cap is not a promise of complete collection on every pipe size.
**Paused reads can force SIGKILL when a TERM handler tries to flush output.** This
resource-stop tradeoff is deliberate. Escaped holders can survive both original
group absence and collector closure; this is not whole-tree containment.

A failed manual cancellation also closes finite collectors at its original
operation deadline, even when no output policy stop occurred. This preserves the
actual leader result and reports unconfirmed collection; escaped pipe holders
can remain alive after that closure.

Spool write/read-open and socket setup precede launch. The bounded startup
`prelaunch` marker is removed before dispatch; an observed broker exit cannot
justify frontend cleanup once that marker is absent. It is a local startup
receipt, not restart recovery or launch authority. Postlaunch output/storage
faults never use the generic failure field that aborts group-control progress.
If postlaunch pidfd acquisition fails, collection continues within budget until
actual EOF or an actual stop/operation requires closure. Control remains
unavailable and bounded attachment remains refused. Overflow or a storage fault
still closes collection as unconfirmed without fabricating group absence,
signaling by numeric PID, or launching another command.

Blocking writes and replay operations own their filesystem handles and lifetime
pins until actual completion. Dropping their awaiting futures does not cancel
kernel I/O, reuse reservations or authorize unlink. Replay has a single gate
held inside its blocking worker; superseded attachments cannot accumulate
blocking readers. Unresolved work keeps its finite reservation and can pin the
broker indefinitely. Returned partial writes retain exact known prefixes;
returned errors release only known unwritten reservation and latch uncertainty.
Writes use unbuffered `std::fs::File` completion; `flush` is not `fsync` and gives
no crash/power-loss durability promise.

Once collectors have finished and storage has resolved, existing idle retention
(default 300 seconds, hardened clamp 86400) applies after terminal output or a
finished cancellation, including an uncertain cancellation. Active attachments,
accepted controls and active cancellation pin retention and reset idle time.
Expiry can discard uncertain facts; it proves neither death nor cleanup or
creation authority. The byte limit covers logical stdout/stderr file contents,
not metadata, broker logs, filesystem allocation overhead, aggregate sessions or
aggregate memory. Existing input frame and connection admission bounds remain
separate. This contract assumes the surviving broker and returning filesystem
operations; broker/host restart, hostile same-user filesystem mutation, power
loss and whole-tree containment are outside it. Aggregate Engine active/retained
session admission is a separate follow-on.

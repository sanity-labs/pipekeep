# pipekeep: resumable process pipes

## Purpose

`pipekeep` keeps a non-interactive process connected across temporary transport
failures. It preserves the useful shape of a normal process invocation:

- stdin remains a byte stream into the process;
- stdout and stderr remain separate byte streams;
- no pseudo-terminal is allocated;
- the process continues running while no client is attached; and
- a reconnecting client can request stdout and stderr from its last known byte
  positions and retransmit stdin from the position requested by the server.

The primary use case is a command started through Kubernetes Exec:

```sh
produce |
  pipekeep -- kubectl exec -i my-pod -- \
    pipekeep --id banana -- remote-command |
  consume
```

The same design works over SSH because `pipekeep` treats the transport as an
opaque command:

```sh
produce |
  pipekeep -- ssh host pipekeep --id banana -- remote-command |
  consume
```

The outer `pipekeep` exposes ordinary pipes locally. The inner `pipekeep` owns the
remote process and its pipes. If the transport exits, the outer process keeps
its local pipes open, starts the same transport command again, and resumes the
session.

## Scope

`pipekeep` does one thing: create, reconnect to, or cancel one process with one
stdin writer and one stdout/stderr reader.

It is not a terminal multiplexer, job scheduler, process supervisor, or remote
shell. It does not allocate a PTY, manage multiple readers, or provide a
general remote-control interface beyond attachment detach, cancellation, and
PID lookup.

The session exists only inside the lifetime and namespaces of the machine,
container, or pod where it was created. In particular, Kubernetes Exec can be
resumed after a connection failure, but not after the container is restarted
or the pod is replaced.

## Command line

```text
Usage:
  pipekeep [--] TRANSPORT [ARG...]
  pipekeep --id ID [--attachment-id ATTACHMENT] [--nobuffer] -- COMMAND [ARG...]
  pipekeep detach --id ID --attachment-id ATTACHMENT
  pipekeep cancel --id ID
  pipekeep pid --id ID

Modes:
  pipekeep -- TRANSPORT ...
      Run a transport command. Re-run it after disconnection and resume the
      pipekeep protocol without closing local stdin, stdout, or stderr.

  pipekeep --id ID -- COMMAND ...
      Create session ID and run COMMAND, or attach to that session when the
      opening protocol message requests a resume. Only one client may be
      attached at a time.

  pipekeep detach --id ID --attachment-id ATTACHMENT
      Release exactly the matching data attachment, wait until broker
      ownership is clear, and leave the command and sticky stdin EOF state
      untouched.

  pipekeep cancel --id ID
      Request graceful termination, escalate if necessary, wait until the
      process group is settled, and return the command's authoritative
      terminal result.

  pipekeep pid --id ID
      Print the PID of the session's command. This permits normal operating
      system tools to be used for signalling and inspection.

Options:
  --id ID
      Name the remote session.

  --attachment-id ATTACHMENT
      Fresh, non-reused opaque identity for this data attachment. Controllers
      must generate a new value for every attachment so stale detach requests
      cannot match a replacement. If omitted, the server form generates a
      unique local value for compatibility with older callers.

  --nobuffer
      Do not spool unbounded replay data. The outer form keeps only a bounded
      64 KiB rolling stdin tail; the inner form keeps no disconnected output
      backlog. Counters still advance and the protocol still reports offsets,
      and bytes no longer retained are omitted at the next reattachment.
```

The transport command is deliberately unspecified. It can be `kubectl exec`,
`ssh`, or another program that provides bidirectional byte streams. For
Kubernetes, `-i` is required and `-t` must not be used.

`cancel` is run where the session broker lives. For example:

```sh
kubectl exec my-pod -- pipekeep cancel --id banana
ssh host pipekeep cancel --id banana
```

Cancellation sends `SIGTERM` to the command's process group, waits for a grace
period, and sends `SIGKILL` to any remaining members. It then waits until the
entire process group is settled and the broker has recorded the terminal
result.

`cancel` returns that terminal result together with a machine-readable
`outcome`: `cancel_won` when the TERM or KILL attempt reached at least one
still-live member of the recorded process group, `already_exited` when the
group had settled before cancellation could signal any remaining member. The
outcome is decided from the actual signal attempts, so a group that
disappears between inspection and signaling resolves honestly to
`already_exited` rather than a fabricated win. If the command exits naturally
before cancellation wins the race, its real exit result is preserved rather
than being relabeled as canceled. Already-buffered stdout and stderr remain
available, and an attached client receives the remaining output and the same
terminal result. A session that does not exist is an error.

## Session model

On first connection, the inner `pipekeep` starts a small session broker and the
requested command. The broker owns the child's three pipes and survives the
loss of the particular Kubernetes Exec or SSH process that created it. Later
invocations with the same ID connect to that broker.

Broker tunables — `PIPEKEEP_SESSION_TTL_SECS` and `PIPEKEEP_CANCEL_GRACE_SECS`
— are read by the broker from its own environment, which is inherited from the
invocation that created the session. Later attachments and `cancel`
invocations cannot change them; set them in the environment of the
session-creating remote invocation.

A session has at most one attached data client. Every data attachment has an
opaque attachment ID. A second data attachment is rejected with a typed opening
error, although idempotent control requests such as an EOF declaration may run
alongside it. This avoids ambiguous stdin ownership and output-consumption
rules.

`pipekeep detach --id ID --attachment-id ATTACHMENT` connects to the broker and
asks it to release exactly that attachment. If the active ID matches, the
broker signals only that attach loop, waits until its fenced guard has actually
cleared broker ownership, then acknowledges with `{"outcome":"detached"}`.
If no attachment remains it returns `{"outcome":"already_detached"}` and exits
successfully. If another attachment owns the slot it returns
`{"outcome":"attachment_mismatch","error":"...","code":"attachment_mismatch"}`
without detaching it. An absent session returns
`{"outcome":"session_missing","error":"session does not exist","code":"session_missing"}`.
Detach never cancels the command process group and never declares or changes
sticky stdin EOF.

The broker records absolute byte positions for all three streams. Position
`N` means that `N` bytes precede the next byte. Positions begin at zero and
never move backwards, including when buffering is disabled or retained data is
discarded.

## Connection handshake

Every attachment begins with one newline-terminated JSON object from the
client. JSON leaves room for compatible additions without changing the command
line or transport.

A client that intends to create a new session sends:

```json
{}
```

Creation fails if the ID already exists. A reconnecting client sends the next
stdout and stderr positions it wants:

```json
{"offsets":{"stdout":12312,"stderr":131}}
```

The server replies with one newline-terminated JSON object before any command
output:

```json
{"offsets":{"stdin":942,"stdout":12312,"stderr":131}}
```

The returned stdin position is authoritative: it is the first stdin byte the
server still needs. The client sends raw stdin beginning at that position. The
returned stdout and stderr positions are the first raw bytes the server will
provide. They may be greater than requested when data was not retained.

Opening errors are JSON objects with a human-readable `error`. Stable machine
codes are present for authoritative missing-session resume attempts and live
data-attachment contention:

```json
{"error":"session does not exist","code":"session_missing"}
{"error":"session already has an attached client","code":"session_attached"}
```

Other opening failures may remain untyped.

A client that has already observed local stdin EOF includes its absolute end:

```json
{"offsets":{"stdout":12312,"stderr":131},"stdin_eof":2048}
```

If stdin EOF is discovered during an attachment, the client makes a separate
short invocation with an EOF declaration while leaving the data attachment
open:

```json
{"action":"stdin-eof","stdin_eof":2048}
```

The broker retains this declaration. It closes command stdin after accepting
byte 2047, even if the EOF declaration's transport disappears, and repeated
declarations of EOF at 2048 are harmless. `stdin_start` may identify the first
input byte a no-buffer client still retains; it is zero and omitted for a
buffered client.

The response can also report sticky stream and process state:

```json
{
  "offsets":{"stdin":2048,"stdout":12312,"stderr":131},
  "stdin_eof":true,
  "stdout_eof":14000,
  "stderr_eof":131,
  "exit":{"code":0}
}
```

An output EOF value is that stream's absolute end. `exit` is reported only
after the command has terminated and both output streams have closed. A client
can therefore replay through the advertised ends and return the retained exit
status without trusting the status of the particular transport invocation.

Unknown JSON fields are ignored. A future incompatible protocol can add an
explicit version field and reject versions it cannot understand. The
`pipekeep capabilities --json` probe reports this handshake as attachment
protocol `1`, together with the package version, the build's source revision,
and the supported capability names, including `typed-opening-errors` and
`fenced-attachment-detach`, so an upper layer can verify binary compatibility
before creating sessions.

## Raw stream transport

After the JSON exchange, the client and remote `pipekeep` use the transport's
ordinary streams directly:

| Direction | Transport stream | Meaning |
| --- | --- | --- |
| client to server | stdin | Command input beginning at the returned stdin offset |
| server to client | stdout | Command stdout beginning at the returned stdout offset |
| server to client | stderr | Command stderr beginning at the returned stderr offset |

SSH and Kubernetes Exec already transport and reconstruct these three streams.
The short-lived remote `pipekeep` translates between them and the detached
broker's private local protocol. A public protocol client does not parse data
frames: it copies bytes and advances one counter after each successful local
pipe write.

Transport closure is never interpreted as command stdin EOF: it means only
that the attachment was lost. Input EOF is the sticky absolute position
declared in a header. Output EOF and exit status are likewise retained broker
state and are reported again on later attachments.

Offsets make retries idempotent at the pipe boundary. The server tells the
client where to restart stdin, while the client tells the server where to
restart stdout and stderr. A forward jump in an opening response denotes a gap
rather than silently renumbering a stream.

## Transport stderr ambiguity in the outer wrapper

An opaque transport CLI such as `ssh` or `kubectl` merges diagnostics from the
transport itself into the same stderr stream that carries the remote command's
stderr. The outer `pipekeep` reads that merged stream, so it fundamentally
cannot distinguish remote command stderr from transport-emitted diagnostics
(for example an SSH `packet_write_wait` or connection-closed message written
when a connection breaks).

The consequence is precise: every stderr byte the outer wrapper delivers
advances its stderr resume position, so diagnostics injected by a failing
transport are counted as delivered remote stderr. The next reattachment then
requests a stderr position that is too far ahead. If it is beyond the remote
stream's end, the broker rejects the attachment ("requested output offset is
ahead of the stream") and the outer wrapper fails rather than corrupting the
stream. If it still falls inside the remote stream, the overcounted remote
bytes are never delivered locally. Exact stderr resume accounting is therefore
invalid after a transport that injects stderr diagnostics; `pipekeep` does not
clamp offsets or skip bytes to paper over this, because doing so would turn a
detectable failure into silent corruption.

Callers that require reliable stderr recovery should either:

- use a transport or API that carries command stderr and transport errors on
  separate channels — the native Kubernetes remote-command API keeps its
  protocol error channel separate from the command's stderr stream, and is the
  intended integration seam for native clients; or
- ensure the transport command exits without writing diagnostics to stderr
  (for example `ssh -q` disables most diagnostic output).

Diagnostics from a transport attempt whose handshake never completes are not
counted: the outer wrapper starts relaying and counting stderr only after a
successful handshake, and abandons a failed attempt's stderr unread. Once a
handshake has succeeded, every stderr byte of that attempt is counted,
including any the transport wrote while the handshake was in flight.

## Buffering and gaps

By default, both ends retain data needed for a likely reconnect:

- the outer `pipekeep` retains all stdin in an unlinked temporary file so any
  server-requested restart position can be replayed; and
- the remote broker spools all stdout and stderr to files in its private
  session directory so a reconnecting client can request any position.

Neither retention is bounded: within a session, spooling grows with the total
amount of data that has passed through, and no retention or backpressure limit
is configurable. This is deliberate for controlled experimentation with
bounded-output commands; `--nobuffer` is the bounded alternative for streams
that are too large to spool.

Replay is best effort, not durable storage. Process loss, local client loss,
or session cleanup can make bytes unavailable. Absolute offsets make such gaps
detectable by protocol clients.

With `--nobuffer`, the protocol is unchanged but disconnected backlog is
bounded or absent. The remote broker holds no disconnected output backlog: it
continues draining and discarding stdout and stderr so the command can run.
The outer process keeps the producer running by retaining only the newest
64 KiB of stdin as a bounded rolling window, whose first retained byte it
advertises as `stdin_start`: a short detachment can replay that tail without a
gap, while older stdin is discarded as the window advances. On reconnect, the
returned positions advance to the first currently available bytes, and the
transparent local pipes omit any range no longer retained. Gap omission
happens only at that reattachment boundary: within a live attachment, output
is relayed through a small fixed queue, and a client that falls behind it
causes the attachment to close — the short-lived remote proxy detects the
resulting offset gap and exits without writing any diagnostic to the public
command streams, which after the handshake carry only native command bytes —
rather than having bytes silently skipped mid-stream. The next attachment then
advances to the live positions at its opening handshake as usual. A caller
using the protocol directly can detect every gap from the offsets.

## Kubernetes Exec

`pipekeep` is designed to work through the Kubernetes remote-command API without
Kubernetes-specific behavior in the protocol. An API client:

1. opens an Exec request with stdin, stdout, and stderr enabled and TTY disabled;
2. invokes `pipekeep --id ID -- COMMAND ...` in the container;
3. sends the JSON handshake on stdin;
4. parses the JSON response on stdout;
5. copies raw stdin, stdout, and stderr while counting delivered bytes;
6. declares stdin EOF by its absolute position when needed; and
7. opens a new Exec request with the output offsets after disconnection.

The command line is identical on every attempt. Only the opening JSON object
changes. The ordinary `kubectl` case uses the outer `pipekeep` to perform these
steps and restart `kubectl exec` automatically.

The binary must be available inside the target container. The session broker
and command share that container's process and filesystem namespaces, which is
why a replacement container cannot resume the session.

## SSH

SSH is simply another transport:

```sh
pipekeep -- ssh host pipekeep --id banana -- command
```

No SSH extension is required. The outer `pipekeep` reruns `ssh` after a broken
connection, and the newly invoked inner `pipekeep` attaches to the existing
broker. SSH authentication, host selection, jump hosts, and connection policy
remain the responsibility of `ssh` and its configuration.

## Expected guarantees

While the session and required buffers remain available, `pipekeep` preserves
byte order within each stream, prevents replayed stdin bytes from being written
twice, and avoids replaying stdout or stderr already delivered by the same
outer client.

There is no total ordering between stdout and stderr, matching ordinary
separate pipes. Delivery to a local file descriptor does not mean a downstream
application has processed the byte. The protocol improves reconnection; it
does not make the command or its side effects transactional.

# rpipe: resumable process pipes

## Purpose

`rpipe` keeps a non-interactive process connected across temporary transport
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
  rpipe -- kubectl exec -i my-pod -- \
    rpipe --id banana -- remote-command |
  consume
```

The same design works over SSH because `rpipe` treats the transport as an
opaque command:

```sh
produce |
  rpipe -- ssh host rpipe --id banana -- remote-command |
  consume
```

The outer `rpipe` exposes ordinary pipes locally. The inner `rpipe` owns the
remote process and its pipes. If the transport exits, the outer process keeps
its local pipes open, starts the same transport command again, and resumes the
session.

## Scope

`rpipe` does one thing: create, reconnect to, or cancel one process with one
stdin writer and one stdout/stderr reader.

It is not a terminal multiplexer, job scheduler, process supervisor, or remote
shell. It does not allocate a PTY, manage multiple readers, or provide a
general remote-control interface beyond cancellation and PID lookup.

The session exists only inside the lifetime and namespaces of the machine,
container, or pod where it was created. In particular, Kubernetes Exec can be
resumed after a connection failure, but not after the container is restarted
or the pod is replaced.

## Command line

```text
Usage:
  rpipe [--] TRANSPORT [ARG...]
  rpipe --id ID [--nobuffer] -- COMMAND [ARG...]
  rpipe cancel --id ID
  rpipe pid --id ID

Modes:
  rpipe -- TRANSPORT ...
      Run a transport command. Re-run it after disconnection and resume the
      rpipe protocol without closing local stdin, stdout, or stderr.

  rpipe --id ID -- COMMAND ...
      Create session ID and run COMMAND, or attach to that session when the
      opening protocol message requests a resume. Only one client may be
      attached at a time.

  rpipe cancel --id ID
      Request graceful termination, escalate if necessary, wait until the
      process group is settled, and return the command's authoritative
      terminal result.

  rpipe pid --id ID
      Print the PID of the session's command. This permits normal operating
      system tools to be used for signalling and inspection.

Options:
  --id ID
      Name the remote session.

  --nobuffer
      Do not retain disconnected stream data. Counters still advance and the
      protocol still reports offsets, but unavailable bytes are skipped.
```

The transport command is deliberately unspecified. It can be `kubectl exec`,
`ssh`, or another program that provides bidirectional byte streams. For
Kubernetes, `-i` is required and `-t` must not be used.

`cancel` is run where the session broker lives. For example:

```sh
kubectl exec my-pod -- rpipe cancel --id banana
ssh host rpipe cancel --id banana
```

Cancellation sends `SIGTERM` to the command's process group, waits for a grace
period, and sends `SIGKILL` to any remaining members. It then waits until the
entire process group is settled and the broker has recorded the terminal
result.

`cancel` returns that terminal result. If the command exits naturally before
cancellation wins the race, its real exit result is preserved rather than
being relabeled as canceled. Already-buffered stdout and stderr remain
available, and an attached client receives the remaining output and the same
terminal result. A session that does not exist is an error.

## Session model

On first connection, the inner `rpipe` starts a small session broker and the
requested command. The broker owns the child's three pipes and survives the
loss of the particular Kubernetes Exec or SSH process that created it. Later
invocations with the same ID connect to that broker.

A session has at most one attached protocol client. A second attachment is
rejected. This avoids ambiguous stdin ownership and output-consumption rules.

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

The server replies with one newline-terminated JSON object before any binary
frames:

```json
{"offsets":{"stdin":942,"stdout":12312,"stderr":131}}
```

The returned stdin position is authoritative: it is the first stdin byte the
server still needs. The outer `rpipe` retransmits from that point. The returned
stdout and stderr positions are the first bytes the server can actually
provide. They may be greater than requested when data was not retained.

Unknown JSON fields are ignored. A future incompatible protocol can add an
explicit version field and reject versions it cannot understand.

## Framed stream protocol

After the JSON exchange, the connection switches to binary frames. Framing is
necessary because acknowledgements, EOF, exit status, stdout, and stderr must
share a transport without contaminating the user's streams.

Each frame contains a type, a payload length, and a payload. Data frames also
carry the absolute offset of their first byte.

| Direction | Frame | Meaning |
| --- | --- | --- |
| client to server | stdin data | Input bytes and their absolute starting offset |
| client to server | stdin EOF | Intentional EOF and the final stdin offset |
| server to client | stdout data | Output bytes and their absolute starting offset |
| server to client | stderr data | Error bytes and their absolute starting offset |
| server to client | stdin position | Next stdin byte still needed by the server |
| server to client | exit | Command exit status |

Transport EOF is never interpreted as child stdin EOF: it means only that the
attachment was lost. Child stdin closes only after an explicit stdin EOF frame.

Offsets make retries idempotent at the pipe boundary. The server discards an
overlapping stdin prefix it has already accepted. The client discards
overlapping stdout or stderr it has already delivered. A forward jump denotes
a gap rather than silently renumbering the stream.

## Buffering and gaps

By default, both ends retain data needed for a likely reconnect:

- the outer `rpipe` retains stdin until the server advances its stdin position;
- the remote broker retains stdout and stderr until the client reconnects and
  requests them; and
- normal backpressure applies when a configured retention limit is reached.

Replay is best effort, not durable storage. Process loss, local client loss,
retention limits, or cleanup can make bytes unavailable. Absolute offsets make
such gaps detectable by protocol clients.

With `--nobuffer`, the protocol is unchanged but neither side holds a backlog
for a disconnected peer. The remote broker continues draining and discarding
stdout and stderr so the command can run. The outer process continues draining
and discarding stdin so the producer can run. On reconnect, the returned
positions advance to the first currently available bytes. The transparent
local pipes omit the missing ranges; a caller using the protocol directly can
detect them from the offsets.

## Kubernetes Exec

`rpipe` is designed to work through the Kubernetes remote-command API without
Kubernetes-specific behavior in the protocol. An API client:

1. opens an Exec request with stdin, stdout, and stderr enabled and TTY disabled;
2. invokes `rpipe --id ID -- COMMAND ...` in the container;
3. sends the JSON handshake on stdin;
4. parses the JSON response and subsequent frames;
5. records the next delivered stdout and stderr offsets; and
6. opens a new Exec request with those offsets after disconnection.

The command line is identical on every attempt. Only the opening JSON object
changes. The ordinary `kubectl` case uses the outer `rpipe` to perform these
steps and restart `kubectl exec` automatically.

The binary must be available inside the target container. The session broker
and command share that container's process and filesystem namespaces, which is
why a replacement container cannot resume the session.

## SSH

SSH is simply another transport:

```sh
rpipe -- ssh host rpipe --id banana -- command
```

No SSH extension is required. The outer `rpipe` reruns `ssh` after a broken
connection, and the newly invoked inner `rpipe` attaches to the existing
broker. SSH authentication, host selection, jump hosts, and connection policy
remain the responsibility of `ssh` and its configuration.

## Expected guarantees

While the session and required buffers remain available, `rpipe` preserves
byte order within each stream, prevents replayed stdin bytes from being written
twice, and avoids replaying stdout or stderr already delivered by the same
outer client.

There is no total ordering between stdout and stderr, matching ordinary
separate pipes. Delivery to a local file descriptor does not mean a downstream
application has processed the byte. The protocol improves reconnection; it
does not make the command or its side effects transactional.

# CS158A PA2 — Member 2 and Member 3 Implementation Handoff

This document is the compatibility handoff for the missing `peer.py` and
`file_manager.py` modules. The current `protocol.py`, `interfaces.py`,
`logger.py`, `cli.py`, `agent.py`, and `README.md` are the source of truth.
Members 2 and 3 must consume those modules and contracts rather than recreate
their behavior or independently change the shared protocol.

## 1. System overview

Each node is both a TCP client and TCP server. Nodes discover one another by
UDP broadcast on port 54321 without a central directory, then retain TCP
connections for file advertisements, requests, data, and errors. A node
advertises filenames from `shared/`; received files are completed in
`downloads/` before publication into `shared/`. The group must demonstrate at
least five nodes, including discovery, filename propagation, transfer, and
cleanup after a peer exits.

## 2. Member 1 implementation already available

### `protocol.py`

Use these helpers directly:

```python
validate_message(message, *, transport=None) -> dict[str, object]
encode_udp(message) -> bytes
decode_udp(datagram) -> dict[str, object]
encode_message(message) -> bytes
send_message(sock, message) -> None
receive_message(sock) -> dict[str, object]
encode_file_content(data: bytes) -> str
decode_file_content(content: str) -> bytes
```

It supplies strict message schemas, UTF-8 JSON serialization, UDP datagram
encoding, TCP framing and exact-read loops, base64 conversion, size checks,
and these exceptions:

```python
class ProtocolError(ValueError): ...
class PeerDisconnected(ConnectionError): ...
```

`send_message()` uses `sendall()` but deliberately does not lock a socket.
Member 2 must hold the relevant per-peer send lock. `receive_message()` loops
over short reads; only EOF or a connection error indicates disconnection.

### `interfaces.py`

This is the authoritative public API contract. Shared types are:

```python
Message = dict[str, object]
MessageHandler = Callable[[str, Message], None]
PeerCallback = Callable[[str], None]
```

Callbacks execute on networking threads and must be called outside internal
peer-table locks. Query methods return detached snapshots.

### `logger.py`

Member 1 provides:

```python
class AgentLogger:
    def __init__(self, path: str = "log.txt") -> None: ...
    def log_message(
        self, direction: str, peer_address: str, message_type: str
    ) -> None: ...
    def log_peer_added(self, peer_address: str) -> None: ...
    def log_peer_removed(self, peer_address: str) -> None: ...
```

It writes thread-safe, flushed lines in this shape:

```text
<local ISO-8601 timestamp> SENT|RECEIVED peer=<ip:port> type=<message type>
<local ISO-8601 timestamp> PEER_ADDED|PEER_REMOVED peer=<ip:port> type=-
```

Member 2 exclusively decides when actual socket messages and peer lifecycle
events occurred and therefore owns all calls to this logger. Neither Member 3
nor the protocol helpers should duplicate those log entries. File/base64
content is never logged.

### `cli.py`

The CLI uses only the public contracts:

| Command | Public call |
|---|---|
| `peers` | `PeerManager.get_peers()` |
| `files` | `FileManager.get_available_files()` |
| `shared` | `FileManager.get_local_files()` |
| `download <peer_id> <filename>` | `FileManager.request_file(peer_id, filename)` |
| `help` | No component call |
| `quit` / `exit` | Returns to `agent.py` for shutdown |

`request_file()` may raise `ValueError`, `ConnectionError`, or `OSError`; the
CLI displays these as request failures. A successful return means only that
the asynchronous request was submitted, not that the download completed.

### `agent.py`

The coordinator constructs and wires the concrete classes, registers the four
file handlers and two lifecycle callbacks, starts the file component before
networking, and stops networking before the file component. It also creates
the configured `shared/` and `downloads/` directories. Members 2 and 3 should
not duplicate this composition logic.

## 3. Shared wire protocol

All messages are JSON objects encoded as UTF-8. Field names and message type
names are case-sensitive. Schemas are exact: missing fields, extra fields,
wrong types, unknown types, and use on the wrong transport raise
`ProtocolError`. Whitespace and JSON object key order are insignificant.

| Message | Transport | Exact required fields |
|---|---|---|
| `PEER_REQUEST` | UDP | `type: str`, `node_id: str` |
| `PEER_ACK` | UDP | `type: str`, `node_id: str`, `tcp_port: int` |
| `HELLO` | TCP | `type: str`, `node_id: str` |
| `FILE_LIST` | TCP | `type: str`, `files: list[str]` |
| `FILE_REQUEST` | TCP | `type: str`, `filename: str` |
| `FILE_DATA` | TCP | `type: str`, `filename: str`, `content: str` |
| `FILE_ERROR` | TCP | `type: str`, `filename: str`, `reason: str` |

Examples:

```json
{"type":"PEER_REQUEST","node_id":"node-A"}
{"type":"PEER_ACK","node_id":"node-B","tcp_port":5001}
{"type":"HELLO","node_id":"node-A"}
{"type":"FILE_LIST","files":["notes.txt","photo.png"]}
{"type":"FILE_REQUEST","filename":"notes.txt"}
{"type":"FILE_DATA","filename":"notes.txt","content":"SGVsbG8="}
{"type":"FILE_ERROR","filename":"missing.txt","reason":"File not found"}
```

### UDP encoding

One datagram contains one complete JSON payload with no length prefix. Only
`PEER_REQUEST` and `PEER_ACK` are valid on UDP.

### TCP framing

Every TCP message, including `HELLO`, is encoded as:

```text
+-------------------------------+------------------------------------+
| 4-byte unsigned BE byte count | Exactly that many UTF-8 JSON bytes |
+-------------------------------+------------------------------------+
```

The count excludes the header. `receive_message()` rejects a zero length and
any advertised payload above the maximum before reading the body. A clean EOF
before a new header and an EOF in a partial header/body both raise
`PeerDisconnected`; incomplete frames are never dispatched.

### Validation limits

- Maximum encoded JSON payload: 16 MiB (`16 * 1024 * 1024` bytes).
- Node ID: nonempty string, at most 128 Unicode characters, unique among
  concurrently running nodes.
- Filename: nonempty string, at most 255 Unicode characters.
- `FILE_LIST.files`: strings only, with no duplicates.
- `PEER_ACK.tcp_port`: integer 1–65535; a JSON boolean is not accepted.
- `FILE_ERROR.reason`: nonempty string, at most 1024 Unicode characters.
- `FILE_DATA.content`: strict standard padded base64 of the complete raw file.
  JSON/base64 overhead counts toward the 16 MiB maximum, so usable raw file
  size is slightly below 12 MiB.

Member 3 must additionally enforce filesystem safety because `protocol.py`
only checks filename type and length. Reject path separators, absolute paths,
directory components, `.`/`..`, and any name that escapes the configured
directory.

### HELLO and duplicate resolution

After `connect()` succeeds, the TCP initiator sends its local node ID in
`HELLO` as the first frame, before any file message. On an accepted socket,
Member 2 must read and validate the complete first frame as `HELLO` before
registering the connection, firing a connected callback, or dispatching file
messages. A missing, malformed, duplicate, late, or self-ID `HELLO` is a
protocol failure; close that unregistered candidate. `HELLO` is handled
internally by `PeerManager` and is never dispatched through
`register_handler()`.

For simultaneous crossing connections, retain the connection initiated by the
lexicographically smaller node ID using ordinary Python string ordering:

- If the local ID is smaller, retain the local outbound connection and reject
  the inbound candidate.
- If the local ID is larger, retain the inbound connection initiated by the
  smaller remote ID and close the local outbound candidate.
- Perform this decision after learning the accepted connection's ID from
  `HELLO` but before registration/callback.
- Closing a rejected candidate must not remove or emit a disconnect callback
  for the retained connection.
- For duplicate candidates with the same initiator direction, retain the
  already registered connection and close the later candidate.

### FILE_ERROR

If a requested file disappeared or cannot be served, Member 3 sends
`FILE_ERROR` containing the requested filename and a nonempty reason. This is
a protocol-level, nonfatal response: keep the TCP connection open. Member 3's
handler reports or records the failure as appropriate; no additional wire
acknowledgment is defined.

The shared protocol has no chunking, checksum, request ID, transfer ACK,
authentication, encryption, retry message, or goodbye message. Do not add or
change message fields/types independently.

## 4. Member 2 handoff — implement `peer.py`

### Required public API

The concrete class does not need to inherit from the protocol type, but it
must be structurally compatible with this exact interface:

```python
class PeerManager:
    def __init__(self, node_id: str, tcp_port: int, logger) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def get_peers(self) -> list[str]: ...
    def send_to_peer(
        self, peer_id: str, message: dict[str, object]
    ) -> None: ...
    def broadcast_to_peers(self, message: dict[str, object]) -> None: ...
    def register_handler(self, message_type: str, handler) -> None: ...
    def on_peer_connected(self, callback) -> None: ...
    def on_peer_disconnected(self, callback) -> None: ...
```

Handler and callback shapes are exact:

```python
handler(peer_id: str, message: dict[str, object]) -> None
callback(peer_id: str) -> None
```

### Discovery and connections

1. Listen for UDP discovery traffic on `protocol.DISCOVERY_PORT` (54321) for
   the full node lifetime.
2. Broadcast `PEER_REQUEST` with the local node ID at startup.
3. Ignore a `PEER_REQUEST` whose node ID equals the local node ID.
4. Reply to another node's request with `PEER_ACK` sent to the request's source
   IP and source UDP port. Advertise this node's configured TCP port.
5. Receive ACKs on the requester's UDP socket. The requesting/new node connects
   to the ACK source IP at the TCP port carried in the ACK.
6. Immediately send local `HELLO` as the first frame on that outbound socket.
7. On accepted sockets, validate and consume `HELLO` before registration.
8. Apply the duplicate policy above so there is at most one retained
   connection per peer ID.

The requester knows an outbound peer's ID from the validated `PEER_ACK`; the
acceptor learns the initiator's ID from `HELLO`. Node IDs are the keys exposed
by `get_peers()` and passed into all handlers/callbacks.

### State, threads, and synchronization

Member 2 owns:

- UDP listener/broadcast behavior and UDP socket lifetime.
- TCP listen/accept/connect behavior and every TCP socket.
- The connected-peer table, address metadata, locks, and connection removal.
- A TCP receiver/dispatch thread for each retained peer.
- The UDP listener and TCP accept-loop threads.
- One send lock per retained TCP connection.

Required concurrency behavior:

- Serialize each complete `protocol.send_message()` call using that peer's
  send lock. Concurrent frames must never interleave.
- Protect peer-table mutation and return a detached list from `get_peers()`.
- Never call message handlers or lifecycle callbacks while holding the
  peer-table lock; callbacks may query state or send messages.
- Register handlers and callbacks before networking starts, as `agent.py`
  already does.
- Dispatch validated `FILE_LIST`, `FILE_REQUEST`, `FILE_DATA`, and
  `FILE_ERROR` messages to the registered handler for their type.
- Consume `HELLO` internally instead of dispatching it.
- Decide how to close a connection after a malformed established-peer message;
  the current contract requires clean failure handling but does not prescribe
  a separate wire error for malformed protocol input.

`send_to_peer()` sends under the peer's lock. If established connection loss
is confirmed, it raises `ConnectionError` or a subclass, removes the peer, and
fires the disconnect callback once. `broadcast_to_peers()` works from a
snapshot and applies the same per-peer behavior. The existing documentation
does not specify whether broadcasting continues after one peer fails; choose a
simple internal behavior and coordinate it if callers need a guarantee.

### Logging and lifecycle notifications

Member 2 is the sole logging owner:

- Log `SENT` only after an actual UDP/TCP send succeeds.
- Log `RECEIVED` once after a datagram/frame successfully decodes and before
  dispatch. Log valid `HELLO` even though it is consumed internally.
- Use the UDP endpoint for discovery logs and TCP endpoint for established
  message/lifecycle logs, formatted as `ip:port`.
- Log `PEER_ADDED` once after a handshaken, retained connection is recorded.
- Fire connected callbacks once after recording it and outside table locks.
- On EOF (`recv` returns `b''`), reset, or established connection failure,
  remove that exact retained connection once, log `PEER_REMOVED`, and fire the
  disconnect callback once outside locks.
- A rejected unregistered duplicate candidate must not generate peer-added or
  peer-removed lifecycle events.

### Startup and shutdown

The constructor stores configuration and callbacks only; it must not start
threads or open listening sockets. `start()` acquires sockets and starts the
UDP listener, TCP accept loop, and receiver threads as connections form.

`stop()` must be idempotent and safe after construction, partial `start()`
failure, complete startup, or an earlier `stop()`. It closes sockets to unblock
receivers/acceptors, signals workers, and joins threads. It sends no goodbye
message. It must clean up every resource that a failed `start()` managed to
create. Do not call callbacks while holding internal locks.

### Interaction boundaries

- Use `protocol.py`; do not duplicate JSON, framing, or base64 logic.
- Use the injected logger; do not create a second message log.
- `agent.py` owns construction, registration, and lifecycle ordering.
- Call Member 3 handlers with `(peer_id, validated_message)`.
- Call Member 3 lifecycle callbacks with `peer_id`.
- Do not scan directories, read/write file content, or own advertised filename
  state.

## 5. Member 3 handoff — implement `file_manager.py`

### Required public API

The concrete class must be structurally compatible with:

```python
class FileManager:
    def __init__(
        self,
        peers,
        shared_dir: str,
        downloads_dir: str,
    ) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def get_local_files(self) -> list[str]: ...
    def get_available_files(self) -> Mapping[str, list[str]]: ...
    def request_file(self, peer_id: str, filename: str) -> None: ...
    def handle_file_list(
        self, peer_id: str, message: dict[str, object]
    ) -> None: ...
    def handle_file_request(
        self, peer_id: str, message: dict[str, object]
    ) -> None: ...
    def handle_file_data(
        self, peer_id: str, message: dict[str, object]
    ) -> None: ...
    def handle_file_error(
        self, peer_id: str, message: dict[str, object]
    ) -> None: ...
    def peer_connected(self, peer_id: str) -> None: ...
    def peer_disconnected(self, peer_id: str) -> None: ...
```

`interfaces.py` types `get_available_files()` as
`Mapping[str, list[str]]`; returning a detached `dict[str, list[str]]`, as shown
in README examples, satisfies the contract and is suitable for the CLI.

### PeerManager use and registered entry points

Store the injected `PeerManager` and use only its public methods:

- `send_to_peer(peer_id, message)` for file lists, data, requests, and errors.
- `broadcast_to_peers(message)` for periodic complete advertisements.
- `get_peers()` when a peer snapshot is needed.

`agent.py` registers these message handlers:

| Message | FileManager method |
|---|---|
| `FILE_LIST` | `handle_file_list` |
| `FILE_REQUEST` | `handle_file_request` |
| `FILE_DATA` | `handle_file_data` |
| `FILE_ERROR` | `handle_file_error` |

It also registers:

| Lifecycle event | FileManager method |
|---|---|
| Peer connected | `peer_connected` |
| Peer disconnected | `peer_disconnected` |

These methods run on networking threads and therefore must not wait on the
interactive CLI.

### Local and advertised file state

- `get_local_files()` scans or otherwise returns a current detached snapshot
  of filenames that are fully present and safe to serve from `shared/`.
- Advertise filenames only—never paths, directory trees, or partial files.
- `peer_connected(peer_id)` immediately sends a complete `FILE_LIST` to that
  peer.
- `start()` launches one worker that broadcasts a complete `FILE_LIST` every
  10 seconds.
- `handle_file_list()` atomically replaces that peer's previous list; it does
  not append to stale state.
- `get_available_files()` returns a detached peer-ID-to-list snapshot safe for
  concurrent CLI iteration.
- `peer_disconnected()` immediately removes the disconnected peer's entire
  advertised-file entry.

Protect the per-peer advertised-file table and any download state shared by
callback and periodic threads. Do not access PeerManager's internal table or
sockets.

### Requests, serving, and errors

`request_file(peer_id, filename)` is called by the CLI. It must:

- Reject unsafe filenames.
- Reject an unknown peer or a filename that peer has not advertised.
- Reject a filename already present in local `shared/`; same-name files are
  treated as the same item and the local copy wins.
- Raise `ValueError` for request validation failures.
- Send exactly one `FILE_REQUEST` through `peers.send_to_peer()`.
- Allow `ConnectionError` from `send_to_peer()` to propagate.
- Treat a successful return as request submission only, not completion.

`handle_file_request()` revalidates the filename at the filesystem boundary.
If the file is present and safe, read its raw bytes, call
`protocol.encode_file_content()`, and send one complete `FILE_DATA`. If it no
longer exists or cannot be served, send `FILE_ERROR` with a nonempty reason and
keep the connection alive.

`handle_file_error()` handles the asynchronous rejection without disconnecting
the peer. The current interface defines no CLI-output callback or return
channel for asynchronous errors; the concrete user-notification policy is not
specified and should not change the public interface without group agreement.

### Receiving and publishing data

`handle_file_data()` must:

1. Revalidate the filename and refuse path traversal.
2. Refuse to overwrite an existing same-name file in `shared/`.
3. Decode content using `protocol.decode_file_content()`.
4. Fully write/stage the received bytes under `downloads/` first.
5. Only after the staged download is complete, atomically copy or move the
   completed file into `shared/`.
6. Never advertise or serve an incomplete download.

The current whole-file protocol has no request IDs or chunks. Avoid silently
adding either. If concurrent same-name responses are possible, synchronize the
collision check/publication so only one completed local file can win.

### Startup and shutdown

The constructor stores configuration and initializes passive state only; it
does not launch the periodic worker. `start()` launches the 10-second
advertisement worker. `stop()` signals and joins every created worker and must
be idempotent and safe after construction, a partial `start()` failure, full
startup, or a prior `stop()`.

Member 3 does not log socket messages or peer lifecycle events and never
creates, reads from, writes to, closes, or locks sockets directly.

## 6. Actual integration contract

The current coordinator uses these integration points:

```python
peers = PeerManager(node_id=args.node_id, tcp_port=args.tcp_port, logger=logger)
files = FileManager(
    peers, shared_dir=str(shared_dir), downloads_dir=str(downloads_dir)
)

peers.register_handler("FILE_LIST", files.handle_file_list)
peers.register_handler("FILE_REQUEST", files.handle_file_request)
peers.register_handler("FILE_DATA", files.handle_file_data)
peers.register_handler("FILE_ERROR", files.handle_file_error)
peers.on_peer_connected(files.peer_connected)
peers.on_peer_disconnected(files.peer_disconnected)

files.start()
peers.start()
CLI(peers, files).run()
```

Cleanup is effectively:

```python
try:
    if peers is not None:
        peers.stop()
finally:
    if files is not None:
        files.stop()
```

Every successfully constructed component participates in cleanup even if
registration or startup fails. This requires resource-free constructors and
idempotent `stop()` implementations capable of cleaning partial startup.

Ownership boundaries:

| State/operation | Owner |
|---|---|
| UDP/TCP sockets, peer table, peer addresses, connection threads | Member 2 |
| Per-peer send locks and TCP dispatch | Member 2 |
| Actual-message and peer-lifecycle logging | Member 2 via `AgentLogger` |
| Advertised filenames by peer and local download/file state | Member 3 |
| Directory scanning, staging, publishing, file-content handling | Member 3 |
| JSON validation, UDP encoding, TCP framing, base64 helpers | Member 1 modules |
| CLI and construction/lifecycle sequencing | Member 1 modules |

## 7. Defined requirements versus open coordination

### Already defined for compatibility

- Exact seven message types, fields, transports, and strict validation.
- UDP discovery port 54321 and requester-initiated TCP connection.
- One-way first-frame `HELLO` and lexicographic duplicate resolution.
- Four-byte big-endian TCP framing and exact receive loops.
- Whole-file strict standard base64 and 16 MiB encoded payload limit.
- Public class methods, handler/callback signatures, and callback lock rules.
- One retained connection per peer ID and per-peer send synchronization.
- Immediate and 10-second complete file advertisements.
- Snapshot query behavior and disconnection advertisement cleanup.
- Missing-file `FILE_ERROR` without forced disconnect.
- Logging ownership and component startup/shutdown ordering.

### Still requiring group agreement or platform verification

The current README explicitly leaves these open:

1. Whether UDP discovery repeats after startup and, if so, its cadence. This
   is separate from the required 10-second `FILE_LIST` cadence.
2. The exact `SO_REUSEADDR`/`SO_REUSEPORT` and bind strategy needed for
   five-process UDP port 54321 behavior on the demonstration operating system.
3. Whether the 16 MiB encoded-payload limit is suitable for the chosen demo
   files. Until the group changes the shared protocol, implementations must
   enforce the current constant.

The current contracts also leave these implementation details unspecified;
they do not require protocol changes, but teammates should avoid depending on
one another's private choice:

- Whether `broadcast_to_peers()` continues to later snapshot peers after one
  peer send fails.
- How `handle_file_error()` surfaces an asynchronous error to a human; there
  is no current CLI notification callback.
- Whether a malformed established TCP message causes immediate connection
  closure. No additional error message is defined for malformed protocol data.

## 8. Consistency review

No incompatible message schemas, method names, callback signatures, or
startup/shutdown orders were found across the reviewed files.

One presentational difference is intentional and compatible:
`interfaces.py` returns `Mapping[str, list[str]]` from
`get_available_files()`, while README examples show
`dict[str, list[str]]`. A detached dictionary satisfies the interface.

No `peer.py` or `file_manager.py` implementation, automated tests, CI,
protocol extensions, demo logs, or fabricated integration results are part of
this handoff.

# CS158A PA2 — P2P Data Sharing Agent

This directory contains Member 1's protocol, public interfaces, logging, CLI,
and application coordinator. `peer.py` belongs to Member 2 and
`file_manager.py` belongs to Member 3 (now implemented). The protocol and APIs below are concrete proposed contracts that the
group should review before treating them as frozen.

## Architecture and ownership

| Module | Owner | Responsibility |
|---|---|---|
| `protocol.py` | Member 1 | Strict message validation, JSON encoding, UDP datagrams, TCP framing, exact reads, base64 helpers |
| `logger.py` | Member 1 | Thread-safe message and peer-lifecycle metadata log |
| `interfaces.py` | Member 1 | Public structural contracts and callback types |
| `cli.py` | Member 1 | Interactive display and request delegation through public APIs |
| `agent.py` | Member 1 | Arguments, construction, callback wiring, lifecycle coordination |
| `peer.py` | Member 2 | UDP/TCP networking, sockets, peer table, receive/dispatch threads, send locks |
| `file_manager.py` | Member 3 | File lists, advertisements, requests, transfers, staging, publication |

There is no central tracker. Every complete node is both client and server.
The networking layer owns sockets and connected-peer lifecycle; the file layer
owns advertised filename state and disk operations. The coordinator owns
neither.

## Proposed wire protocol

All messages are UTF-8 JSON objects. Field names and type names are
case-sensitive. Receivers reject missing fields, extra fields, wrong JSON
types, unknown message types, invalid base64, and use of a message on the
wrong transport. JSON whitespace and object key order are insignificant.

### Message schemas

| Type | Transport | Exact fields | Meaning |
|---|---|---|---|
| `PEER_REQUEST` | UDP | `type: str`, `node_id: str` | Ask existing nodes to identify themselves |
| `PEER_ACK` | UDP | `type: str`, `node_id: str`, `tcp_port: int` | Advertise the ACK sender's TCP port |
| `HELLO` | TCP | `type: str`, `node_id: str` | Identify the TCP initiator before connection registration |
| `FILE_LIST` | TCP | `type: str`, `files: list[str]` | Complete replacement filename list |
| `FILE_REQUEST` | TCP | `type: str`, `filename: str` | Request one file |
| `FILE_DATA` | TCP | `type: str`, `filename: str`, `content: str` | One complete file as standard padded base64 |
| `FILE_ERROR` | TCP | `type: str`, `filename: str`, `reason: str` | Nonfatal inability to serve a request |

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

Validation limits are part of the proposal:

- Encoded JSON payload: at most 16 MiB (`MAX_PAYLOAD_SIZE`).
- Node ID: 1–128 Unicode characters and unique among running nodes.
- Filename: 1–255 Unicode characters. `FILE_LIST` cannot contain duplicates.
- Error reason: 1–1024 Unicode characters.
- TCP port: integer 1–65535; JSON booleans are not accepted as integers.
- File content: strict standard base64. Because JSON and base64 overhead count
  toward the 16 MiB payload, the largest raw file is slightly less than 12 MiB.

Member 3 must additionally reject unsafe filenames: names containing `/` or
`\\`, `.` or `..`, absolute paths, directory components, or any value that can
escape the configured directory. Protocol validation does not perform
filesystem policy checks.

### UDP discovery

Each UDP datagram contains exactly one JSON payload and has no length prefix.
Every node listens on UDP port 54321 for its lifetime and broadcasts a
`PEER_REQUEST` at startup. It ignores its own node ID. A receiving node sends a
`PEER_ACK` to the request datagram's source IP and source UDP port; the ACK
contains the receiver's separate TCP listening port. The requester initiates
the TCP connection to that advertised port and already knows the receiver's
identity from the validated ACK.

Member 2 must receive ACKs on the requester's UDP socket and prevent a second
live TCP connection for the same peer ID. Platform-specific UDP port reuse and
broadcast fan-out must be verified during the five-process demonstration.

### TCP identity handshake and duplicate connections

Immediately after `connect()` succeeds, the initiating TCP client sends
`{"type":"HELLO","node_id":"<its local node ID>"}` as the first framed TCP
message. It must send no `FILE_LIST`, `FILE_REQUEST`, `FILE_DATA`, or
`FILE_ERROR` before that `HELLO` completes. `HELLO` uses the ordinary TCP
framing and payload limit described below.

For every newly accepted socket, Member 2 must synchronously read and validate
the first complete frame as `HELLO` before adding the peer to the connected
peer table, firing `on_peer_connected`, or dispatching any normal file
message. A missing, malformed, duplicate, or non-initial `HELLO`, a self node
ID, or a disconnect before the full `HELLO` is a protocol failure: close the
candidate socket without registering it. `HELLO` is consumed internally by
`PeerManager`; it is never passed to a handler registered through
`register_handler()`.

The deterministic policy for simultaneous crossing TCP connections is based
on the two node IDs. For peers `A` and `B`, the connection initiated by
`min(A, B)` under Python's ordinary lexicographic string ordering is the one
retained. Concretely, the lexicographically smaller local node keeps its
outbound connection and rejects/closes its inbound candidate; the larger node
keeps the inbound connection initiated by the smaller peer and closes its own
outbound candidate. Deduplication must occur after the accepted side learns
the initiator's ID from `HELLO` and before a candidate is registered or causes
a connected callback. Closing a rejected candidate must not remove the
retained connection or fire a disconnect callback for it. If duplicate
candidates have the same initiator direction, retain the already registered
connection and close the later candidate.

### TCP framing

Every TCP message is one frame:

```text
+-------------------------------+------------------------------------+
| 4-byte unsigned BE byte count | Exactly that many UTF-8 JSON bytes |
+-------------------------------+------------------------------------+
```

The count covers the encoded JSON payload only, not the header. The receiver
checks the length against 16 MiB before reading or allocating the body and
loops until the complete header and payload arrive. A short positive `recv`
is normal. Empty `recv` before a header or during a frame raises
`PeerDisconnected`; a partial JSON message is never dispatched.

`send_message()` calls `sendall()` but deliberately does not own a lock.
Member 2 must hold a per-peer send lock around every complete call so frames
from multiple threads cannot interleave. Connection resets, broken pipes, and
other socket failures are Member 2's responsibility.

The connection stays open after `FILE_ERROR`. `FILE_DATA` contains one whole
file; apart from the required one-way `HELLO`, there is no application
handshake, chunking, request ID, checksum, transfer ACK, or goodbye message.
These features must not be added without a coordinated protocol revision.

## Required internal interfaces

`interfaces.py` contains the authoritative typing contracts. The concrete
classes do not need to inherit from the protocols, but their public methods
and behavior must match them.

### Member 2: `peer.py`

```python
class PeerManager:
    def __init__(self, node_id: str, tcp_port: int, logger): ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def get_peers(self) -> list[str]: ...
    def send_to_peer(self, peer_id: str, message: dict[str, object]) -> None: ...
    def broadcast_to_peers(self, message: dict[str, object]) -> None: ...
    def register_handler(self, message_type: str, handler) -> None: ...
    def on_peer_connected(self, callback) -> None: ...
    def on_peer_disconnected(self, callback) -> None: ...
```

Required behavior:

- Use `protocol.encode_udp`/`decode_udp` for discovery and
  `protocol.send_message`/`receive_message` for established TCP connections.
- Send `HELLO` first on every outbound TCP connection. On every accepted
  connection, validate and consume `HELLO` before registration or dispatch.
- Own all sockets, receive threads, peer records, address records, table locks,
  and one send lock per TCP peer.
- `get_peers()` returns a detached snapshot, never the internal collection.
- `send_to_peer()` validates/sends under the per-peer lock. On established
  connection failure it raises `ConnectionError` (or a subclass), performs
  peer removal, and fires the disconnect callback once.
- `broadcast_to_peers()` works from a peer snapshot and applies the same
  failure semantics separately to each peer.
- A message handler has signature
  `handler(peer_id: str, message: dict[str, object]) -> None`.
- A lifecycle callback has signature `callback(peer_id: str) -> None`.
- Registering a handler/callback adds it for future events. Invoke handlers on
  receiver threads. Never invoke any callback while holding a peer-table lock.
- Fire the connected callback once only after a handshaken, retained connection
  is recorded; fire disconnected once only after that record is removed.
- Resolve simultaneous crossing connections by retaining the connection
  initiated by the lexicographically smaller node ID, exactly as specified in
  the handshake section above.
- Constructors must store configuration only; opening listening sockets and
  launching workers belongs in `start()`.
- Close sockets on `stop()` without sending a protocol goodbye. `stop()` must
  be idempotent and safe after construction, partial `start()` failure, full
  startup, or a prior `stop()`, and must signal and join its worker threads.

#### Logging ownership

Member 2 is the sole caller of message and peer-lifecycle logging, because it
knows whether socket activity actually occurred. This prevents duplicates.

- After each successful UDP/TCP send, call
  `logger.log_message("SENT", address, message["type"])`.
- After each successfully decoded UDP/TCP receive, call
  `logger.log_message("RECEIVED", address, message["type"])` exactly once,
  before dispatch.
- Log a valid sent/received `HELLO` like every other actual message even though
  PeerManager consumes it internally rather than dispatching it.
- When a TCP peer becomes available, call `log_peer_added(address)` once.
- When that peer is removed, call `log_peer_removed(address)` once.
- Address strings are supplied as `ip:port`; use the UDP endpoint for discovery
  messages and the TCP endpoint for connection messages/lifecycle.
- Do not log a message as sent when encoding or `sendall()` failed, and never
  include base64 content in the log.

### Member 3: `file_manager.py`

```python
class FileManager:
    def __init__(self, peers, shared_dir: str, downloads_dir: str): ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def get_local_files(self) -> list[str]: ...
    def get_available_files(self) -> dict[str, list[str]]: ...
    def request_file(self, peer_id: str, filename: str) -> None: ...
    def handle_file_list(self, peer_id: str, message: dict[str, object]) -> None: ...
    def handle_file_request(self, peer_id: str, message: dict[str, object]) -> None: ...
    def handle_file_data(self, peer_id: str, message: dict[str, object]) -> None: ...
    def handle_file_error(self, peer_id: str, message: dict[str, object]) -> None: ...
    def peer_connected(self, peer_id: str) -> None: ...
    def peer_disconnected(self, peer_id: str) -> None: ...
```

Required behavior:

- The constructor stores configuration but does not launch workers. `start()`
  creates the periodic worker that advertises the complete local filename list
  every 10 seconds. `stop()` is idempotent, is safe after a partial `start()`
  failure, and signals and joins every worker that was created.
- `peer_connected()` immediately sends the complete local list.
- `handle_file_list()` atomically replaces that peer's previous list.
- `peer_disconnected()` immediately removes all filenames for that peer.
- `get_local_files()` and `get_available_files()` return detached snapshots
  safe for concurrent CLI reading.
- `request_file()` rejects an unknown peer, an unadvertised/unsafe filename,
  and a filename already in local `shared/`. Use `ValueError` for request
  validation failures and allow `ConnectionError` from `send_to_peer()` to
  propagate. A successful return means only that the request was sent—not that
  a download completed.
- If a requested local file disappeared, send `FILE_ERROR` and keep the peer.
- Decode `FILE_DATA`, fully stage it under `downloads/`, then atomically copy or
  move the completed file into `shared/`. Never advertise or serve partial
  data, and never overwrite an existing same-name shared file.
- Validate paths again at the filesystem boundary. Use
  `protocol.encode_file_content` and `decode_file_content` for content.
- Handler/callback methods run on networking threads and must synchronize
  shared state without waiting on the interactive CLI.

## Startup and shutdown contract

`agent.py` performs this exact integration sequence:

1. Construct `AgentLogger`, `PeerManager`, and `FileManager`.
2. Register all four TCP message handlers and both lifecycle callbacks.
3. Start `FileManager`, making the periodic worker ready.
4. Start `PeerManager`, allowing connections only after registration.
5. Run the CLI on the main thread.
6. On exit, stop `PeerManager` first so new callbacks cease, then stop
   `FileManager`. No goodbye message is sent.

Constructors must not acquire long-lived resources: sockets and workers belong
in `start()`. Once a component is successfully constructed, the coordinator
will call its idempotent `stop()` during cleanup even if handler registration,
`FileManager.start()`, or `PeerManager.start()` raises. Thus a component whose
`start()` opened some resources and then failed must remember and release those
partial resources in `stop()`. Cleanup is nested so `FileManager.stop()` is
still attempted if `PeerManager.stop()` raises. If `FileManager` construction
fails after `PeerManager` was constructed, `PeerManager.stop()` is still
called. A constructor that raises is responsible for cleaning up anything it
acquired before raising, which is another reason constructors must remain
resource-free.

The coordinator uses only the public interfaces above. Imports of teammate
modules are delayed so Member 1's protocol, logger, and CLI remain independently
importable before integration.

## Running

Python 3.10 or later is recommended; only the standard library is used.
After Members 2 and 3 supply their modules:

```text
cd cs158a/pa2
python agent.py --node-id node-A --tcp-port 5001
```

Optional paths support isolated node directories:

```text
python agent.py --node-id node-B --tcp-port 5002 --shared-dir node-B/shared --downloads-dir node-B/downloads --log-file node-B/log.txt
```

CLI commands are `help`, `peers`, `files`, `shared`,
`download <peer_id> <filename>`, and `quit`. Use quotes around names containing
spaces. The download acknowledgement says only that the asynchronous request
was submitted.

For the required demonstration, run at least five distinct node IDs and TCP
ports with separate shared/download/log paths. Use a local router or hotspot if
campus Wi-Fi blocks broadcast. Preserve real per-node `log.txt` files and real
terminal output or screenshots; none are included yet because integration and
the demonstration have not occurred.

## Decisions requiring group approval

The code currently implements these proposed choices:

1. JSON and exact message/field names shown above.
2. Four-byte big-endian TCP framing and a 16 MiB JSON payload maximum.
3. Whole-file standard base64 transfer with no chunking.
4. Unique, CLI-supplied node IDs and callback identity by node ID.
5. Exact schema validation; no extension fields without a protocol revision.
6. `ConnectionError`-family send failures with Member 2 owning cleanup.
7. Callback registration/timing, snapshot queries, and logging ownership as
   specified above.
8. Peer networking stops before file management during coordinated shutdown.
9. A one-way TCP `HELLO` identifies the initiator before registration or file
   message dispatch.
10. Simultaneous crossing connections retain the connection initiated by the
    lexicographically smaller node ID.

The group still must choose and document:

- Whether peer discovery repeats after startup and, if so, its cadence. This is
  separate from the mandatory 10-second file advertisement.
- Exact `SO_REUSEADDR`/`SO_REUSEPORT` and bind strategy after testing the target
  operating system's five-process UDP broadcast behavior.
- Whether the 16 MiB payload limit is appropriate for the demonstration files.

## Remaining integration and submission work

- Members 2 and 3 implement their modules against the contracts above.
- The group reviews and freezes open protocol/API decisions together.
- Integrate and manually verify discovery, duplicate prevention, short TCP
  reads, file-list replacement, safe transfer, missing-file error, and cleanup.
- Conduct the required real five-node demonstration and add genuine logs and
  terminal output/screenshots.
- Add example files under `shared/`, final member names/contributions, and the
  clickable GitHub repository URL. Contribution claims must match commit
  history.

No automated test suite, CI configuration, invented demo output, or substitute
network/file implementation is included.

## Division of work

We split the project into three parts and agreed on the message format
before anyone started coding.

- An Ngo did the protocol and the shared pieces: `protocol.py` (message
  validation, JSON, TCP length prefix, base64), `interfaces.py`, `logger.py`,
  `cli.py`, `agent.py`, and the team handoff doc so the rest of us knew what
  to plug into.
- Kevin Chau did the networking in `peer.py`: UDP broadcast and listening on
  port 54321, the TCP accept loop and connections, the peer table, and the
  receive threads that notice when a peer leaves.
- Marl Jonson did `file_manager.py`: reading `shared/`, the table of what each
  peer has, sending the file list on connect and every 10 seconds, handling
  file requests and the FILE_ERROR reply, and saving downloads to `downloads/`
  before copying them into `shared/`. Marl also added the sample files in
  `shared/`.

The commit history on GitHub shows who wrote what.

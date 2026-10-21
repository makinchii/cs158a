# CS158A PA2 — P2P Data Sharing Agent

A peer-to-peer file sharing agent. Every node is both a TCP server and a TCP
client; there is no central tracker. Nodes find each other with UDP broadcast,
keep one TCP connection per peer, advertise the files in their `shared/`
directory, and serve those files on request.

Only the Python standard library is used (Python 3.10 or later).

## Files

| File | Responsibility |
|---|---|
| `agent.py` | Entry point: parses arguments, wires the components together, starts and stops them |
| `protocol.py` | Message schemas and validation, JSON encoding, UDP datagrams, TCP length-prefix framing, base64 helpers |
| `peer.py` | Networking: UDP discovery, TCP accept/connect, `HELLO` handshake, peer table, receive and dispatch threads |
| `file_manager.py` | File lists, the peer → files table, file requests, serving files, saving downloads |
| `cli.py` | Interactive commands |
| `logger.py` | Thread-safe `log.txt` writer |
| `interfaces.py` | Typed contracts between the components |
| `shared/` | Sample files this node offers |
| `demo/` | Five-node demo: script, plus each node's `log.txt`, `terminal.txt` and files |

## How to run

From the `pa2` directory:

```text
python agent.py --node-id node-A --tcp-port 5001
```

| Option | Default | Meaning |
|---|---|---|
| `--node-id` | required | Unique name for this node |
| `--tcp-port` | required | TCP port this node listens on (advertised in `PEER_ACK`) |
| `--shared-dir` | `shared` | Directory of files to offer |
| `--downloads-dir` | `downloads` | Where downloads are written first |
| `--log-file` | `log.txt` | This node's log |

To run several nodes on one machine, give each a different node ID, TCP port,
and its own directories:

```text
python agent.py --node-id node-B --tcp-port 5002 --shared-dir node-B/shared --downloads-dir node-B/downloads --log-file node-B/log.txt
```

Commands at the `p2p>` prompt:

| Command | Effect |
|---|---|
| `peers` | List connected peer IDs |
| `files` | Show the table of files each peer advertises |
| `shared` | List this node's shared files |
| `download <peer_id> <file>` | Request a file that peer advertises |
| `help` | Show the commands |
| `quit` | Close all sockets and exit (Ctrl+C also works) |

Discovery uses UDP broadcast, so test on a home router or phone hotspot
rather than campus Wi-Fi, and allow Python through the OS firewall.

## Message format

### Overview

Every message is one UTF-8 JSON object whose `type` field names the message,
so a receiver always knows what it got regardless of arrival order. Each type
has an exact set of fields. A message with a missing field, an extra field, a
wrong JSON type, an unknown `type`, or a type used on the wrong transport is
rejected.

| Type | Transport | Fields | Purpose |
|---|---|---|---|
| `PEER_REQUEST` | UDP | `type`, `node_id: str` | Peer request: "who is out there?" |
| `PEER_ACK` | UDP | `type`, `node_id: str`, `tcp_port: int` | Peer ACK: "I am `node_id`, connect to me on `tcp_port`" |
| `HELLO` | TCP | `type`, `node_id: str` | First TCP frame from the connecting node, identifying it |
| `FILE_LIST` | TCP | `type`, `files: list[str]` | Complete list of filenames the sender offers |
| `FILE_REQUEST` | TCP | `type`, `filename: str` | Data request for one file |
| `FILE_DATA` | TCP | `type`, `filename: str`, `content: str` | File data: the whole file, base64-encoded |
| `FILE_ERROR` | TCP | `type`, `filename: str`, `reason: str` | The requested file cannot be served |

Field limits:

- `node_id`: 1–128 characters, unique among running nodes.
- `tcp_port`: integer 1–65535.
- `filename` and each `files` entry: 1–255 characters. `files` has no duplicates.
- `reason`: 1–1024 characters.
- `content`: strict standard base64.
- Whole JSON payload: at most 16 MiB.

Examples:

```json
{"type":"PEER_REQUEST","node_id":"node-A"}
{"type":"PEER_ACK","node_id":"node-B","tcp_port":5002}
{"type":"HELLO","node_id":"node-A"}
{"type":"FILE_LIST","files":["data.csv","hello.txt","notes.txt"]}
{"type":"FILE_REQUEST","filename":"hello.txt"}
{"type":"FILE_DATA","filename":"hello.txt","content":"aGVsbG8gZnJvbSB0aGUgcGEyIGZpbGUgc2hhcmluZyBkZW1vDQo="}
{"type":"FILE_ERROR","filename":"missing.txt","reason":"File not found"}
```

### Message boundaries

**UDP:** each datagram holds exactly one JSON message, so the datagram is the
boundary. There is no length prefix.

**TCP:** every message is a frame made of a 4-byte unsigned big-endian length
followed by exactly that many bytes of UTF-8 JSON:

```text
+-------------------------------+----------------------------------+
| length: 4 bytes, big-endian   | JSON payload: `length` bytes     |
+-------------------------------+----------------------------------+
```

The length counts only the payload, not the header. A length of 0 or more
than 16 MiB is a protocol error. The receiver loops on `recv` until it has
all 4 header bytes, then loops again until it has all `length` payload bytes,
so short reads and several messages arriving together are both handled.

### File content encoding

File bytes are carried as **base64 text** in the `content` field of
`FILE_DATA`, and the whole file is sent in one message.

We chose base64 because it keeps every TCP message the same shape: one framed
JSON object, read by the same code and checked by the same validator. A raw
byte stream after a header would need a second kind of frame and a separate
read path. The cost is about 33% size overhead and holding the file in memory.
That is fine for the small files this assignment shares. Because of the 16 MiB
payload limit, the largest file that can be sent is a little under 12 MiB.
Larger files get a `FILE_ERROR` reply.

## Protocol behavior

### Discovery (UDP port 54321)

- Every node binds UDP port 54321 (with `SO_REUSEADDR`, so several nodes can
  run on one machine) and listens on it for the node's whole lifetime.
- Each node also has a second UDP socket on a random port that it uses to
  send `PEER_REQUEST`s and receive the ACKs. ACKs are sent back to that port,
  so they reach the node that asked even when several nodes share a machine.
- On startup a node broadcasts `PEER_REQUEST` to port 54321 at
  `255.255.255.255` (the LAN) and `127.255.255.255` (the same machine).
  It rebroadcasts every 5 seconds, so a node whose first broadcast was missed
  is still found.
- A node that receives a `PEER_REQUEST` replies with a `PEER_ACK` carrying
  its node ID and TCP port. A node ignores requests carrying its own node ID.
  Its own broadcasts can still appear in its log as `RECEIVED` because every
  datagram is logged before the node ID is checked.

### Connection setup (TCP)

- The node that receives a `PEER_ACK` (the new node) is the TCP client. It
  connects to the ACK's source IP and `tcp_port` and sends `HELLO` as its
  first frame. The node that sent the ACK is the TCP server.
- The server reads `HELLO` before adding the peer, so both sides know each
  other's node ID. A first frame that is not a valid `HELLO`, or a later
  `HELLO`, closes the connection.
- A node never dials a peer it is already connected to or is already
  connecting to. If two connections for the same pair still cross (for
  example, both nodes rebroadcast at the same moment), both sides keep the
  one opened by the node with the lexicographically smaller node ID and close
  the other.
- Right after a connection is added, each side sends its `FILE_LIST`.

### File lists

- A node sends `FILE_LIST` to every connected peer every 10 seconds, and
  immediately after a new connection.
- Each `FILE_LIST` replaces that peer's previous entry in the peer → files
  table. The `files` command shows this table.
- Only plain filenames are listed. Hidden temporary files used while copying
  (prefix `.incoming-`) are never listed.

### File transfer

1. `download <peer> <file>` checks that the peer advertises the file and that
   it is not already in `shared/`, then sends `FILE_REQUEST`.
2. The serving node replies with `FILE_DATA`. If the file is gone, unreadable,
   or too large, it replies with `FILE_ERROR` instead and the connection stays
   open. Reasons used: `File not found`, `Could not read file`, `File is too
   large to send`.
3. The requester writes the file to `downloads/` first. Only after the write
   is complete does it copy the file into `shared/` (through a temporary name,
   then a rename), so a half-written file is never advertised or served.
4. If a file with the same name already exists in `shared/`, the existing
   copy is kept. `FILE_DATA` that was not requested is ignored.
5. Filenames containing path separators, `..`, or drive letters are rejected,
   so a peer cannot read or write outside `shared/`.

### Leaving the network

There is no goodbye message. A node leaves by closing its sockets. The other
side treats any of these as a disconnection:

- `recv` returns empty bytes before or in the middle of a frame;
- `ConnectionResetError` or another socket error;
- a malformed frame.

It then closes the socket, removes the peer from the peer table, and drops
that peer's file list. A node that comes back is found again through UDP
discovery.

## Threads

| Thread | Job |
|---|---|
| Main | Runs the command line |
| UDP discovery | Listens on 54321, answers requests, receives ACKs, rebroadcasts every 5 s |
| TCP accept | Accepts incoming connections |
| Handshake (one per new connection) | Connects or reads `HELLO`, then registers the peer |
| Receive (one per connection) | Reads frames and queues them |
| Dispatch (one per peer) | Runs the file handlers in order, so sending a large file never stops the socket from being read |
| File advertiser | Sends `FILE_LIST` to all peers every 10 s |

The peer table is guarded by a lock in `PeerManager`. The peer → files table
is guarded by a lock in `FileManager`. Each connection has its own send lock,
so frames from different threads never interleave.

## Log

Each node appends to its log file (`log.txt` by default). Every line has a
timestamp, the event, the peer address, and the message type:

```text
2026-10-20T23:52:49.815-07:00 RECEIVED peer=10.0.0.2:52924 type=PEER_REQUEST
2026-10-20T23:52:49.815-07:00 SENT peer=10.0.0.2:52924 type=PEER_ACK
2026-10-20T23:52:49.818-07:00 RECEIVED peer=10.0.0.2:38611 type=HELLO
2026-10-20T23:52:50.317-07:00 PEER_ADDED peer=10.0.0.2:38611 type=-
```

- `SENT` / `RECEIVED`: every UDP and TCP message, after it was actually sent or
  fully received. File content is never written to the log.
- `PEER_ADDED`: a peer joined the peer table after a successful connection.
- `PEER_REMOVED`: a peer left the peer table after a detected disconnection.

## Division of work

We agreed on the message format before writing code, then split the work
three ways. The commit history on GitHub shows each part.

- **An Ngo**: protocol design and shared code. `protocol.py` (message
  validation, JSON, TCP length prefix, base64), `interfaces.py`, `logger.py`,
  `cli.py`, `agent.py`, and this README.
- **Kevin Chau**: networking in `peer.py`. UDP broadcast and listening on port
  54321, the TCP accept loop and connections, the `HELLO` handshake and
  duplicate-connection handling, the peer table, and the receive threads that
  detect when a peer leaves.
- **Marl Jonson**: `file_manager.py`. Reading `shared/`, the peer → files
  table, sending the file list on connect and every 10 seconds, handling file
  requests and the `FILE_ERROR` reply, and saving downloads to `downloads/`
  before copying them into `shared/`. Marl also added the sample files in
  `shared/`.

## Execution example

Five nodes ran on one machine as separate processes on TCP ports 5001–5005.
Each node has its own directory under [`demo/`](demo/) with its `shared/`
files, `log.txt`, and `terminal.txt` (everything it printed). The commands
were typed into each node by [`demo/run_demo.py`](demo/run_demo.py), which
starts the nodes 2 seconds apart and then runs the steps below. To repeat it:

```text
python demo/run_demo.py
```

**1. Discovery and file lists.** Each node found the other four, and each
`files` table shows what the others advertise. node-A also offers
`old_draft.txt`, which it deletes later to show the error reply.

```text
node-B
p2p> peers
Connected peers: node-A, node-C, node-D, node-E
p2p> files
Available files:
  node-A: data.csv, hello.txt, notes.txt, old_draft.txt
  node-C: todo.txt
  node-D: poem.txt
  node-E: schedule.csv
```

**2. Successful transfer.** node-B downloads `hello.txt` from node-A. The file
lands in `downloads/` and then in `shared/`.

```text
node-B
p2p> download node-A hello.txt
Requested 'hello.txt' from 'node-A'; completion is asynchronous.
[files] downloaded hello.txt from node-A
p2p> shared
Local shared files: hello.txt, recipe.txt
```

**3. Missing file.** node-A deleted `old_draft.txt` after advertising it.
node-C's request gets a `FILE_ERROR` and the connection stays open.

```text
node-C
p2p> download node-A old_draft.txt
Requested 'old_draft.txt' from 'node-A'; completion is asynchronous.
[files] node-A could not send old_draft.txt: File not found
```

**4. File lists propagate.** After the next 10-second file list, node-D sees
that node-B now offers `hello.txt`, and that `old_draft.txt` is gone from
node-A.

```text
node-D
p2p> files
Available files:
  node-A: data.csv, hello.txt, notes.txt
  node-B: hello.txt, recipe.txt
  node-C: todo.txt
  node-E: schedule.csv
```

**5. A node leaves.** node-C quits. The other four drop it from their peer
table, and `todo.txt` disappears from their file tables.

```text
node-C
p2p> quit
```

```text
node-D
p2p> peers
Connected peers: node-A, node-B, node-E
p2p> files
Available files:
  node-A: data.csv, hello.txt, notes.txt
  node-B: hello.txt, recipe.txt
  node-E: schedule.csv
```

### Log excerpts

node-B starting up: it broadcasts a peer request, gets node-A's ACK, connects
to node-A's TCP port 5001, and both sides exchange file lists.

```text
2026-10-21T00:05:31.045-07:00 SENT peer=255.255.255.255:54321 type=PEER_REQUEST
2026-10-21T00:05:31.045-07:00 RECEIVED peer=10.0.0.2:54321 type=PEER_ACK
2026-10-21T00:05:31.047-07:00 SENT peer=10.0.0.2:5001 type=HELLO
2026-10-21T00:05:31.549-07:00 PEER_ADDED peer=10.0.0.2:5001 type=-
2026-10-21T00:05:31.550-07:00 SENT peer=10.0.0.2:5001 type=FILE_LIST
2026-10-21T00:05:31.553-07:00 RECEIVED peer=10.0.0.2:5001 type=FILE_LIST
```

node-B downloading `hello.txt`:

```text
2026-10-21T00:05:54.355-07:00 SENT peer=10.0.0.2:5001 type=FILE_REQUEST
2026-10-21T00:05:54.355-07:00 RECEIVED peer=10.0.0.2:5001 type=FILE_DATA
```

node-A answering node-C's request for the deleted file:

```text
2026-10-21T00:05:55.856-07:00 RECEIVED peer=10.0.0.2:29585 type=FILE_REQUEST
2026-10-21T00:05:55.857-07:00 SENT peer=10.0.0.2:29585 type=FILE_ERROR
```

node-D detecting that node-C (TCP port 5003) left:

```text
2026-10-21T00:06:08.360-07:00 PEER_REMOVED peer=10.0.0.2:5003 type=-
```

Each new node connected only to the nodes already running: across the five
logs, node-A sent 0 `HELLO`s, node-B 1, node-C 2, node-D 3, and node-E 4.
That makes 10 connections for 10 pairs, with no duplicates.

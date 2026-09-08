# PA1: Asynchronous Ring Leader Election

`myleprocess.py` is one node in a unidirectional ring. Each node listens at the
first address in `config.txt`, then connects to the next node at the second
address. It generates a UUID on startup. UUID candidates circulate with
`flag: 0`; smaller candidates are discarded. When the largest candidate returns
to its owner, that node sends a leader announcement with `flag: 1`.

## Protocol shared by every node

Messages are UTF-8, newline-delimited JSON. One message is sent per line:

```json
{"uuid": "123e4567-e89b-42d3-a456-556642440000", "flag": 0}
```

The client sends to the server: a node receives from its accepted server socket
and forwards via its client socket. `flag` is `0` during the election and `1`
for the final leader announcement.

## Configuration

The first line is this node's listening address. The second line is the next
node's listening address. For example:

```text
127.0.0.1,5001
127.0.0.1,5002
```

## Local three-node test

Make three copies of this directory named `node1`, `node2`, and `node3`. Keep
the same `myleprocess.py` in each. Use these configurations:

```text
node1/config.txt          node2/config.txt          node3/config.txt
127.0.0.1,5001            127.0.0.1,5002            127.0.0.1,5003
127.0.0.1,5002            127.0.0.1,5003            127.0.0.1,5001
```

Open one terminal in each directory, then run these commands (the log argument
makes the three required demo logs easy to collect):

```powershell
# terminal in node1
python myleprocess.py --log log1.txt

# terminal in node2
python myleprocess.py --log log2.txt

# terminal in node3
python myleprocess.py --log log3.txt
```

Starting them in any order is fine: each client retries until its next server
is listening. Every terminal should print the same `Leader is <UUID>` line.
Copy the resulting three logs into this `pa1` directory as `log1.txt`,
`log2.txt`, and `log3.txt` before submitting.

### Example local execution

The three-node local test completed with the following shared leader ID:

```text
Node 1: Leader is fba45dcd-ff9a-4475-b89f-571f51016f5a
Node 2: Leader is fba45dcd-ff9a-4475-b89f-571f51016f5a
Node 3: Leader is fba45dcd-ff9a-4475-b89f-571f51016f5a
```

The corresponding detailed traces are included in `log1.txt`, `log2.txt`, and
`log3.txt`.

## Class ring

Put your own IP and port on the first line. Put the next student's IP and port
on the second. The previous student must use your IP and port as their second
line. Start the program with:

```powershell
python myleprocess.py
```

"""
This module implements the peer-to-peer networking logic, where each socket has a reader and each peer has an ordered callback queue, so file
handlers can send replies while the reader keeps receiving more TCP bytes.
"""

import logging
import queue
import select
import socket
import threading
import time

import protocol


DISCOVERY_INTERVAL = 5                   # independent of FileManager's 10s lists
HANDSHAKE_TIMEOUT = 5
DUPLICATE_GRACE = 0.5
POLL_INTERVAL = 0.25
FILE_TYPES = {"FILE_LIST", "FILE_REQUEST", "FILE_DATA", "FILE_ERROR"}


class Peer:
    # Keep one callback queue for a peer, even if a crossing socket replaces it.
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.connection = None
        self.events = queue.Queue()
        self.removed = threading.Event()


class Connection:
    # Store socket-specific state separately from the peer's callback lifecycle.
    def __init__(self, sock, address, outbound):
        self.sock = sock
        self.address = address
        self.outbound = outbound
        self.send_lock = threading.Lock()
        self.peer = None
        self.messages = []              # complete frames arriving before registration
        self.done = threading.Event()
        self.invalid = False


class PeerManager:
    # Validate configuration and initialize passive state; start() opens sockets.
    def __init__(self, node_id, tcp_port, logger):
        protocol.validate_message(
            {"type": "PEER_ACK", "node_id": node_id, "tcp_port": tcp_port},
            transport="udp",
        )
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.logger = logger
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._stop.set()
        self._running = False
        self._shutdowns = 0              # stop() callers currently joining workers
        # A record stays until its disconnect callbacks finish. A None
        # connection means it is being removed, so get_peers() hides it.
        self._peers = {}                # peer IDs -> Peer
        self._connecting = {}           # peer IDs -> completed-attempt Events
        self._callbacks = {}
        self._sockets = set()
        self._threads = set()

    # Add a file-message handler; HELLO belongs only to connection setup.
    def register_handler(self, message_type, handler):
        if message_type not in FILE_TYPES:
            raise ValueError("Register handlers only for FILE_* message types")
        self._add_callback(message_type, handler)

    # Add a notification for a newly registered peer.
    def on_peer_connected(self, callback):
        self._add_callback("connected", callback)

    # Add a notification for a peer whose retained connection has ended.
    def on_peer_disconnected(self, callback):
        self._add_callback("disconnected", callback)

    # Protect the callback registry and preserve multiple registrations.
    def _add_callback(self, kind, callback):
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            self._callbacks.setdefault(kind, []).append(callback)

    # Return a copy so callers can iterate without changing the peer table.
    def get_peers(self):
        with self._lock:
            return [peer_id for peer_id, peer in self._peers.items()
                    if peer.connection is not None]

    # Bind both listeners before broadcasting, and remember partial resources.
    def start(self):
        try:
            with self._lock:
                if self._running:
                    return
                if self._shutdowns or self._threads:
                    raise RuntimeError("Previous networking workers are still stopping")
                self._running = True
                self._stop.clear()
                tcp = self._new_socket(socket.SOCK_STREAM)
                tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                tcp.bind(("", self.tcp_port))
                tcp.listen(64)
                tcp.settimeout(POLL_INTERVAL)
                listener = self._new_socket(socket.SOCK_DGRAM)
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("", protocol.DISCOVERY_PORT))
                listener.setblocking(False)
                # Requests always go TO 54321. A unique reply socket prevents
                # unicast ACKs reaching the wrong same-machine process.
                requester = self._new_socket(socket.SOCK_DGRAM)
                requester.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                requester.bind(("", 0))
                requester.setblocking(False)
                self._start_thread(self._accept_loop, tcp)
                self._start_thread(self._discovery_loop, listener, requester)
                self._broadcast_discovery(requester)
        except Exception:
            self.stop()
            raise

    # Close sockets to unblock workers, then join them; repeated calls are safe.
    def stop(self):
        with self._lock:
            self._running = False
            self._stop.set()
            sockets = list(self._sockets)
            is_worker = threading.current_thread() in self._threads
            if not is_worker:
                self._shutdowns += 1
        for sock in sockets:
            self._close_socket(sock)
        # A callback may call stop(); it must not join itself or its own reader.
        if is_worker:
            return
        while True:
            with self._lock:
                threads = list(self._threads)
            if not threads:
                break
            for thread in threads:
                thread.join()
        with self._lock:
            self._connecting.clear()
            self._peers.clear()
            self._shutdowns -= 1

    # Validate and send one complete frame under the connection's send lock.
    def send_to_peer(self, peer_id, message):
        message = protocol.validate_message(message, transport="tcp")
        if message["type"] == "HELLO":
            raise protocol.ProtocolError("HELLO is only sent during connection setup")
        if message["type"] == "FILE_LIST":
            message["files"] = list(message["files"])
        with self._lock:
            peer = self._peers.get(peer_id)
            connection = peer.connection if peer else None
        if connection is None:
            raise ConnectionError(f"Peer {peer_id!r} is not connected")
        try:
            with connection.send_lock:
                with self._lock:
                    if peer.connection is not connection or not self._running:
                        raise ConnectionError(f"Peer {peer_id!r} is no longer connected")
                protocol.send_message(connection.sock, message)
                self._call(self.logger.log_message, "SENT", connection.address,message["type"])
        except OSError as error:
            self._disconnect(connection)
            # Do not wait for callbacks here: the caller may hold a file lock
            # that a disconnected callback needs. Its notification is queued.
            raise ConnectionError(f"Send to {peer_id!r} failed: {error}") from error

    # Send to a snapshot and continue if any individual peer has disconnected.
    def broadcast_to_peers(self, message):
        message = protocol.validate_message(message, transport="tcp")
        if message["type"] == "HELLO":
            raise protocol.ProtocolError("HELLO cannot be broadcast")
        for peer_id in self.get_peers():
            try:
                self.send_to_peer(peer_id, message)
            except ConnectionError:
                continue

    # Track sockets immediately so startup failures and stop() can close them.
    def _new_socket(self, kind):
        sock = socket.socket(socket.AF_INET, kind)
        with self._lock:
            if not self._running:
                sock.close()
                raise ConnectionError("Peer manager is stopping")
            self._sockets.add(sock)
        return sock

    # Wake blocked reads/writes before closing and removing socket ownership.
    def _close_socket(self, sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass
        with self._lock:
            self._sockets.discard(sock)

    # Track a worker from before it starts until it finishes, including failures.
    def _start_thread(self, target, *args):
        # Always release this worker's tracking entry, even if its target fails.
        def run():
            try:
                target(*args)
            except Exception:
                logging.exception("Networking worker failed")
            finally:
                with self._lock:
                    self._threads.discard(threading.current_thread())
        with self._lock:
            if not self._running:
                return None
            thread = threading.Thread(target=run, daemon=True)
            self._threads.add(thread)
            try:
                thread.start()
            except Exception:
                self._threads.discard(thread)
                raise
            return thread

    # Keep a failed user callback or logger from preventing networking cleanup.
    def _call(self, function, *args):
        try:
            function(*args)
        except Exception:
            logging.exception("Peer callback or logger failed")

    # Announce on the LAN and loopback, always using discovery destination 54321.
    def _broadcast_discovery(self, requester):
        message = {"type": "PEER_REQUEST", "node_id": self.node_id}
        data = protocol.encode_udp(message)
        for ip in ("255.255.255.255", "127.255.255.255"):
            try:
                requester.sendto(data, (ip, protocol.DISCOVERY_PORT))
                self._call(self.logger.log_message, "SENT", f"{ip}:{protocol.DISCOVERY_PORT}", "PEER_REQUEST")
            except OSError:
                continue

    # Listen for late joiners and ACKs; repeat discovery every five seconds.
    def _discovery_loop(self, listener, requester):
        next_request = time.monotonic() + DISCOVERY_INTERVAL
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([listener, requester], [], [], POLL_INTERVAL)
            except (OSError, ValueError):
                break
            for sock in ready:
                try:
                    data, endpoint = sock.recvfrom(65535)
                    message = protocol.decode_udp(data)
                except (OSError, protocol.ProtocolError):
                    continue
                address = f"{endpoint[0]}:{endpoint[1]}"
                self._call(self.logger.log_message, "RECEIVED", address, message["type"])
                if message["node_id"] == self.node_id:
                    continue
                if message["type"] == "PEER_REQUEST":
                    ack = {"type": "PEER_ACK", "node_id": self.node_id,"tcp_port": self.tcp_port}
                    try:
                        sock.sendto(protocol.encode_udp(ack), endpoint)
                        self._call(self.logger.log_message, "SENT", address, "PEER_ACK")
                    except OSError:
                        continue
                elif sock is requester:
                    self._connect_peer(message["node_id"],(endpoint[0], message["tcp_port"]))
            if time.monotonic() >= next_request:
                self._broadcast_discovery(requester)
                next_request = time.monotonic() + DISCOVERY_INTERVAL

    # Only the ACK recipient dials, with one outstanding attempt per peer ID.
    def _connect_peer(self, peer_id, endpoint):
        with self._lock:
            peer = self._peers.get(peer_id)
            if (not self._running or peer_id in self._connecting
                    or (peer is not None and peer.connection is not None)):
                return
            self._connecting[peer_id] = threading.Event()
            try:
                self._start_thread(self._handshake, None, endpoint, peer_id)
            except Exception:
                self._connecting.pop(peer_id).set()
                raise

    # Accept quickly and read each candidate's HELLO on a separate worker.
    def _accept_loop(self, listener):
        while not self._stop.is_set():
            try:
                sock, endpoint = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._lock:
                if not self._running:
                    sock.close()
                    break
                self._sockets.add(sock)
                try:
                    self._start_thread(self._handshake, sock, endpoint)
                except Exception:
                    self._close_socket(sock)
                    raise

    # Send or consume the first HELLO, then choose whether to keep the socket.
    def _handshake(self, sock, endpoint, peer_id=None):
        outbound = peer_id is not None
        retained = False
        address = f"{endpoint[0]}:{endpoint[1]}"
        try:
            if outbound:
                sock = self._new_socket(socket.SOCK_STREAM)
                sock.settimeout(HANDSHAKE_TIMEOUT)
                sock.connect(endpoint)
                protocol.send_message(sock, {"type": "HELLO", "node_id": self.node_id})
                self._call(self.logger.log_message, "SENT", address, "HELLO")
            else:
                sock.settimeout(HANDSHAKE_TIMEOUT)
                message = protocol.receive_message(sock)
                self._call(self.logger.log_message, "RECEIVED", address, message["type"])
                if message["type"] != "HELLO" or message["node_id"] == self.node_id:
                    return
                peer_id = message["node_id"]
            sock.settimeout(None)       # quiet established peers are not dead peers
            retained = self._register_connection(peer_id, sock, address, outbound)
        except (OSError, protocol.ProtocolError):
            pass
        finally:
            if outbound:
                with self._lock:
                    finished = self._connecting.pop(peer_id, None)
                    if finished:
                        finished.set()
            if sock is not None and not retained:
                self._close_socket(sock)

    # Deduplicate sockets but keep one ordered lifecycle for the logical peer.
    def _register_connection(self, peer_id, sock, address, outbound):
        connection = Connection(sock, address, outbound)
        if self._start_thread(self._read_loop, connection) is None:
            return False
        preferred = self.node_id < peer_id
        if outbound != preferred:
            self._stop.wait(DUPLICATE_GRACE)
        # If the preferred outbound attempt is already underway, let it finish
        # before activating an opposite candidate. The reader still drains TCP.
        with self._lock:
            attempt = self._connecting.get(peer_id) if preferred and not outbound else None
        while attempt and not attempt.wait(POLL_INTERVAL):
            # Even if this candidate reaches EOF, its complete initial list may
            # need to be delivered through the preferred connection below.
            if self._stop.is_set():
                break
        # A rejoin must not race an old disconnect callback deleting its files.
        while not self._stop.is_set() and not connection.done.is_set():
            with self._lock:
                peer = self._peers.get(peer_id)
                if peer is None or peer.connection is not None:
                    break
            peer.removed.wait(POLL_INTERVAL)
        with self._lock:
            peer = self._peers.get(peer_id)
            old = peer.connection if peer else None
            reject = (not self._running or connection.done.is_set() or (peer is not None and peer.connection is None))
            if old:
                reject = reject or old.outbound == outbound or outbound != preferred
            if not reject:
                if peer is None:
                    peer = Peer(peer_id)
                    self._start_thread(self._dispatch_loop, peer)
                connection.peer = peer
                peer.connection = connection
                self._peers[peer_id] = peer
                if old is None:
                    peer.events.put(("connected", connection, None))
                for message in connection.messages:
                    peer.events.put(("message", connection, message))
                connection.messages.clear()
        if reject:
            self._close_socket(sock)
            connection.done.wait()
            # An opposite crossing socket may already contain the remote's
            # immediate FILE_LIST. Preserve complete valid frames, not fragments.
            with self._lock:
                peer = self._peers.get(peer_id)
                if (peer and peer.connection and not connection.invalid and self._running
                        and peer.connection.outbound != outbound):
                    for message in connection.messages:
                        peer.events.put(("message", peer.connection, message))
            return False
        if old:
            self._close_socket(old.sock)
        return True

    # Drain TCP independently of handlers that may send large files in reply.
    def _read_loop(self, connection):
        try:
            while not self._stop.is_set():
                message = protocol.receive_message(connection.sock)
                self._call(self.logger.log_message, "RECEIVED", connection.address,message["type"])
                if message["type"] == "HELLO":
                    raise protocol.ProtocolError("HELLO must be the first frame only")
                with self._lock:
                    if connection.peer:
                        connection.peer.events.put(("message", connection, message))
                    else:
                        connection.messages.append(message)
        except protocol.ProtocolError:
            connection.invalid = True
        except OSError:
            pass                       # includes EOF, mid-frame EOF, and reset
        finally:
            self._close_socket(connection.sock)
            connection.done.set()
            self._disconnect(connection)

    # Deliver connected, messages, and disconnected in order, outside table locks.
    def _dispatch_loop(self, peer):
        while True:
            kind, connection, message = peer.events.get()
            with self._lock:
                # A complete FILE_DATA just before EOF is still valid. Drain
                # queued messages before the ordered disconnected notification.
                if kind == "message" and self._peers.get(peer.peer_id) is not peer:
                    continue
                callbacks = list(self._callbacks.get(
                    message["type"] if kind == "message" else kind, []))
            if kind == "connected":
                self._call(self.logger.log_peer_added, connection.address)
            elif kind == "disconnected":
                self._call(self.logger.log_peer_removed, connection.address)
            for callback in callbacks:
                if kind == "message":
                    copy = dict(message)
                    if copy["type"] == "FILE_LIST":
                        copy["files"] = list(copy["files"])
                    self._call(callback, peer.peer_id, copy)
                else:
                    self._call(callback, peer.peer_id)
            if kind == "disconnected":
                with self._lock:
                    if self._peers.get(peer.peer_id) is peer:
                        del self._peers[peer.peer_id]
                    peer.removed.set()
                return

    # Remove exactly the retained socket once; a discarded socket cannot erase it.
    def _disconnect(self, connection):
        with self._lock:
            peer = connection.peer
            if peer is None or peer.connection is not connection:
                return
            peer.connection = None
            peer.events.put(("disconnected", connection, None))
        self._close_socket(connection.sock)

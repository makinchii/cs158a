"""File sharing logic for the PA2 peer-to-peer agent (Member 3)."""

from __future__ import annotations

import threading
from pathlib import Path

# Prefix for files that are still being copied into shared/. They are never
# listed, so a half copied file can't be advertised or served.
TEMP_PREFIX = ".incoming-"

# How often (seconds) we send our file list to all peers.
ADVERTISE_INTERVAL = 10


class FileManager:
    def __init__(self, peers, shared_dir: str, downloads_dir: str) -> None:
        # Only store config here, the worker thread is made in start().
        self._peers = peers
        self._shared = Path(shared_dir)
        self._downloads = Path(downloads_dir)

        # Protects _available and _pending, they are used by the network
        # threads, the CLI thread and the advertise thread.
        self._lock = threading.Lock()
        self._available = {}  # peer_id -> list of filenames
        self._pending = set()  # (peer_id, filename) we asked for

        # Only one download at a time may check + publish into shared/.
        self._publish_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._worker = None

    def start(self) -> None:
        # Worker that sends our file list to everyone every 10 seconds.
        self._stop_event.clear()
        self._worker = threading.Thread(
            target=self._advertise_loop, name="file-advertiser", daemon=True
        )
        self._worker.start()

    def stop(self) -> None:
        # Safe to call many times, or even if start() never ran.
        self._stop_event.set()
        worker = self._worker
        self._worker = None
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=2)

    def _advertise_loop(self) -> None:
        # wait() returns True when stop() was called, so we exit right away.
        while not self._stop_event.wait(ADVERTISE_INTERVAL):
            try:
                self._peers.broadcast_to_peers(self._file_list_message())
            except Exception as exc:  # keep the thread alive
                print(f"[files] advertise failed: {exc}")

    def _file_list_message(self) -> dict:
        return {"type": "FILE_LIST", "files": self.get_local_files()}

    # A name is safe only if it is one plain file name inside the directory.
    @staticmethod
    def _is_safe_name(name) -> bool:
        if not isinstance(name, str) or not name:
            return False
        if name in (".", ".."):
            return False
        if name.startswith(TEMP_PREFIX):
            return False
        # No path separators, drive letters (c:foo) or NUL characters.
        for bad in ("/", "\\", ":", "\0"):
            if bad in name:
                return False
        return Path(name).name == name

    def get_local_files(self) -> list[str]:
        names = []
        try:
            entries = list(self._shared.iterdir())
        except OSError:
            return names
        for entry in entries:
            if self._is_safe_name(entry.name) and entry.is_file():
                names.append(entry.name)
        return sorted(names)

    def get_available_files(self) -> dict[str, list[str]]:
        # Copy everything so the CLI can loop without holding our lock.
        with self._lock:
            return {peer: list(files) for peer, files in self._available.items()}

    def peer_connected(self, peer_id: str) -> None:
        # Tell the new peer what we have right away, not in 10 seconds.
        try:
            self._peers.send_to_peer(peer_id, self._file_list_message())
        except ConnectionError:
            pass  # PeerManager already removes the peer and calls us back

    def peer_disconnected(self, peer_id: str) -> None:
        # Peer left: forget its files and any download we were waiting on.
        with self._lock:
            self._available.pop(peer_id, None)
            self._pending = {p for p in self._pending if p[0] != peer_id}

    def handle_file_list(self, peer_id: str, message: dict) -> None:
        # Replace the old list completely, don't append to it.
        files = list(message["files"])
        with self._lock:
            self._available[peer_id] = files

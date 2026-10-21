"""File sharing logic for the PA2 peer-to-peer agent (Member 3)."""

from __future__ import annotations

import threading
from pathlib import Path

# Prefix for files that are still being copied into shared/. They are never
# listed, so a half copied file can't be advertised or served.
TEMP_PREFIX = ".incoming-"


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
        pass

    def stop(self) -> None:
        pass

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

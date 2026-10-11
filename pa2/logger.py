"""Thread-safe metadata logging for the PA2 agent."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from threading import Lock


class AgentLogger:
    """Append one timestamped line per message or peer lifecycle event."""

    def __init__(self, path: str = "log.txt") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def _write(self, event: str, peer_address: str, message_type: str = "-") -> None:
        if not peer_address:
            raise ValueError("peer_address must not be empty")
        timestamp = datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")
        line = f"{timestamp} {event} peer={peer_address} type={message_type}\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(line)
                stream.flush()

    def log_message(
        self, direction: str, peer_address: str, message_type: str
    ) -> None:
        direction = direction.upper()
        if direction not in {"SENT", "RECEIVED"}:
            raise ValueError("direction must be SENT or RECEIVED")
        if not message_type:
            raise ValueError("message_type must not be empty")
        self._write(direction, peer_address, message_type)

    def log_peer_added(self, peer_address: str) -> None:
        self._write("PEER_ADDED", peer_address)

    def log_peer_removed(self, peer_address: str) -> None:
        self._write("PEER_REMOVED", peer_address)

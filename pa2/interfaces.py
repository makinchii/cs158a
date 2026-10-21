"""Public contracts joining the PA2 components.

``peer.PeerManager`` implements ``PeerManagerContract`` and
``file_manager.FileManager`` implements ``FileManagerContract``. The CLI and
``agent.py`` use only these APIs.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol, TypeAlias


Message: TypeAlias = dict[str, object]
MessageHandler: TypeAlias = Callable[[str, Message], None]
PeerCallback: TypeAlias = Callable[[str], None]


class LoggerContract(Protocol):
    def __init__(self, path: str = "log.txt") -> None: ...

    def log_message(
        self, direction: str, peer_address: str, message_type: str
    ) -> None: ...

    def log_peer_added(self, peer_address: str) -> None: ...

    def log_peer_removed(self, peer_address: str) -> None: ...


class PeerManagerContract(Protocol):
    """Thread-safe public interface of the networking layer (peer.py).

    Handler and lifecycle callbacks run on networking threads and must always
    be invoked outside peer-table locks. Each registration adds one callback;
    each connection/disconnection callback fires exactly once per lifecycle.
    PeerManager consumes the initial TCP HELLO internally; it never dispatches
    HELLO through an application message handler. Construction must not open
    listening sockets or launch workers; resource acquisition belongs in
    start(), with stop() able to clean up a partially completed start().
    """

    def __init__(self, node_id: str, tcp_port: int, logger: LoggerContract) -> None: ...

    def start(self) -> None: ...

    def stop(self) -> None:
        """Release resources after construction, partial start, or full start.

        This method must be idempotent so coordinator cleanup is always safe.
        """
        ...

    def get_peers(self) -> list[str]:
        """Return a new snapshot of connected peer IDs."""
        ...

    def send_to_peer(self, peer_id: str, message: Message) -> None:
        """Send atomically or raise ConnectionError; own failure cleanup."""
        ...

    def broadcast_to_peers(self, message: Message) -> None:
        """Send to a snapshot of peers; failures use send_to_peer semantics."""
        ...

    def register_handler(self, message_type: str, handler: MessageHandler) -> None: ...

    def on_peer_connected(self, callback: PeerCallback) -> None: ...

    def on_peer_disconnected(self, callback: PeerCallback) -> None: ...


class FileManagerContract(Protocol):
    """Thread-safe public interface of the file layer (file_manager.py).

    Construction must not launch workers; resource acquisition belongs in
    start(), with stop() able to clean up a partially completed start().
    """

    def __init__(
        self,
        peers: PeerManagerContract,
        shared_dir: str,
        downloads_dir: str,
    ) -> None: ...

    def start(self) -> None: ...

    def stop(self) -> None:
        """Release resources after construction, partial start, or full start.

        This method must be idempotent so coordinator cleanup is always safe.
        """
        ...

    def get_local_files(self) -> list[str]:
        """Return a new snapshot of filenames currently safe to share."""
        ...

    def get_available_files(self) -> Mapping[str, list[str]]:
        """Return a detached peer-ID to filename-list snapshot."""
        ...

    def request_file(self, peer_id: str, filename: str) -> None:
        """Validate and enqueue/send a request, or raise ValueError/ConnectionError."""
        ...

    def handle_file_list(self, peer_id: str, message: Message) -> None: ...

    def handle_file_request(self, peer_id: str, message: Message) -> None: ...

    def handle_file_data(self, peer_id: str, message: Message) -> None: ...

    def handle_file_error(self, peer_id: str, message: Message) -> None: ...

    def peer_connected(self, peer_id: str) -> None: ...

    def peer_disconnected(self, peer_id: str) -> None: ...

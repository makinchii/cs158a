"""Wire-format helpers for the PA2 peer-to-peer agent.

UDP uses one UTF-8 JSON object per datagram. TCP uses the same JSON payload,
prefixed by a four-byte unsigned big-endian payload length. The TCP initiator
must send ``HELLO`` with its node ID as the first frame; ``PeerManager``
enforces that ordering and consumes HELLO before registering an accepted
connection. This module validates the HELLO schema but has no connection state,
so it cannot determine whether a valid HELLO arrived first.

This module does not own sockets or synchronize concurrent writers;
``PeerManager`` owns both.
"""

from __future__ import annotations

import base64
import binascii
import json
import struct
from typing import Any, Final, Mapping


DISCOVERY_PORT: Final = 54321
MAX_PAYLOAD_SIZE: Final = 16 * 1024 * 1024
MAX_NODE_ID_LENGTH: Final = 128
MAX_FILENAME_LENGTH: Final = 255
MAX_REASON_LENGTH: Final = 1024

UDP_MESSAGE_TYPES: Final = frozenset({"PEER_REQUEST", "PEER_ACK"})
TCP_MESSAGE_TYPES: Final = frozenset(
    {"HELLO", "FILE_LIST", "FILE_REQUEST", "FILE_DATA", "FILE_ERROR"}
)
MESSAGE_TYPES: Final = UDP_MESSAGE_TYPES | TCP_MESSAGE_TYPES

_HEADER = struct.Struct("!I")
_SCHEMAS: Final[dict[str, dict[str, type]]] = {
    "PEER_REQUEST": {"type": str, "node_id": str},
    "PEER_ACK": {"type": str, "node_id": str, "tcp_port": int},
    "HELLO": {"type": str, "node_id": str},
    "FILE_LIST": {"type": str, "files": list},
    "FILE_REQUEST": {"type": str, "filename": str},
    "FILE_DATA": {"type": str, "filename": str, "content": str},
    "FILE_ERROR": {"type": str, "filename": str, "reason": str},
}


class ProtocolError(ValueError):
    """A message is malformed, unsupported, or exceeds protocol limits."""


class PeerDisconnected(ConnectionError):
    """The remote peer closed while a TCP frame was being received."""


def _require_nonempty_text(value: Any, field: str, maximum: int) -> None:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ProtocolError(
            f"{field} must be a nonempty string of at most {maximum} characters"
        )


def validate_message(
    message: Mapping[str, object], *, transport: str | None = None
) -> dict[str, object]:
    """Validate a message and return a plain shallow copy.

    Schemas are intentionally strict: missing and extra fields are rejected.
    ``transport`` may be ``"udp"`` or ``"tcp"`` to enforce transport use.
    Filesystem-safe filename checks remain FileManager's responsibility.
    """
    if not isinstance(message, Mapping):
        raise ProtocolError("message must be a JSON object")

    message_type = message.get("type")
    if not isinstance(message_type, str) or message_type not in MESSAGE_TYPES:
        raise ProtocolError("message has an unknown or missing type")

    if transport not in (None, "udp", "tcp"):
        raise ValueError("transport must be 'udp', 'tcp', or None")
    allowed = UDP_MESSAGE_TYPES if transport == "udp" else TCP_MESSAGE_TYPES
    if transport is not None and message_type not in allowed:
        raise ProtocolError(f"{message_type} is not valid on {transport.upper()}")

    schema = _SCHEMAS[message_type]
    if set(message) != set(schema):
        missing = sorted(set(schema) - set(message))
        extra = sorted(set(message) - set(schema))
        details = []
        if missing:
            details.append(f"missing fields: {', '.join(missing)}")
        if extra:
            details.append(f"unexpected fields: {', '.join(extra)}")
        raise ProtocolError("; ".join(details))

    for field, expected_type in schema.items():
        value = message[field]
        # bool is an int subclass, but is never a valid port.
        if not isinstance(value, expected_type) or (
            expected_type is int and isinstance(value, bool)
        ):
            raise ProtocolError(f"{field} must be {expected_type.__name__}")

    if "node_id" in message:
        _require_nonempty_text(message["node_id"], "node_id", MAX_NODE_ID_LENGTH)
    if "filename" in message:
        _require_nonempty_text(message["filename"], "filename", MAX_FILENAME_LENGTH)
    if message_type == "PEER_ACK" and not 1 <= message["tcp_port"] <= 65535:
        raise ProtocolError("tcp_port must be between 1 and 65535")
    if message_type == "FILE_LIST":
        files = message["files"]
        if any(not isinstance(name, str) for name in files):
            raise ProtocolError("files must contain only strings")
        for name in files:
            _require_nonempty_text(name, "files item", MAX_FILENAME_LENGTH)
        if len(files) != len(set(files)):
            raise ProtocolError("files must not contain duplicates")
    if message_type == "FILE_ERROR":
        _require_nonempty_text(message["reason"], "reason", MAX_REASON_LENGTH)
    if message_type == "FILE_DATA":
        # Validate base64 at the boundary so malformed content is never dispatched.
        decode_file_content(message["content"])

    return dict(message)


def _json_payload(message: Mapping[str, object], transport: str) -> bytes:
    validated = validate_message(message, transport=transport)
    try:
        payload = json.dumps(
            validated, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"message is not JSON serializable: {exc}") from exc
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError(
            f"encoded payload exceeds {MAX_PAYLOAD_SIZE} byte maximum"
        )
    return payload


def _decode_json(payload: bytes, transport: str) -> dict[str, object]:
    if not payload:
        raise ProtocolError("empty payload")
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError(f"payload exceeds {MAX_PAYLOAD_SIZE} byte maximum")
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"invalid UTF-8 JSON payload: {exc}") from exc
    return validate_message(decoded, transport=transport)


def encode_udp(message: Mapping[str, object]) -> bytes:
    """Encode one validated discovery message as a UDP datagram."""
    return _json_payload(message, "udp")


def decode_udp(datagram: bytes) -> dict[str, object]:
    """Decode and validate one complete UDP discovery datagram."""
    if not isinstance(datagram, bytes):
        raise ProtocolError("UDP datagram must be bytes")
    return _decode_json(datagram, "udp")


def encode_message(message: Mapping[str, object]) -> bytes:
    """Return a complete validated TCP frame, including its length header."""
    payload = _json_payload(message, "tcp")
    return _HEADER.pack(len(payload)) + payload


def send_message(sock: Any, message: Mapping[str, object]) -> None:
    """Send one complete TCP frame.

    The caller must hold the connection's per-peer send lock. The caller also
    logs the send only after ``sendall`` succeeds.
    """
    sock.sendall(encode_message(message))


def _recv_exact(sock: Any, count: int, *, frame_part: str) -> bytes:
    chunks: list[bytes] = []
    remaining = count
    while remaining:
        chunk = sock.recv(remaining)
        if chunk == b"":
            if frame_part == "header" and remaining == count:
                raise PeerDisconnected("peer closed the connection")
            raise PeerDisconnected(f"peer closed during TCP {frame_part}")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def receive_message(sock: Any) -> dict[str, object]:
    """Read exactly one framed TCP message, handling arbitrary short reads."""
    header = _recv_exact(sock, _HEADER.size, frame_part="header")
    (payload_size,) = _HEADER.unpack(header)
    if payload_size == 0:
        raise ProtocolError("TCP payload length cannot be zero")
    if payload_size > MAX_PAYLOAD_SIZE:
        raise ProtocolError(
            f"TCP payload length exceeds {MAX_PAYLOAD_SIZE} byte maximum"
        )
    payload = _recv_exact(sock, payload_size, frame_part="payload")
    return _decode_json(payload, "tcp")


def encode_file_content(data: bytes) -> str:
    """Encode raw file bytes as standard padded base64 text."""
    if not isinstance(data, bytes):
        raise ProtocolError("file content must be bytes")
    return base64.b64encode(data).decode("ascii")


def decode_file_content(content: str) -> bytes:
    """Decode strict standard base64 text into raw file bytes."""
    if not isinstance(content, str):
        raise ProtocolError("file content must be a base64 string")
    try:
        return base64.b64decode(content.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
        raise ProtocolError("file content is not valid standard base64") from exc

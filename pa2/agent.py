"""Composition root for the PA2 peer-to-peer agent."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from cli import CLI
from logger import AgentLogger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one PA2 P2P file-sharing node")
    parser.add_argument("--node-id", required=True, help="unique ID for this running node")
    parser.add_argument("--tcp-port", required=True, type=int, help="TCP listen port")
    parser.add_argument("--shared-dir", default="shared", help="published file directory")
    parser.add_argument(
        "--downloads-dir", default="downloads", help="download staging directory"
    )
    parser.add_argument("--log-file", default="log.txt", help="this node's log path")
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if not args.node_id.strip():
        parser.error("--node-id must not be empty")
    if not 1 <= args.tcp_port <= 65535:
        parser.error("--tcp-port must be between 1 and 65535")


def run(args: argparse.Namespace) -> None:
    """Construct, wire, run, and reliably stop the three owned components."""
    # Delayed imports keep Member 1 modules importable and CLI-checkable before
    # the independently owned implementations are added to the repository.
    try:
        from peer import PeerManager
        from file_manager import FileManager
    except ImportError as exc:
        raise RuntimeError(
            "Integration is incomplete: Member 2 must provide peer.py with "
            "PeerManager and Member 3 must provide file_manager.py with FileManager."
        ) from exc

    shared_dir = Path(args.shared_dir)
    downloads_dir = Path(args.downloads_dir)
    shared_dir.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)

    logger = AgentLogger(args.log_file)
    peers = None
    files = None
    try:
        # Constructors must not launch workers or open listening sockets. Once
        # an object is constructed, its idempotent stop() participates in
        # cleanup even if registration or a later start() fails partway.
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
    finally:
        # Stop networking first so no new callbacks race FileManager shutdown.
        # Nested finally ensures FileManager cleanup still runs if peer cleanup
        # itself raises. stop() is required to tolerate a partial start.
        try:
            if peers is not None:
                peers.stop()
        finally:
            if files is not None:
                files.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(parser, args)
    try:
        run(args)
    except RuntimeError as exc:
        parser.exit(1, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

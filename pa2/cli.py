"""Interactive command-line interface for the PA2 agent."""

from __future__ import annotations

import shlex
from collections.abc import Callable

from interfaces import FileManagerContract, PeerManagerContract


HELP_TEXT = """Commands:
  help                         Show this help
  peers                        List connected peer IDs
  files                        List filenames advertised by each peer
  shared                       List local shared filenames
  download <peer_id> <file>    Request one advertised file
  quit                         Stop this node

Quote peer IDs or filenames containing spaces.
"""


class CLI:
    def __init__(
        self,
        peers: PeerManagerContract,
        files: FileManagerContract,
        *,
        input_fn: Callable[[str], str] = input,
        output_fn: Callable[[str], None] = print,
    ) -> None:
        self._peers = peers
        self._files = files
        self._input = input_fn
        self._output = output_fn

    def run(self) -> None:
        """Run on the main thread until quit, EOF, or Ctrl+C."""
        self._output("P2P agent ready. Type 'help' for commands.")
        while True:
            try:
                line = self._input("p2p> ")
            except (EOFError, KeyboardInterrupt):
                self._output("")
                return
            if self.execute(line):
                return

    def execute(self, line: str) -> bool:
        """Execute one command; return True only when shutdown was requested."""
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            self._output(f"Invalid command: {exc}")
            return False
        if not parts:
            return False

        command, *arguments = parts
        command = command.lower()
        if command in {"quit", "exit"}:
            if arguments:
                self._output("Usage: quit")
                return False
            return True
        if command == "help":
            self._output(HELP_TEXT.rstrip())
        elif command == "peers":
            self._no_arguments(arguments, self._show_peers)
        elif command == "files":
            self._no_arguments(arguments, self._show_files)
        elif command == "shared":
            self._no_arguments(arguments, self._show_shared)
        elif command == "download":
            self._download(arguments)
        else:
            self._output(f"Unknown command: {command}. Type 'help' for commands.")
        return False

    def _no_arguments(self, arguments: list[str], action: Callable[[], None]) -> None:
        if arguments:
            self._output("This command takes no arguments.")
        else:
            action()

    def _show_peers(self) -> None:
        peers = sorted(self._peers.get_peers())
        self._output("Connected peers: " + (", ".join(peers) if peers else "none"))

    def _show_files(self) -> None:
        available = self._files.get_available_files()
        if not available:
            self._output("No peer files are currently advertised.")
            return
        lines = ["Available files:"]
        for peer_id in sorted(available):
            names = sorted(available[peer_id])
            lines.append(f"  {peer_id}: {', '.join(names) if names else '(none)'}")
        self._output("\n".join(lines))

    def _show_shared(self) -> None:
        names = sorted(self._files.get_local_files())
        self._output("Local shared files: " + (", ".join(names) if names else "none"))

    def _download(self, arguments: list[str]) -> None:
        if len(arguments) != 2:
            self._output("Usage: download <peer_id> <filename>")
            return
        peer_id, filename = arguments
        try:
            self._files.request_file(peer_id, filename)
        except (ValueError, ConnectionError, OSError) as exc:
            self._output(f"Download request failed: {exc}")
        else:
            self._output(
                f"Requested {filename!r} from {peer_id!r}; completion is asynchronous."
            )

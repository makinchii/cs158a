"""Run the five-node demo on one machine and save each node's output.

Starts node-A .. node-E as separate agent.py processes on TCP ports 5001-5005,
each with its own shared/, downloads/ and log.txt under demo/node-X/. The
script types commands into each node's stdin and saves what the node printed
to demo/node-X/terminal.txt.

Steps shown:
  1. discovery: nodes start 2 s apart and find each other
  2. file lists: every node shows the files the other four advertise
  3. transfer: node-B downloads hello.txt from node-A
  4. error: node-C requests a file node-A deleted after advertising it
  5. propagation: after the next 10 s file list, node-D sees node-B offering hello.txt
  6. leaving: node-C quits and its files disappear from the other tables

Run from the pa2 directory:  python demo/run_demo.py
"""

import os
import shutil
import subprocess
import sys
import threading
import time

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
AGENT = os.path.join(DEMO_DIR, "..", "agent.py")
SAMPLE_SHARED = os.path.join(DEMO_DIR, "..", "shared")
PROMPT = "p2p> "

# Each node starts with different files so the transfers are easy to follow.
NODES = {
    "node-A": None,  # copies the sample files from pa2/shared/
    "node-B": {"recipe.txt": "Pancakes: flour, milk, eggs, butter.\n"},
    "node-C": {"todo.txt": "1. finish PA2\n2. study for the midterm\n"},
    "node-D": {"poem.txt": "Packets fly through the air,\nbroadcast everywhere.\n"},
    "node-E": {"schedule.csv": "day,class\nMon,CS158A\nWed,CS158A\n"},
}

processes = {}
transcripts = {name: [] for name in NODES}


def prepare(name, files):
    """Give a node a fresh directory with its starting shared files."""
    node_dir = os.path.join(DEMO_DIR, name)
    shutil.rmtree(node_dir, ignore_errors=True)
    shared = os.path.join(node_dir, "shared")
    if files is None:
        shutil.copytree(SAMPLE_SHARED, shared)
    else:
        os.makedirs(shared)
        for filename, text in files.items():
            with open(os.path.join(shared, filename), "w", newline="\n") as f:
                f.write(text)
    return node_dir


def read_output(name, process):
    """Collect a node's stdout, dropping the bare prompts input() prints."""
    for line in process.stdout:
        line = line.replace(PROMPT, "")
        if line.strip():
            transcripts[name].append(line)


def start(name):
    node_dir = prepare(name, NODES[name])
    port = 5001 + list(NODES).index(name)
    process = subprocess.Popen(
        [sys.executable, "-u", AGENT, "--node-id", name, "--tcp-port", str(port),
         "--shared-dir", os.path.join(node_dir, "shared"),
         "--downloads-dir", os.path.join(node_dir, "downloads"),
         "--log-file", os.path.join(node_dir, "log.txt")],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    processes[name] = process
    threading.Thread(target=read_output, args=(name, process), daemon=True).start()
    print(f"started {name} on TCP port {port}")


def type_command(name, command, pause=0.5):
    """Type one command into a node, as a user at its terminal would."""
    transcripts[name].append(f"{PROMPT}{command}\n")
    processes[name].stdin.write(command + "\n")
    processes[name].stdin.flush()
    time.sleep(pause)


def main():
    # node-A also offers a file it will delete, to show the FILE_ERROR reply.
    start("node-A")
    with open(os.path.join(DEMO_DIR, "node-A", "shared", "old_draft.txt"), "w") as f:
        f.write("draft\n")
    time.sleep(2)
    for name in list(NODES)[1:]:
        start(name)
        time.sleep(2)

    # 1-2. Discovery and file lists.
    time.sleep(3)
    for name in NODES:
        type_command(name, "peers")
        type_command(name, "files")

    # 3. Successful transfer.
    type_command("node-B", "download node-A hello.txt", pause=1)
    type_command("node-B", "shared")

    # 4. Requested file no longer exists.
    os.remove(os.path.join(DEMO_DIR, "node-A", "shared", "old_draft.txt"))
    type_command("node-C", "download node-A old_draft.txt", pause=1)

    # 5. Wait for the next periodic file list.
    time.sleep(11)
    type_command("node-D", "files")

    # 6. node-C leaves; the others drop it and its files.
    type_command("node-C", "quit", pause=2)
    for name in ("node-A", "node-B", "node-D", "node-E"):
        type_command(name, "peers")
        type_command(name, "files")

    for name in ("node-A", "node-B", "node-D", "node-E"):
        type_command(name, "quit", pause=0)
    for name, process in processes.items():
        process.wait(timeout=10)
    time.sleep(0.5)

    for name in NODES:
        path = os.path.join(DEMO_DIR, name, "terminal.txt")
        with open(path, "w", newline="\n") as f:
            f.writelines(transcripts[name])
        print(f"\n===== {name} =====")
        print("".join(transcripts[name]), end="")


if __name__ == "__main__":
    main()

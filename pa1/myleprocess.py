"""A unidirectional asynchronous-ring leader-election process.

The node receives messages through its server connection and forwards messages
through its client connection.  Messages are newline-delimited JSON objects.
"""

import argparse
import json
import logging
import socket
import threading
import time
import uuid


class Message:
    """The message format as required."""

    # stores UUID and initializes variables
    def __init__(self, uuid_value, flag):
        self.uuid = uuid_value
        self.flag = flag

    # serializes message into JSON
    def to_json(self):
        return json.dumps({"uuid": str(self.uuid), "flag": self.flag})

    # converts JSON back into a Message object
    @classmethod
    def from_json(cls, text):
        data = json.loads(text)
        return cls(uuid.UUID(data["uuid"]), int(data["flag"]))


def read_config(path):
    """Return ((my_ip, my_port), (next_ip, next_port)) from config.txt."""

    with open(path, "r", encoding="utf-8") as file:
        lines = [line.strip() for line in file if line.strip()]

    if len(lines) != 2:
        raise ValueError("config.txt must contain exactly two non-empty lines")

    # helper method to extract IP & port
    def parse_address(line):
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 2:
            raise ValueError("Each config line must have IP-address,port")
        return parts[0], int(parts[1])

    # returns this node's server addr and the next node's addr
    return parse_address(lines[0]), parse_address(lines[1])


class ElectionNode:
    """Represents a node in the LE ring."""

    # initializes state of election, including random UUID.
    def __init__(self, my_address, next_address):
        self.my_address = my_address
        self.next_address = next_address
        self.my_id = uuid.uuid4()
        self.leader_id = None
        self.state = 0
        self.incoming_socket = None
        self.outgoing_socket = None
        self.ready = threading.Event()

    def log(self, message, *args):
        logging.info(message, *args)

    def accept_previous_neighbor(self):
        """Server side: accept exactly one permanent incoming connection."""

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates IPv4 TCP socket
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(self.my_address) # attaches the socket to the node's IP and port

        # starts listening and logs
        server.listen(1)
        self.log("Listening on %s:%s", *self.my_address)

        # blocks until the previous neighbor connects
        self.incoming_socket, peer = server.accept()
        self.log("Previous neighbor connected from %s:%s", *peer)

        # Keeping the accepted socket is sufficient; the listening socket is no
        # longer needed because this assignment uses only one previous neighbor.
        server.close()

    def connect_to_next_neighbor(self):
        """Client side: retry until the next node's server is ready."""

        while True:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates IPv4 TCP socket
            try:
                client.connect(self.next_address) # attempts to connect to the next node
                self.outgoing_socket = client
                self.log("Connected to next neighbor at %s:%s", *self.next_address)
                return
            except OSError:
                client.close()
                self.log("Next neighbor is not ready; retrying in one second")
                time.sleep(1)

    def send_message(self, message):
        """Send one newline-delimited JSON message to the next node."""

        # converts message to JSON and sends it to the next node
        self.outgoing_socket.sendall((message.to_json() + "\n").encode("utf-8"))
        self.log("Sent: uuid=%s, flag=%s", message.uuid, message.flag)

    def receive_log(self, message):
        """Logs details whenever a message arrives"""

        comparison = "greater" if message.uuid > self.my_id else (
            "same" if message.uuid == self.my_id else "less"
        )

        state_description = str(self.state)
        
        if self.state == 1:
            state_description += ", leader_id=" + str(self.leader_id)
        self.log(
            "Received: uuid=%s, flag=%s, %s, %s",
            message.uuid,
            message.flag,
            comparison,
            state_description,
        )

    def announce_leader(self, leader_id):
        """Changes this node to a leader-known state and reports the leader"""
        self.leader_id = leader_id
        self.state = 1
        announcement = "Leader is %s" % leader_id
        print(announcement, flush=True)
        self.log(announcement)

    def handle_message(self, message):
        """Determines if a message will be sent forward."""

        # logs message first
        self.receive_log(message)

        # while leader is still unknown
        if message.flag == 0:
            if message.uuid > self.my_id: # if a larger UUID appears, forward it to the next node
                self.send_message(message)
                return False
            if message.uuid < self.my_id: # if a smaller UUID appears, discard and log it
                self.log("Ignored: uuid=%s, flag=0", message.uuid)
                return False

            # This is this node's own UUID after one complete trip. Announce itself as the leader and send message.
            self.announce_leader(self.my_id)
            self.send_message(Message(self.my_id, 1))
            return False

        # leader has been found
        if message.flag == 1:

            # this node did not know the leader yet
            if self.state == 0:
                self.announce_leader(message.uuid)
                self.send_message(message)

            # the leader message has made a full loop
            elif message.uuid == self.my_id:
                self.log("Leader announcement returned to the leader; terminating.")
                return True

            # should not happen, but discards it in case
            else:
                self.log("Ignored: uuid=%s, flag=1", message.uuid)
            return False

        # in event of wrong flag
        self.log("Ignored: uuid=%s, invalid flag=%s", message.uuid, message.flag)
        return False

    def run(self):
        """Runs the node from startup until it terminates."""
        self.log("My UUID: %s", self.my_id)

        server_thread = threading.Thread(target=self.accept_previous_neighbor)
        client_thread = threading.Thread(target=self.connect_to_next_neighbor)
        server_thread.start()
        client_thread.start()
        server_thread.join()
        client_thread.join()
        self.ready.set()

        # Sends the node's UUID
        self.send_message(Message(self.my_id, 0))

        # loop to handle incoming messages
        with self.incoming_socket.makefile("r", encoding="utf-8") as reader:
            for line in reader:
                if line.strip() and self.handle_message(Message.from_json(line)):
                    return


def main():
    
    # CLI functionality
    parser = argparse.ArgumentParser(description="Asynchronous ring leader-election node")
    parser.add_argument("--config", default="config.txt", help="two-line configuration file")
    parser.add_argument("--log", default="log.txt", help="log-file path")
    args = parser.parse_args()

    # sets up the logger
    logging.basicConfig(
        filename=args.log,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s %(message)s",
    )

    # attempts to start the leader election process
    try:
        my_address, next_address = read_config(args.config)
        ElectionNode(my_address, next_address).run()
    except Exception as error:
        logging.exception("Fatal error")
        print("Fatal error: %s" % error, flush=True)
        raise


if __name__ == "__main__":
    main()

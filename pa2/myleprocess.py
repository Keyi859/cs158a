import argparse
import json
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime


RETRY_SECONDS = 2


@dataclass
class Message:
    candidate: uuid.UUID
    flag: int

    def encode(self):
        data = {
            "uuid": str(self.candidate),
            "flag": self.flag
        }

       
        return (json.dumps(data) + "\n").encode("utf-8")

    @staticmethod
    def decode(data):
        obj = json.loads(data)

        return Message(
            candidate=uuid.UUID(obj["uuid"]),
            flag=int(obj["flag"])
        )


class LeaderElectionProcess:
    def __init__(self, config_file, log_file):
        self.config_file = config_file
        self.log_file = log_file

        self.identifier = uuid.uuid4()
        self.leader_id = None
        self.state = 0
        self.running = True

        self.incoming_socket = None
        self.outgoing_socket = None
        self.listener = None

        self.incoming_ready = threading.Event()
        self.outgoing_ready = threading.Event()

        self.send_lock = threading.Lock()
        self.log_lock = threading.Lock()

        self.startup_error = None

        self.local_address, self.next_address = self.read_config()

        open(self.log_file, "w", encoding="utf-8").close()

        self.log(
            f"Process started: uuid={self.identifier}"
        )

    def read_config(self):
        addresses = []

        with open(self.config_file, "r", encoding="utf-8") as file:
            for line in file:
               
                line = line.split("#", 1)[0].strip()

                if not line:
                    continue

                host, port = line.split(",")

                addresses.append(
                    (host.strip(), int(port.strip()))
                )

        if len(addresses) != 2:
            raise ValueError(
                "config.txt must contain exactly two address lines"
            )

        return addresses[0], addresses[1]

    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        with self.log_lock:
            with open(
                self.log_file,
                "a",
                encoding="utf-8"
            ) as file:
                file.write(f"[{timestamp}] {message}\n")

    def start_server(self):
        host, port = self.local_address

        try:
            self.listener = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            self.listener.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1
            )

            self.listener.bind((host, port))
            self.listener.listen(1)

            self.log(
                f"Listening on {host}:{port}"
            )

            self.incoming_socket, address = (
                self.listener.accept()
            )

            self.log(
                f"Accepted connection from {address}"
            )

            self.incoming_ready.set()

        except OSError as error:
            self.startup_error = error
            self.log(f"Server error: {error}")
            self.incoming_ready.set()

    def start_client(self):
        host, port = self.next_address

        while self.running:
            client = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            try:
                client.settimeout(3)
                client.connect((host, port))
                client.settimeout(None)

                self.outgoing_socket = client

                self.log(
                    f"Connected to neighbor {host}:{port}"
                )

                self.outgoing_ready.set()
                return

            except OSError:
                client.close()

                self.log(
                    f"Cannot connect to {host}:{port}; "
                    f"retrying in {RETRY_SECONDS} seconds"
                )

                time.sleep(RETRY_SECONDS)

    def send_message(self, message):
        if self.outgoing_socket is None:
            raise RuntimeError(
                "Outgoing connection is not ready"
            )

        with self.send_lock:
            self.outgoing_socket.sendall(message.encode())

        self.log(
            f"Sent: uuid={message.candidate}, "
            f"flag={message.flag}"
        )

    def compare_candidate(self, candidate):
        if candidate == self.identifier:
            return "same"

        if candidate > self.identifier:
            return "greater"

        return "less"

    def receive_messages(self):
        reader = self.incoming_socket.makefile(
            "r",
            encoding="utf-8"
        )

        for line in reader:
            if not self.running:
                break

            line = line.strip()

            if not line:
                continue

            try:
                message = Message.decode(line)
            except (json.JSONDecodeError, ValueError) as error:
                self.log(f"Invalid message: {error}")
                continue

            relationship = self.compare_candidate(
                message.candidate
            )

            if self.state == 1:
                state_info = (
                    f"1, leader_id={self.leader_id}"
                )
            else:
                state_info = "0"

            self.log(
                f"Received: uuid={message.candidate}, "
                f"flag={message.flag}, "
                f"{relationship}, state={state_info}"
            )

            if message.flag == 0:
                self.handle_election_message(message)
            elif message.flag == 1:
                self.handle_leader_message(message)
            
            if not self.running:
                break

    def handle_election_message(self, message):
        if message.candidate == self.identifier:
            self.leader_id = self.identifier
            self.state = 1

            self.log(
                f"Leader is decided to {self.leader_id}"
            )

            self.send_message(
                Message(
                    candidate=self.identifier,
                    flag=1
                )
            )

        elif message.candidate > self.identifier:
            self.send_message(message)

        else:
            self.log(
                f"Ignored smaller candidate: "
                f"{message.candidate}"
            )

    def handle_leader_message(self, message):
        self.leader_id = message.candidate
        self.state = 1

        self.log(
            f"Leader recorded: {self.leader_id}"
        )

        if message.candidate != self.identifier:
            self.send_message(message)

        # The leader message has completed one full ring.
        self.running = False

    def run(self):
        server_thread = threading.Thread(
            target=self.start_server,
            daemon=True
        )

        client_thread = threading.Thread(
            target=self.start_client,
            daemon=True
        )

        server_thread.start()
        client_thread.start()

        self.incoming_ready.wait()
        self.outgoing_ready.wait()

        if self.startup_error is not None:
            raise self.startup_error

        # Start the election with this node's UUID.
        self.send_message(
            Message(
                candidate=self.identifier,
                flag=0
            )
        )

        try:
            self.receive_messages()
        finally:
            self.close()

        self.log(
            f"Leader is decided to {self.leader_id}"
        )

        print(f"leader is {self.leader_id}")

    def close(self):
        self.running = False

        for connection in (
            self.incoming_socket,
            self.outgoing_socket,
            self.listener
        ):
            if connection is not None:
                try:
                    connection.close()
                except OSError:
                    pass


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        default="config.txt"
    )

    parser.add_argument(
        "--log",
        default="log.txt"
    )

    args = parser.parse_args()

    process = LeaderElectionProcess(
        args.config,
        args.log
    )

    process.run()


if __name__ == "__main__":
    main()



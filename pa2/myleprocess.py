import argparse
import json
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Message:
    uuid: uuid.UUID
    flag: int

    def to_json(self):
        return json.dumps({
            "uuid": str(self.uuid),
            "flag": self.flag
        })

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return Message(
            uuid=uuid.UUID(obj["uuid"]),
            flag=int(obj["flag"])
        )


class LeaderElectionProcess:
    def __init__(self, config_file, log_file):
        self.config_file = config_file
        self.log_file = log_file

        self.uuid = uuid.uuid4()
        self.leader_id = None
        self.state = 0
        self.running = True

        self.incoming_socket = None
        self.outgoing_socket = None

        self.incoming_ready = threading.Event()
        self.outgoing_ready = threading.Event()

        self.send_lock = threading.Lock()
        self.log_lock = threading.Lock()

        self.local_address, self.next_address = self.read_config()

        open(self.log_file, "w").close()
        self.log(f"Process started: uuid={self.uuid}")

    def read_config(self):
        with open(self.config_file, "r") as file:
            lines = [
                line.strip()
                for line in file
                if line.strip()
            ]

        local_host, local_port = lines[0].split(",")
        next_host, next_port = lines[1].split(",")

        return (
            (local_host.strip(), int(local_port.strip())),
            (next_host.strip(), int(next_port.strip()))
        )

    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        with self.log_lock:
            with open(self.log_file, "a") as file:
                file.write(f"[{timestamp}] {message}\n")

    def start_server(self):
        host, port = self.local_address

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(1)

        self.log(f"Server listening on {host}:{port}")

        self.incoming_socket, address = server.accept()
        self.log(f"Accepted connection from {address}")

        self.incoming_ready.set()

    def start_client(self):
        host, port = self.next_address

        while self.running:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((host, port))
                self.outgoing_socket = client

                self.log(f"Connected to {host}:{port}")
                self.outgoing_ready.set()
                return

            except OSError:
                self.log(f"Cannot connect to {host}:{port}; retrying")
                time.sleep(1)

    def send_message(self, message):
        data = (message.to_json() + "\n").encode("utf-8")

        with self.send_lock:
            self.outgoing_socket.sendall(data)

        self.log(
            f"Sent: uuid={message.uuid}, flag={message.flag}"
        )

    def receive_messages(self):
        reader = self.incoming_socket.makefile(
            "r",
            encoding="utf-8"
        )

        while self.running:
            line = reader.readline()

            if not line:
                break

            message = Message.from_json(line.strip())

            if message.uuid > self.uuid:
                comparison = "greater"
            elif message.uuid < self.uuid:
                comparison = "less"
            else:
                comparison = "same"

            state_info = str(self.state)

            if self.state == 1:
                state_info += f", leader_id={self.leader_id}"

            self.log(
                f"Received: uuid={message.uuid}, "
                f"flag={message.flag}, "
                f"{comparison}, {state_info}"
            )

            if message.flag == 0:
                self.handle_election_message(message)
            else:
                self.handle_leader_message(message)

    def handle_election_message(self, message):
        if message.uuid == self.uuid:
            self.leader_id = self.uuid
            self.state = 1

            self.log(
                f"Leader is decided to {self.leader_id}."
            )

            self.send_message(
                Message(uuid=self.leader_id, flag=1)
            )

        elif message.uuid > self.uuid:
            self.send_message(message)

        else:
            self.log(
                f"Ignored: uuid={message.uuid} is smaller"
            )

    def handle_leader_message(self, message):
        if self.leader_id is None:
            self.leader_id = message.uuid
            self.state = 1

            self.log(
                f"Leader recorded: {self.leader_id}"
            )

        if message.uuid == self.uuid:
            self.running = False
            return

        self.send_message(message)
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

        self.send_message(
            Message(uuid=self.uuid, flag=0)
        )

        self.receive_messages()

        print(f"leader is {self.leader_id}")


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



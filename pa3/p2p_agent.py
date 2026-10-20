import base64
import ipaddress
import json
import re
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4


UDP_PORT = 54321
BUFFER_SIZE = 4096

SHARED_DIR = Path("shared")
DOWNLOADS_DIR = Path("downloads")
LOG_FILE = Path("log.txt")

NODE_ID = str(uuid4())
REQUEST_ID = str(uuid4())

local_tcp_port = None

write_lock = threading.Lock()
peer_lock = threading.Lock()
shutdown_event = threading.Event()

peers = {}
nodes_that_requested_us = set()
connecting_nodes = set()


def write_log(direction, peer, message_type, detail=""):
    timestamp = datetime.now().astimezone().isoformat(
        timespec="seconds"
    )

    line = (
        f"{timestamp} | "
        f"{direction} | "
        f"peer={peer} | "
        f"type={message_type}"
    )

    if detail:
        line += f" | {detail}"

    with write_lock:
        with LOG_FILE.open("a", encoding="utf-8") as output:
            output.write(line + "\n")

        # Important events appear in the Terminal.
        # All sent and received messages are still saved
        # in log.txt.
        if (
            direction == "SYSTEM"
            or message_type in {
                "PEER_ADDED",
                "PEER_REMOVED",
                "FILE_SAVED",
                "FILE_TRANSFER_ERROR"
            }
        ):
            print(line)


def ensure_directories():
    SHARED_DIR.mkdir(exist_ok=True)
    DOWNLOADS_DIR.mkdir(exist_ok=True)


def get_shared_filenames():
    return sorted(
        path.name
        for path in SHARED_DIR.iterdir()
        if path.is_file()
    )


def is_safe_filename(filename):
    if not filename:
        return False

    path = Path(filename)

    return (
        path.name == filename
        and filename not in {".", ".."}
    )


# -------------------------------------------------
# Network address detection
# -------------------------------------------------

def get_local_ip():
    """
    Find the local IP address used for the current network.

    This does not send application data. It only asks the
    operating system which interface it would use.
    """
    test_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    try:
        test_socket.connect(("8.8.8.8", 80))
        return test_socket.getsockname()[0]

    except OSError:
        return None

    finally:
        test_socket.close()


def get_broadcast_address():
    """
    Automatically find the broadcast address of the
    currently active network.

    Examples:

        10.0.0.126/255.255.255.0
        becomes 10.0.0.255

        192.168.1.20/255.255.255.0
        becomes 192.168.1.255
    """
    local_ip = get_local_ip()

    if local_ip is None:
        return "255.255.255.255"

    try:
        ifconfig_output = subprocess.check_output(
            ["ifconfig"],
            text=True,
            stderr=subprocess.DEVNULL
        )

    except (OSError, subprocess.SubprocessError):
        return "255.255.255.255"

    for line in ifconfig_output.splitlines():
        if f"inet {local_ip} " not in line:
            continue

        broadcast_match = re.search(
            r"\bbroadcast\s+"
            r"(\d+\.\d+\.\d+\.\d+)",
            line
        )

        if broadcast_match:
            return broadcast_match.group(1)

        netmask_match = re.search(
            r"\bnetmask\s+(\S+)",
            line
        )

        if not netmask_match:
            continue

        netmask = netmask_match.group(1)

        try:
            if netmask.startswith("0x"):
                netmask_number = int(netmask, 16)

                netmask = socket.inet_ntoa(
                    struct.pack(
                        "!I",
                        netmask_number
                    )
                )

            network = ipaddress.ip_network(
                f"{local_ip}/{netmask}",
                strict=False
            )

            return str(network.broadcast_address)

        except ValueError:
            continue

    return "255.255.255.255"


# -------------------------------------------------
# TCP message framing
# -------------------------------------------------

def send_message(connection, message, peer_name):
    """
    TCP format:

        [4-byte message length][JSON message bytes]

    The length prefix allows the receiver to know
    where one TCP message ends.
    """
    encoded_message = json.dumps(
        message
    ).encode("utf-8")

    length_prefix = struct.pack(
        "!I",
        len(encoded_message)
    )

    connection.sendall(
        length_prefix + encoded_message
    )

    write_log(
        "SENT",
        peer_name,
        message.get("type", "UNKNOWN")
    )


def receive_exact(connection, number_of_bytes):
    """
    Receive exactly number_of_bytes.

    A single recv() may return fewer bytes, so this
    function keeps reading until the full message arrives.

    None means that the peer closed the connection.
    """
    data = b""

    while len(data) < number_of_bytes:
        chunk = connection.recv(
            number_of_bytes - len(data)
        )

        if not chunk:
            return None

        data += chunk

    return data


def receive_message(connection, peer_name):
    length_data = receive_exact(
        connection,
        4
    )

    if length_data is None:
        return None

    message_length = struct.unpack(
        "!I",
        length_data
    )[0]

    if message_length <= 0:
        return None

    message_data = receive_exact(
        connection,
        message_length
    )

    if message_data is None:
        return None

    message = json.loads(
        message_data.decode("utf-8")
    )

    write_log(
        "RECEIVED",
        peer_name,
        message.get("type", "UNKNOWN")
    )

    return message


# -------------------------------------------------
# Peer table
# -------------------------------------------------

def add_peer(
    remote_node_id,
    connection,
    remote_ip,
    remote_tcp_port
):
    with peer_lock:
        if remote_node_id in peers:
            return False

        peers[remote_node_id] = {
            "socket": connection,
            "address": remote_ip,
            "tcp_port": remote_tcp_port,
            "files": set()
        }

    write_log(
        "SYSTEM",
        f"{remote_ip}:{remote_tcp_port}",
        "PEER_ADDED",
        f"node_id={remote_node_id}"
    )

    return True


def remove_peer(remote_node_id, connection):
    if remote_node_id is None:
        return

    with peer_lock:
        peer = peers.get(remote_node_id)

        if peer is None:
            return

        if peer["socket"] is not connection:
            return

        remote_ip = peer["address"]
        remote_tcp_port = peer["tcp_port"]

        del peers[remote_node_id]

    write_log(
        "SYSTEM",
        f"{remote_ip}:{remote_tcp_port}",
        "PEER_REMOVED",
        "connection closed; file list dropped"
    )


def get_peer_address(remote_node_id):
    with peer_lock:
        peer = peers.get(remote_node_id)

        if peer is None:
            return remote_node_id

        return (
            f"{peer['address']}:"
            f"{peer['tcp_port']}"
        )


# -------------------------------------------------
# File list
# -------------------------------------------------

def send_file_list(remote_node_id, connection):
    message = {
        "type": "file_list",
        "filenames": get_shared_filenames()
    }

    peer_address = get_peer_address(
        remote_node_id
    )

    try:
        send_message(
            connection,
            message,
            peer_address
        )

    except OSError:
        remove_peer(
            remote_node_id,
            connection
        )


def periodic_file_list_sender():
    while not shutdown_event.wait(10):
        with peer_lock:
            current_peers = list(
                peers.items()
            )

        for remote_node_id, peer in current_peers:
            send_file_list(
                remote_node_id,
                peer["socket"]
            )


def handle_file_list(
    remote_node_id,
    filenames,
    peer_address
):
    with peer_lock:
        if remote_node_id in peers:
            peers[remote_node_id]["files"] = set(
                filenames
            )

    write_log(
        "SYSTEM",
        peer_address,
        "FILE_TABLE_UPDATED",
        f"filenames={filenames}"
    )


# -------------------------------------------------
# File transfer
# -------------------------------------------------

def send_file_to_peer(
    connection,
    peer_address,
    filename
):
    file_path = SHARED_DIR / filename

    if not file_path.is_file():
        error_message = {
            "type": "file_data",
            "status": "error",
            "filename": filename,
            "error": "file not found"
        }

        send_message(
            connection,
            error_message,
            peer_address
        )

        return

    file_bytes = file_path.read_bytes()

    encoded_data = base64.b64encode(
        file_bytes
    ).decode("ascii")

    message = {
        "type": "file_data",
        "status": "ok",
        "filename": filename,
        "data": encoded_data
    }

    send_message(
        connection,
        message,
        peer_address
    )


def save_received_file(filename, encoded_data):
    if not is_safe_filename(filename):
        write_log(
            "SYSTEM",
            "-",
            "FILE_TRANSFER_ERROR",
            "unsafe filename"
        )
        return

    try:
        file_bytes = base64.b64decode(
            encoded_data.encode("ascii")
        )

        download_path = DOWNLOADS_DIR / filename
        shared_path = SHARED_DIR / filename

        # First save the complete file to downloads/.
        download_path.write_bytes(
            file_bytes
        )

        # Only copy to shared/ after the transfer
        # has completed successfully.
        shutil.copy2(
            download_path,
            shared_path
        )

        write_log(
            "SYSTEM",
            "-",
            "FILE_SAVED",
            f"downloads/{filename} and "
            f"shared/{filename}"
        )

        print(
            f"Downloaded {filename} successfully."
        )

    except (ValueError, OSError) as error:
        write_log(
            "SYSTEM",
            "-",
            "FILE_TRANSFER_ERROR",
            str(error)
        )


def handle_file_data(message, peer_address):
    filename = message.get("filename")
    status = message.get("status")

    if status == "error":
        error_message = message.get(
            "error",
            "unknown file error"
        )

        write_log(
            "SYSTEM",
            peer_address,
            "FILE_TRANSFER_ERROR",
            error_message
        )

        print(
            f"File transfer failed: {error_message}"
        )

        return

    if status != "ok":
        return

    encoded_data = message.get(
        "data",
        ""
    )

    save_received_file(
        filename,
        encoded_data
    )


# -------------------------------------------------
# TCP connections
# -------------------------------------------------

def handle_connection(connection, remote_address):
    remote_node_id = None
    remote_tcp_port = None

    initial_peer_address = (
        f"{remote_address[0]}:"
        f"{remote_address[1]}"
    )

    peer_address = initial_peer_address

    try:
        hello_message = {
            "type": "hello",
            "node_id": NODE_ID,
            "tcp_port": local_tcp_port
        }

        send_message(
            connection,
            hello_message,
            initial_peer_address
        )

        first_message = receive_message(
            connection,
            initial_peer_address
        )

        if first_message is None:
            return

        if first_message.get("type") != "hello":
            write_log(
                "SYSTEM",
                initial_peer_address,
                "PROTOCOL_ERROR",
                "first message was not HELLO"
            )
            return

        remote_node_id = first_message.get(
            "node_id"
        )

        remote_tcp_port = first_message.get(
            "tcp_port"
        )

        peer_address = (
            f"{remote_address[0]}:"
            f"{remote_tcp_port}"
        )

        if remote_node_id == NODE_ID:
            return

        added = add_peer(
            remote_node_id,
            connection,
            remote_address[0],
            remote_tcp_port
        )

        if not added:
            write_log(
                "SYSTEM",
                peer_address,
                "DUPLICATE_CONNECTION",
                "connection ignored"
            )
            return

        # Send the first file list immediately.
        send_file_list(
            remote_node_id,
            connection
        )

        while not shutdown_event.is_set():
            message = receive_message(
                connection,
                peer_address
            )

            if message is None:
                break

            message_type = message.get(
                "type"
            )

            if message_type == "file_list":
                handle_file_list(
                    remote_node_id,
                    message.get(
                        "filenames",
                        []
                    ),
                    peer_address
                )

            elif message_type == "data_request":
                filename = message.get(
                    "filename"
                )

                if not is_safe_filename(filename):
                    error_message = {
                        "type": "file_data",
                        "status": "error",
                        "filename": filename,
                        "error": "invalid filename"
                    }

                    send_message(
                        connection,
                        error_message,
                        peer_address
                    )

                    continue

                send_file_to_peer(
                    connection,
                    peer_address,
                    filename
                )

            elif message_type == "file_data":
                handle_file_data(
                    message,
                    peer_address
                )

            else:
                write_log(
                    "SYSTEM",
                    peer_address,
                    "UNKNOWN_MESSAGE",
                    f"type={message_type}"
                )

    except ConnectionResetError:
        write_log(
            "SYSTEM",
            peer_address,
            "CONNECTION_RESET"
        )

    except (BrokenPipeError, OSError) as error:
        write_log(
            "SYSTEM",
            peer_address,
            "CONNECTION_ERROR",
            str(error)
        )

    finally:
        remove_peer(
            remote_node_id,
            connection
        )

        try:
            connection.close()
        except OSError:
            pass


def tcp_accept_loop(server_socket):
    write_log(
        "SYSTEM",
        "-",
        "TCP_LISTEN",
        f"port={local_tcp_port}"
    )

    while not shutdown_event.is_set():
        try:
            connection, remote_address = (
                server_socket.accept()
            )

            thread = threading.Thread(
                target=handle_connection,
                args=(
                    connection,
                    remote_address
                ),
                daemon=True
            )

            thread.start()

        except OSError:
            break


def connect_to_peer(
    remote_ip,
    remote_tcp_port,
    remote_node_id
):
    with peer_lock:
        if remote_node_id in peers:
            return

        if remote_node_id in connecting_nodes:
            return

        connecting_nodes.add(
            remote_node_id
        )

    peer_address = (
        f"{remote_ip}:{remote_tcp_port}"
    )

    try:
        connection = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        connection.settimeout(5)

        connection.connect(
            (remote_ip, remote_tcp_port)
        )

        connection.settimeout(None)

        write_log(
            "SYSTEM",
            peer_address,
            "TCP_CONNECTED"
        )

        handle_connection(
            connection,
            (remote_ip, remote_tcp_port)
        )

    except OSError as error:
        write_log(
            "SYSTEM",
            peer_address,
            "TCP_CONNECT_ERROR",
            str(error)
        )

    finally:
        with peer_lock:
            connecting_nodes.discard(
                remote_node_id
            )


# -------------------------------------------------
# UDP discovery
# -------------------------------------------------

def create_udp_socket():
    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    udp_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_BROADCAST,
        1
    )

    udp_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    if hasattr(socket, "SO_REUSEPORT"):
        udp_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEPORT,
            1
        )

    udp_socket.bind(
        ("", UDP_PORT)
    )

    return udp_socket


def send_peer_request(udp_socket):
    broadcast_address = get_broadcast_address()

    message = {
        "type": "peer_request",
        "node_id": NODE_ID,
        "tcp_port": local_tcp_port,
        "request_id": REQUEST_ID
    }

    udp_socket.sendto(
        json.dumps(message).encode("utf-8"),
        (
            broadcast_address,
            UDP_PORT
        )
    )

    write_log(
        "SENT",
        "broadcast",
        "PEER_REQUEST",
        f"tcp_port={local_tcp_port}, "
        f"broadcast={broadcast_address}"
    )


def send_peer_ack(
    udp_socket,
    remote_ip,
    remote_request_id
):
    message = {
        "type": "peer_ack",
        "node_id": NODE_ID,
        "tcp_port": local_tcp_port,
        "ack_for": remote_request_id
    }

    udp_socket.sendto(
        json.dumps(message).encode("utf-8"),
        (
            remote_ip,
            UDP_PORT
        )
    )

    write_log(
        "SENT",
        f"{remote_ip}:{UDP_PORT}",
        "PEER_ACK",
        f"tcp_port={local_tcp_port}"
    )


def udp_listener(udp_socket):
    while not shutdown_event.is_set():
        try:
            data, sender_address = udp_socket.recvfrom(
                BUFFER_SIZE
            )

            message = json.loads(
                data.decode("utf-8")
            )

            message_type = message.get(
                "type"
            )

            remote_node_id = message.get(
                "node_id"
            )

            remote_ip = sender_address[0]

            if remote_node_id == NODE_ID:
                continue

            if message_type == "peer_request":
                remote_tcp_port = message.get(
                    "tcp_port"
                )

                remote_request_id = message.get(
                    "request_id"
                )

                with peer_lock:
                    nodes_that_requested_us.add(
                        remote_node_id
                    )

                write_log(
                    "RECEIVED",
                    f"{remote_ip}:{UDP_PORT}",
                    "PEER_REQUEST",
                    f"remote_tcp_port={remote_tcp_port}"
                )

                send_peer_ack(
                    udp_socket,
                    remote_ip,
                    remote_request_id
                )

            elif message_type == "peer_ack":
                remote_tcp_port = message.get(
                    "tcp_port"
                )

                ack_for = message.get(
                    "ack_for"
                )

                write_log(
                    "RECEIVED",
                    f"{remote_ip}:{UDP_PORT}",
                    "PEER_ACK",
                    f"remote_tcp_port={remote_tcp_port}"
                )

                if ack_for != REQUEST_ID:
                    continue

                with peer_lock:
                    simultaneous_request = (
                        remote_node_id
                        in nodes_that_requested_us
                    )

                # If both nodes requested each other,
                # only the node with the smaller UUID
                # establishes the TCP connection.
                if (
                    simultaneous_request
                    and NODE_ID > remote_node_id
                ):
                    write_log(
                        "SYSTEM",
                        f"{remote_ip}:{remote_tcp_port}",
                        "TCP_CONNECT_SKIPPED",
                        "waiting for the other node"
                    )

                    continue

                thread = threading.Thread(
                    target=connect_to_peer,
                    args=(
                        remote_ip,
                        remote_tcp_port,
                        remote_node_id
                    ),
                    daemon=True
                )

                thread.start()

        except json.JSONDecodeError:
            write_log(
                "SYSTEM",
                "-",
                "UDP_ERROR",
                "invalid JSON"
            )

        except OSError as error:
            if not shutdown_event.is_set():
                write_log(
                    "SYSTEM",
                    "-",
                    "UDP_ERROR",
                    str(error)
                )

            break


# -------------------------------------------------
# User commands
# -------------------------------------------------

def print_peer_table():
    with peer_lock:
        if not peers:
            print("No connected peers.")
            return

        print("\nConnected peers:")

        for remote_node_id, peer in peers.items():
            print(
                f"- node_id={remote_node_id}"
            )

            print(
                f"  address={peer['address']}:"
                f"{peer['tcp_port']}"
            )

            print(
                f"  files={sorted(peer['files'])}"
            )


def request_file(filename):
    if not is_safe_filename(filename):
        print("Invalid filename.")
        return

    if filename in get_shared_filenames():
        print(
            f"You already have {filename} "
            f"in shared/."
        )
        return

    selected_peer_id = None
    selected_peer = None

    with peer_lock:
        for remote_node_id, peer in peers.items():
            if filename in peer["files"]:
                selected_peer_id = remote_node_id
                selected_peer = peer
                break

    if selected_peer is None:
        print(
            f"No connected peer advertises "
            f"{filename}."
        )
        return

    message = {
        "type": "data_request",
        "filename": filename
    }

    peer_address = (
        f"{selected_peer['address']}:"
        f"{selected_peer['tcp_port']}"
    )

    try:
        send_message(
            selected_peer["socket"],
            message,
            peer_address
        )

        print(
            f"Requested {filename} from "
            f"{peer_address}."
        )

    except OSError as error:
        print(
            f"Could not request file: {error}"
        )


def command_loop():
    print("\nCommands:")
    print("  peers")
    print("  get FILENAME")
    print("  help")
    print("  quit")

    while not shutdown_event.is_set():
        try:
            command = input(
                "\npa3> "
            ).strip()

        except EOFError:
            break

        if command == "peers":
            print_peer_table()

        elif command.startswith("get "):
            filename = command[4:].strip()
            request_file(filename)

        elif command == "help":
            print(
                "peers       Show connected peers "
                "and files"
            )

            print(
                "get NAME    Download a file"
            )

            print(
                "quit        Stop the agent"
            )

        elif command == "quit":
            shutdown_event.set()
            break

        elif command:
            print(
                "Unknown command. Type help."
            )


# -------------------------------------------------
# Main
# -------------------------------------------------

def main():
    global local_tcp_port

    if len(sys.argv) != 2:
        print(
            "Usage: python3 p2p_agent.py TCP_PORT"
        )
        return

    try:
        local_tcp_port = int(
            sys.argv[1]
        )

        if not 1 <= local_tcp_port <= 65535:
            raise ValueError

    except ValueError:
        print(
            "TCP_PORT must be between 1 and 65535."
        )
        return

    ensure_directories()

    write_log(
        "SYSTEM",
        "-",
        "START",
        f"node_id={NODE_ID}, "
        f"tcp_port={local_tcp_port}"
    )

    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server_socket.bind(
        ("", local_tcp_port)
    )

    server_socket.listen()

    tcp_thread = threading.Thread(
        target=tcp_accept_loop,
        args=(server_socket,),
        daemon=True
    )

    tcp_thread.start()

    file_list_thread = threading.Thread(
        target=periodic_file_list_sender,
        daemon=True
    )

    file_list_thread.start()

    udp_socket = create_udp_socket()

    udp_thread = threading.Thread(
        target=udp_listener,
        args=(udp_socket,),
        daemon=True
    )

    udp_thread.start()

    time.sleep(1)

    send_peer_request(
        udp_socket
    )

    print("PA3 agent is running.")
    print(f"Node ID: {NODE_ID}")
    print(f"TCP port: {local_tcp_port}")
    print(f"UDP port: {UDP_PORT}")
    print(
        f"Broadcast address: "
        f"{get_broadcast_address()}"
    )
    print(
        f"Shared files: "
        f"{get_shared_filenames()}"
    )

    command_thread = threading.Thread(
        target=command_loop,
        daemon=True
    )

    command_thread.start()

    try:
        while not shutdown_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown_event.set()

    finally:
        shutdown_event.set()

        try:
            udp_socket.close()
        except OSError:
            pass

        try:
            server_socket.close()
        except OSError:
            pass

        write_log(
            "SYSTEM",
            "-",
            "STOP",
            "agent stopped"
        )


if __name__ == "__main__":
    main()
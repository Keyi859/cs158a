# PA3 - P2P Data Sharing Agent

## 1. Overview

This project implements a peer-to-peer data sharing agent.

Every node acts as both:

- A UDP discovery listener
- A UDP discovery sender
- A TCP server
- A TCP client
- A file-list sender
- A file server
- A file downloader

There is no central directory, tracker, or server.

This project was completed individually by Keyi Tan. Three physical computers were used as separate network nodes for testing. The computers were used only as test nodes, not as group members.

Each node contains:

```text
shared/
    Files offered to other peers

downloads/
    Files downloaded from other peers

log.txt
    Log of network messages and peer-table changes

2. Technologies
The agent uses:
- Python 3
- UDP broadcast
- TCP sockets
- JSON messages
- A 4-byte TCP length prefix
- Base64 encoding for file data
- Python threads
- UTF-8 text encoding
UDP peer discovery uses port:
54321

Each node uses a different TCP port.

Example:
MacBook Pro: TCP port 5001
iMac: TCP port 5002
MacBook Air: TCP port 5003

All computers must be connected to the same local Wi-Fi network or hotspot.

The program automatically calculates the directed broadcast address for the active network interface. In the test described below, the broadcast address was:
10.0.0.255

Some public or campus Wi-Fi networks may block UDP broadcast traffic.
3. Directory Structure
pa2/
├── p2p_agent.py
├── README.md
├── log.txt
├── shared/
└── downloads/

The shared/ directory contains files offered to peers.

The downloads/ directory contains files downloaded from peers.

A downloaded file is first written completely to downloads/. Only after the transfer completes successfully is it copied into shared/.
4. How to Run
From the pa2 directory, run:
python3 p2p_agent.py TCP_PORT

On the MacBook Pro:
cd ~/Desktop/pa2
python3 p2p_agent.py 5001

On the iMac:
cd ~/Desktop/pa2
python3 p2p_agent.py 5002

On the MacBook Air:
cd ~/Desktop/pa2
python3 p2p_agent.py 5003

The UDP discovery port is always:
54321

The TCP port is different for every node.

The program displays these commands:
peers
get FILENAME
help
quit

peers
Displays connected peers and their advertised files.
get FILENAME
Requests a file from a connected peer.

Example:
get macair_note.txt

The file is first saved to:
downloads/macair_note.txt

After the transfer completes, it is copied to:
shared/macair_note.txt

help
Displays the available commands.
quit
Stops the agent and closes its sockets.
5. Message Boundary Format
TCP is a byte stream, so the program uses a 4-byte length prefix.

Every TCP message has this format:
[4-byte message length][JSON message bytes]

The length is:
- An unsigned 32-bit integer
- Stored in network byte order
- Stored in big-endian order
The Python implementation is:
struct.pack("!I", len(encoded_message))


The receiver first reads exactly 4 bytes to determine the message length.

The receiver then repeatedly calls recv() until it has received the complete JSON message.

Receiving fewer bytes than requested does not mean that the peer disconnected.

An empty read means that the peer closed the connection.

The program also handles ConnectionResetError as a disconnection.
6. Message Encoding
All TCP messages are encoded as UTF-8 JSON.

Every message contains a type field.

The receiver uses the type field to identify the message.

File content is encoded using Base64 and placed inside a JSON message.

Base64 allows arbitrary binary file data to be safely carried inside JSON.
7. UDP Message Formats
7.1 Peer Request
A peer request is sent using UDP broadcast.

The destination UDP port is:
54321

Example:
{
  "type": "peer_request",
  "node_id": "client-node-uuid",
  "tcp_port": 5001,
  "request_id": "request-uuid"
}

Fields:
Field	Description
type	Always peer_request
node_id	UUID identifying the sender
tcp_port	TCP port where the sender accepts connections
request_id	UUID identifying this discovery request


A node ignores a peer request if the node_id matches its own node ID.
7.2 Peer ACK
A node sends a peer ACK after receiving a peer request.

Example:
{
  "type": "peer_ack",
  "node_id": "server-node-uuid",
  "tcp_port": 5002,
  "ack_for": "request-uuid"
}

Fields:
Field	Description
type	Always peer_ack
node_id	UUID identifying the responding node
tcp_port	TCP port where the responding node accepts connections
ack_for	Request ID from the original peer request


After receiving a valid peer ACK, the requesting node establishes a TCP connection using the advertised IP address and TCP port.

If both nodes request each other at the same time, the node IDs are compared so that only one TCP connection is created.
8. TCP Message Formats
8.1 HELLO
HELLO is the first TCP message exchanged by both sides.

Example:
{
  "type": "hello",
  "node_id": "node-uuid",
  "tcp_port": 5001
}

After both nodes exchange HELLO messages, the connection is added to the peer table.
8.2 File List
A file list is sent immediately after a new TCP connection is established.

It is also sent every 10 seconds.

Example:
{
  "type": "file_list",
  "filenames": [
    "sample.txt",
    "imac_note.txt"
  ]
}

The file list contains filenames only. It does not contain file contents.
8.3 Data Request
Example:
{
  "type": "data_request",
  "filename": "sample.txt"
}

A peer sends this message to request a file from another peer.
8.4 File Data
A successful file response contains:
{
  "type": "file_data",
  "status": "ok",
  "filename": "sample.txt",
  "data": "base64-encoded-file-data"
}

If the file cannot be found, the response contains an error status.
9. File Transfer Behavior
Before sending a file, the agent checks that the requested filename is safe.

The agent rejects unsafe filenames such as filenames containing path traversal characters.

The complete file is read and encoded using Base64.

The receiving node decodes the file and writes it completely to the downloads/ directory.

After the download succeeds, the file is copied into the shared/ directory.

This allows the newly downloaded file to be advertised to other peers.
10. Peer Disconnection Behavior
When a TCP connection closes, the corresponding peer is removed from the peer table.

The peer's advertised file list is also removed.

The program writes a PEER_REMOVED entry to the log.

For example:
PEER_REMOVED | connection closed; file list dropped

This ensures that files from an offline peer are no longer displayed by other nodes.
11. Logging
The program records important events in log.txt.

Examples include:
START
TCP_LISTEN
PEER_REQUEST
PEER_ACK
TCP_CONNECTED
PEER_ADDED
FILE_TABLE_UPDATED
FILE_SAVED
PEER_REMOVED
STOP

Example connection log:
SYSTEM | peer=10.0.0.151:5002 | type=TCP_CONNECTED

Example file-transfer log:
SYSTEM | peer=- | type=FILE_SAVED | downloads/macair_note.txt and shared/macair_note.txt

Example disconnection log:
SYSTEM | peer=10.0.0.248:5003 | type=PEER_REMOVED | connection closed; file list dropped

12. Three-Node Demonstration
The original assignment requests a five-node demonstration. The instructor approved a three-node demonstration for this individual submission.

All computers were connected to the same local network.
Node Configuration
Computer	IP Address	TCP Port
MacBook Pro	10.0.0.126	5001
iMac	10.0.0.151	5002
MacBook Air	10.0.0.248	5003


All nodes used UDP port:
54321

12.1 Node Discovery and TCP Connections
The MacBook Pro node was started on TCP port 5001.

The iMac node was started on TCP port 5002 and connected to the MacBook Pro.

The MacBook Air node was started on TCP port 5003 and connected to the iMac.

The logs showed:
TCP_CONNECTED
PEER_ADDED
FILE_TABLE_UPDATED

The iMac displayed both connected peers:
Connected peers:
- node_id=0f262d50-90ea-479e-87f8-726455c229ce
  address=10.0.0.126:5001

- node_id=278e7309-e53f-4979-87c6-97621266ce97
  address=10.0.0.248:5003

This demonstrated UDP discovery and TCP connection establishment.
12.2 File List Propagation
The MacBook Air advertised the following files:
imac_note.txt
imac_only.txt
imac_temporary.txt
macair_note.txt
sample.txt

The iMac received the file list from the MacBook Air:
FILE_TABLE_UPDATED
filenames=['imac_note.txt', 'imac_only.txt', 'imac_temporary.txt',
'macair_note.txt', 'sample.txt']

This demonstrated that file lists were exchanged and updated between peers.
12.3 Successful File Transfer
The iMac requested a file from the MacBook Air using:
get macair_note.txt

The agent displayed:
Requested macair_note.txt from 10.0.0.248:5003.

The file was transferred successfully:
Downloaded macair_note.txt successfully.

The log recorded:
FILE_SAVED | downloads/macair_note.txt and shared/macair_note.txt

The file was verified in both directories:
downloads/macair_note.txt
shared/macair_note.txt

This demonstrated successful file transfer and file storage.
12.4 Node Leaving the Network
The MacBook Air node was stopped using:
quit

The MacBook Air displayed:
STOP | agent stopped

After the MacBook Air left, the iMac entered:
peers

The iMac then displayed only the MacBook Pro:
Connected peers:
- node_id=0f262d50-90ea-479e-87f8-726455c229ce
  address=10.0.0.126:5001
  files=['imac_note.txt', 'imac_only.txt',
  'imac_temporary.txt', 'sample.txt']

The MacBook Air node and its advertised macair_note.txt file were no longer shown.

This demonstrated that a leaving node is removed from the peer table and its files disappear from the other nodes' file tables.
13. Individual Work
This project was completed individually by:
Keyi Tan

There were no additional group members.

The project was tested using three physical computers:
- The MacBook Pro acted as one P2P node.
- The iMac acted as one P2P node.
- The MacBook Air acted as one P2P node.
The computers were used only as separate network nodes for testing. They were not separate group members.

Keyi Tan implemented and tested:
- UDP peer discovery
- UDP peer acknowledgements
- TCP server
- TCP client
- HELLO exchange
- Peer table management
- File-list exchange
- Periodic file-list updates
- File requests
- Base64 file transfer
- Download and shared-file storage
- Filename safety checking
- Disconnection handling
- Logging
- Network testing and integration
14. Submission Files
The final submission directory contains:
pa2/
├── p2p_agent.py
├── README.md
├── log.txt
├── log_imac.txt
├── log_macair.txt
├── shared/
└── downloads/
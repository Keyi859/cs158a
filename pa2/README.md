# CS158A PA2: Leader Election

## Files

- `myleprocess.py`: Python implementation of the leader election process
- `config.txt`: configuration file
- `log1.txt`: log from node 1
- `log2.txt`: log from node 2
- `log3.txt`: log from node 3

## Algorithm

This program implements leader election in a unidirectional asynchronous ring.

Each process generates a unique UUID using Python's `uuid.uuid4()` method.

Each process initially sends its UUID to the next process in the ring.

When a process receives a message:

- If the received UUID is greater than its own UUID, it forwards the message.
- If the received UUID is smaller than its own UUID, it ignores the message.
- If its own UUID returns to itself, it becomes the leader.
- The leader sends a message with `flag=1` to announce the elected leader.

After receiving the leader announcement, every process stores the leader UUID in its `leader_id` member variable.

## Message Format

Messages are serialized as JSON.

Each message contains:

```json
{
    "uuid": "process-uuid",
    "flag": 0
}
```

The value of `flag` is:

- `0`: leader election is still in progress
- `1`: a leader has been elected

## Configuration

Each node has a separate `config.txt` file.

The first line contains the local server address.
The second line contains the address of the next node.

Node 1 configuration:

```text
127.0.0.1,5001
127.0.0.1,5002
```

Node 2 configuration:

```text
127.0.0.1,5002
127.0.0.1,5003
```

Node 3 configuration:

```text
127.0.0.1,5003
127.0.0.1,5001
```

## Running the Demo

Three processes were executed in three separate terminal windows.

Node 1:

```bash
cd ~/cs158a/pa2/node1
python3 myleprocess.py --log log1.txt
```

Node 2:

```bash
cd ~/cs158a/pa2/node2
python3 myleprocess.py --log log2.txt
```

Node 3:

```bash
cd ~/cs158a/pa2/node3
python3 myleprocess.py --log log3.txt
```

## Execution Result

All three processes selected the same leader:

```text
leader is d3e0fac1-74f9-4394-9a8e-ec83cb342599
leader is d3e0fac1-74f9-4394-9a8e-ec83cb342599
leader is d3e0fac1-74f9-4394-9a8e-ec83cb342599
```

## Example Log

```text
[13:21:10.417] Leader recorded: d3e0fac1-74f9-4394-9a8e-ec83cb342599
[13:21:10.417] Leader is decided to d3e0fac1-74f9-4394-9a8e-ec83cb342599.
[13:21:10.417] Leader recorded: d3e0fac1-74f9-4394-9a8e-ec83cb342599
```

## Termination

The program terminates after all processes learn the same leader UUID.

The leader election satisfies:

1. Termination
2. Uniqueness
3. Agreement

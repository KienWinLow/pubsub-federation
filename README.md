# pubsub-federation
Python pub/sub server and client with federated servers

# Pub/Sub Federation (Python)

A publish/subscribe messaging system built from scratch over TCP sockets: a **server** that can link up with other servers into a federation, and a **client** that publishes and subscribes to topics. Written in Python 3 using only the standard library.

Built for **COMS3200 (Computer Networks I)**, University of Queensland, Semester 1 2026.

## What it does

- Clients connect to a server, subscribe to topics, and publish text messages or files.
- Servers can be linked as **peers**. Messages, subscriptions and unsubscriptions are forwarded across the federation, so a client on one server receives messages published on another.
- **Numeric filters** on subscriptions: only receive messages whose value matches a condition such as `>= 20` (supports `<`, `>`, `<=`, `>=`, `==`, `!=`).
- **File publishing**: binary files are hex-encoded and delivered to subscribers (file messages never match filtered subscriptions).
- **Per-client rate limiting** set by the server operator for a given topic (0 to 3600 seconds).
- Handshake validation: duplicate client IDs are rejected, and peers refuse to connect to themselves, to an existing peer, or if server IDs would clash across the federation.

## Usage

```
python3 pubsubserver.py [--server [host]:port]... [--listenon port] serverid
python3 pubsubclient.py [--topic topic] [host]:port clientid [message]
```

Example: two servers federated together, with one client on each.

```
python3 pubsubserver.py --listenon 4000 serverA
python3 pubsubserver.py --listenon 4001 --server localhost:4000 serverB
python3 pubsubclient.py --topic weather localhost:4000 alice
python3 pubsubclient.py --topic weather localhost:4001 bob
```

**Client commands:** `/subscribe`, `/unsubscribe`, `/topic`, `/sendfile`, `/publish`, `/listsubs`, `/listlimits`, `/quit`
**Server commands:** `/listclients [--all]`, `/listpeers [--all]`, `/peer [host]:port`, `/limit clientid topic N`, `/quit`

## How it works

- **Single-threaded event loop** using `select()` over the listening socket, stdin, and every client and peer connection.
- **Custom line-based protocol**: frames are `TYPE<US>field1<US>field2...\n`, using the ASCII unit separator (0x1F) as the delimiter. Binary payloads are hex-encoded as the last field.
- Each connection has its own receive buffer, so partial reads and multiple frames per read are handled correctly.
- Peer forwarding re-sends messages to every peer except the one they arrived from, which keeps messages from bouncing back.
- A custom double-quote-aware tokenizer parses operator commands.

## Not included

The assignment specification is not included in this repository.

## AI assistance

As declared in the source file headers, Claude was used while developing this project: to help understand and implement parts of the networking and federation logic, and to debug issues such as disconnected peers, duplicate clients and subscription propagation. Suggested code was modified and adapted to fit the assignment requirements.

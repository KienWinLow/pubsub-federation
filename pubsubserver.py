"""! @file pubsubserver.py
@author Kien Win Low
@ai Wrote Code
@ai Debugging
@aitool Claude
@aidetails AI assistance was used to help understand and implement some of
the more complex networking and federation components of the assignment,
including peer server communication, threaded connection handling, message
forwarding, and shared federation state management. Some generated examples
and suggested logic were incorporated into the final code after modification
to fit the assignment requirements.

AI tools were additionally used throughout development to help debug issues
related to disconnected peers, duplicate clients, subscription propagation,
and general unexpected runtime behaviour.
"""

import sys
import os
import select
import socket
import time

# ---------------------------------------------------------------------------
# Protocol - simple line-based framing using ASCII unit separator (0x1F)
# Frame format: TYPE<SEP>field1<SEP>field2...\n
# Binary data (files) are hex-encoded as the last field.
# ---------------------------------------------------------------------------
SEP = "\x1F"

USAGE = "Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid\n"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv):
    peers = []
    listenon = None
    i = 0
    seen_listenon = False

    while i < len(argv) and argv[i].startswith("--"):
        opt = argv[i]
        if opt == "--server":
            if i + 1 >= len(argv) or argv[i + 1] == "":
                sys.stderr.write(USAGE); sys.exit(1)
            peers.append(argv[i + 1])
            i += 2
        elif opt == "--listenon":
            if seen_listenon:
                sys.stderr.write(USAGE); sys.exit(1)
            if i + 1 >= len(argv) or argv[i + 1] == "":
                sys.stderr.write(USAGE); sys.exit(1)
            listenon = argv[i + 1]
            seen_listenon = True
            i += 2
        else:
            sys.stderr.write(USAGE); sys.exit(1)

    remaining = argv[i:]
    if len(remaining) != 1 or remaining[0] == "":
        sys.stderr.write(USAGE); sys.exit(1)

    return peers, listenon, remaining[0]


def validate_id(s):
    return 2 <= len(s) <= 32 and all(c.isalpha() or c.isdigit() for c in s)


def validate_topic(t):
    if not t or not t[0].isalpha():
        return False
    return all(c.isalpha() or c.isdigit() or c == ' ' or c == '/' for c in t)


# ---------------------------------------------------------------------------
# Framing helpers
# ---------------------------------------------------------------------------

def make_frame(*fields, raw_payload=None):
    parts = [str(f) for f in fields]
    if raw_payload is not None:
        parts.append(raw_payload.hex())
    return (SEP.join(parts) + "\n").encode("utf-8")


def send_frame(sock, *fields, raw_payload=None):
    try:
        sock.sendall(make_frame(*fields, raw_payload=raw_payload))
        return True
    except Exception:
        return False


def parse_frame(line):
    return line.split(SEP)


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def parse_filter(s):
    for op in ("<=", ">=", "!=", "==", "<", ">"):
        if s.strip().startswith(op):
            try:
                return op, float(s.strip()[len(op):].strip())
            except ValueError:
                return None
    return None


def matches_filter(message, op, val):
    try:
        num = float(message.strip())
    except ValueError:
        return False
    return (op == "<"  and num < val  or
            op == ">"  and num > val  or
            op == "<=" and num <= val or
            op == ">=" and num >= val or
            op == "==" and num == val or
            op == "!=" and num != val)


def ascii_sort(lst):
    return sorted(lst)


# ---------------------------------------------------------------------------
# Tokenizer (double-quote aware, same rules for server and client)
# ---------------------------------------------------------------------------

def tokenize(line):
    """Split line into tokens. Returns list or None on parse error."""
    if line.count('"') % 2 != 0:
        return None
    tokens = []
    i, n = 0, len(line)
    while i < n:
        while i < n and line[i] in (' ', '\t'):
            i += 1
        if i >= n:
            break
        if line[i] == '"':
            i += 1
            start = i
            while i < n and line[i] != '"':
                i += 1
            if i >= n:
                return None
            token = line[start:i]
            i += 1
            if i < n and line[i] not in (' ', '\t'):
                return None
            tokens.append(token)
        else:
            start = i
            while i < n and line[i] not in (' ', '\t'):
                if line[i] == '"':
                    return None
                i += 1
            tokens.append(line[start:i])
    return tokens


# ---------------------------------------------------------------------------
# Connection buffer helper
# ---------------------------------------------------------------------------

class ConnBuf:
    """Per-connection receive buffer with line extraction."""
    def __init__(self):
        self.data = bytearray()

    def feed(self, chunk):
        self.data.extend(chunk)

    def has_line(self):
        return b"\n" in self.data

    def take_line(self):
        if b"\n" not in self.data:
            return None
        idx = self.data.index(b"\n")
        line = self.data[:idx].decode("utf-8", errors="replace")
        self.data = self.data[idx + 1:]
        return line


# ---------------------------------------------------------------------------
# Main server class
# ---------------------------------------------------------------------------

class Server:
    def __init__(self, serverid, listen_sock, listen_port):
        self.serverid = serverid
        self.listen_sock = listen_sock
        self.listen_port = listen_port

        # sock -> ConnBuf
        self.bufs = {}
        # sock -> {'type': 'client'|'peer', 'id': str}
        self.conns = {}
        # clientid -> sock
        self.clients = {}
        # peerid -> sock
        self.peers = {}

        # Subscriptions: list of {'clientid', 'topic', 'filter'}
        self.subs = []
        # (clientid, topic) -> {'n': int, 'last': float|None}
        self.rate_limits = {}

        # Federation subscriptions from peers: list of {'clientid', 'peerid', 'topic', 'filter'}
        self.peer_subs = []

    # -----------------------------------------------------------------------
    # Low-level I/O
    # -----------------------------------------------------------------------

    def _recv(self, sock):
        """Read available data into buffer. Returns False if connection closed."""
        buf = self.bufs.get(sock)
        if buf is None:
            return False
        try:
            chunk = sock.recv(4096)
        except BlockingIOError:
            return True
        except Exception:
            return False
        if not chunk:
            return False
        buf.feed(chunk)
        return True

    # -----------------------------------------------------------------------
    # Handshake helpers (blocking-with-select for up to 1s)
    # -----------------------------------------------------------------------

    def _read_line_timeout(self, sock, buf, deadline):
        """Read one line from sock/buf within deadline. Returns line or None."""
        while not buf.has_line():
            rem = deadline - time.time()
            if rem <= 0:
                return None
            r, _, _ = select.select([sock], [], [], rem)
            if not r:
                return None
            try:
                chunk = sock.recv(4096)
            except Exception:
                return None
            if not chunk:
                return None
            buf.feed(chunk)
        return buf.take_line()

    # -----------------------------------------------------------------------
    # Accept incoming connection
    # -----------------------------------------------------------------------

    def accept(self):
        try:
            conn, addr = self.listen_sock.accept()
            conn.setblocking(False)
        except Exception:
            return

        buf = ConnBuf()
        deadline = time.time() + 1.0

        line = self._read_line_timeout(conn, buf, deadline)
        if line is None:
            sys.stderr.write("pubsubserver: Connection with unknown client aborted\n")
            sys.stderr.flush()
            conn.close()
            return

        fields = parse_frame(line)
        msg_type = fields[0] if fields else ""

        if msg_type == "HELLO_CLIENT":
            self._handshake_client(conn, buf, deadline)
        elif msg_type == "HELLO_SERVER":
            self._handshake_peer_incoming(conn, buf, deadline)
        else:
            sys.stderr.write("pubsubserver: Connection with unknown client aborted\n")
            sys.stderr.flush()
            conn.close()

    def _handshake_client(self, conn, buf, deadline):
        line = self._read_line_timeout(conn, buf, deadline)
        if line is None:
            sys.stderr.write("pubsubserver: Connection with unknown client aborted\n")
            sys.stderr.flush()
            conn.close()
            return

        fields = parse_frame(line)
        if fields[0] != "ID" or len(fields) < 2:
            sys.stderr.write("pubsubserver: Connection with unknown client aborted\n")
            sys.stderr.flush()
            conn.close()
            return

        client_id = fields[1]

        # Check uniqueness (local server only, per spec v1.2)
        if client_id in self.clients:
            send_frame(conn, "NACK", "duplicate")
            sys.stdout.write(f'pubsubserver: Client ID "{client_id}" would be duplicated - aborting connection\n')
            sys.stdout.flush()
            conn.close()
            return

        send_frame(conn, "ACK")
        self.bufs[conn] = buf
        self.conns[conn] = {'type': 'client', 'id': client_id}
        self.clients[client_id] = conn
        sys.stdout.write(f'pubsubserver: Client "{client_id}" has connected\n')
        sys.stdout.flush()

    def _handshake_peer_incoming(self, conn, buf, deadline):
        line = self._read_line_timeout(conn, buf, deadline)
        if line is None:
            sys.stderr.write("pubsubserver: Connection with unknown client aborted\n")
            sys.stderr.flush()
            conn.close()
            return

        fields = parse_frame(line)
        if fields[0] != "ID" or len(fields) < 2:
            sys.stderr.write("pubsubserver: Connection with unknown client aborted\n")
            sys.stderr.flush()
            conn.close()
            return
        peer_id = fields[1]

        line2 = self._read_line_timeout(conn, buf, deadline)
        their_fed = []
        if line2:
            f2 = parse_frame(line2)
            if f2[0] == "FED_IDS" and len(f2) > 1 and f2[1]:
                their_fed = f2[1].split(SEP)

        # Validate
        err = self._check_peer_connect(peer_id, set(their_fed))
        if err:
            send_frame(conn, "NACK", err)
            self._print_peer_error(peer_id, err, "")
            conn.close()
            return

        # Send our reply
        send_frame(conn, "HELLO_SERVER")
        send_frame(conn, "ID", self.serverid)
        our_fed = list(self.peers.keys()) + [self.serverid]
        send_frame(conn, "FED_IDS", SEP.join(our_fed))

        self.bufs[conn] = buf
        self.conns[conn] = {'type': 'peer', 'id': peer_id}
        self.peers[peer_id] = conn
        sys.stdout.write(f'pubsubserver: Connection received from peer "{peer_id}"\n')
        sys.stdout.flush()

    def _check_peer_connect(self, peer_id, their_fed_ids):
        if peer_id == self.serverid:
            return "self"
        if peer_id in self.peers:
            return "already"
        our = set(self.peers.keys()) | {self.serverid}
        them = their_fed_ids | {peer_id}
        if our & them:
            return "dup_ids"
        return None

    def _print_peer_error(self, peer_id, err, addr_str):
        if err == "self":
            sys.stderr.write("pubsubserver: Can't connect to self as peer\n")
        elif err == "already":
            sys.stderr.write(f'pubsubserver: Already connected to peer server at "{addr_str}"\n')
        elif err == "dup_ids":
            sys.stderr.write(f'pubsubserver: Unable to connect to server "{addr_str}" due to common server IDs\n')
        sys.stderr.flush()

    # -----------------------------------------------------------------------
    # Outgoing peer connection
    # -----------------------------------------------------------------------

    def connect_peer(self, addr_str):
        colon = addr_str.rfind(":")
        if colon < 0:
            sys.stderr.write(f'pubsubserver: can\'t connect to peer "{addr_str}"\n')
            sys.stderr.flush()
            return
        host = addr_str[:colon] or "localhost"
        port_s = addr_str[colon + 1:]

        try:
            port = int(port_s)
        except ValueError:
            try:
                port = socket.getservbyname(port_s)
            except Exception:
                sys.stderr.write(f'pubsubserver: can\'t connect to peer "{addr_str}"\n')
                sys.stderr.flush()
                return

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.setblocking(False)
        except Exception:
            sys.stderr.write(f'pubsubserver: can\'t connect to peer "{addr_str}"\n')
            sys.stderr.flush()
            return

        buf = ConnBuf()
        deadline = time.time() + 1.0

        # Send hello
        send_frame(sock, "HELLO_SERVER")
        send_frame(sock, "ID", self.serverid)
        our_fed = list(self.peers.keys()) + [self.serverid]
        send_frame(sock, "FED_IDS", SEP.join(our_fed))

        # Read peer's reply
        line = self._read_line_timeout(sock, buf, deadline)
        if not line or parse_frame(line)[0] != "HELLO_SERVER":
            sys.stderr.write(f'pubsubserver: Peer server not found at "{addr_str}"\n')
            sys.stderr.flush()
            sock.close()
            return

        line = self._read_line_timeout(sock, buf, deadline)
        if not line:
            sys.stderr.write(f'pubsubserver: Peer server not found at "{addr_str}"\n')
            sys.stderr.flush()
            sock.close()
            return
        fields = parse_frame(line)
        if fields[0] != "ID" or len(fields) < 2:
            sys.stderr.write(f'pubsubserver: Peer server not found at "{addr_str}"\n')
            sys.stderr.flush()
            sock.close()
            return
        peer_id = fields[1]

        line = self._read_line_timeout(sock, buf, deadline)
        their_fed = []
        if line:
            f = parse_frame(line)
            if f[0] == "FED_IDS" and len(f) > 1 and f[1]:
                their_fed = f[1].split(SEP)

        err = self._check_peer_connect(peer_id, set(their_fed))
        if err:
            self._print_peer_error(peer_id, err, addr_str)
            sock.close()
            return

        self.bufs[sock] = buf
        self.conns[sock] = {'type': 'peer', 'id': peer_id}
        self.peers[peer_id] = sock
        sys.stdout.write(f'pubsubserver: Connected to peer "{peer_id}" at "{addr_str}"\n')
        sys.stdout.flush()

    # -----------------------------------------------------------------------
    # Dispatch incoming data
    # -----------------------------------------------------------------------

    def dispatch(self, sock):
        """Called when select() says sock is readable. Read data and process lines."""
        ok = self._recv(sock)
        if not ok:
            info = self.conns.get(sock)
            if info:
                if info['type'] == 'client':
                    self._drop_client(sock, info['id'])
                else:
                    self._drop_peer(sock, info['id'], orderly=False)
            return

        buf = self.bufs.get(sock)
        if buf is None:
            return

        while buf.has_line():
            line = buf.take_line()
            info = self.conns.get(sock)
            if info is None:
                break
            if info['type'] == 'client':
                self._handle_client_msg(sock, info['id'], line)
            else:
                self._handle_peer_msg(sock, info['id'], line)

    # -----------------------------------------------------------------------
    # Client message handling
    # -----------------------------------------------------------------------

    def _handle_client_msg(self, sock, client_id, line):
        fields = parse_frame(line)
        if not fields:
            return
        t = fields[0]

        if t == "SUBSCRIBE":
            topic = fields[1] if len(fields) > 1 else ""
            fstr  = fields[2] if len(fields) > 2 else ""
            fp    = parse_filter(fstr) if fstr else None
            self.subs.append({'clientid': client_id, 'topic': topic, 'filter': fp})
            # Propagate to peers
            for pid, ps in self.peers.items():
                send_frame(ps, "SUB_FWD", client_id, self.serverid, topic, fstr)

        elif t == "UNSUBSCRIBE":
            topic = fields[1] if len(fields) > 1 else ""
            self.subs = [s for s in self.subs
                         if not (s['clientid'] == client_id and s['topic'] == topic)]
            for pid, ps in self.peers.items():
                send_frame(ps, "UNSUB_FWD", client_id, self.serverid, topic)

        elif t == "PUBLISH":
            topic   = fields[1] if len(fields) > 1 else ""
            message = fields[2] if len(fields) > 2 else ""
            self._publish(client_id, self.serverid, topic, message, is_file=False)

        elif t == "PUBLISH_FILE":
            topic    = fields[1] if len(fields) > 1 else ""
            basename = fields[2] if len(fields) > 2 else ""
            hexdata  = fields[3] if len(fields) > 3 else ""
            raw = bytes.fromhex(hexdata)
            self._publish(client_id, self.serverid, topic, basename, is_file=True, raw=raw)

        elif t == "DISCONNECT":
            self._drop_client(sock, client_id)

    # -----------------------------------------------------------------------
    # Peer message handling
    # -----------------------------------------------------------------------

    def _handle_peer_msg(self, sock, peer_id, line):
        fields = parse_frame(line)
        if not fields:
            return
        t = fields[0]

        if t == "FWD":
            # FWD SEP src_server SEP src_client SEP topic SEP message
            if len(fields) < 5: return
            src_srv, src_cli, topic, msg = fields[1], fields[2], fields[3], fields[4]
            self._deliver_local(src_srv, src_cli, topic, msg, is_file=False)
            # Re-flood to other peers (except source)
            for pid, ps in self.peers.items():
                if ps != sock:
                    send_frame(ps, "FWD", src_srv, src_cli, topic, msg)

        elif t == "FWD_FILE":
            if len(fields) < 6: return
            src_srv, src_cli, topic = fields[1], fields[2], fields[3]
            basename, hexdata = fields[4], fields[5]
            raw = bytes.fromhex(hexdata)
            self._deliver_local(src_srv, src_cli, topic, basename, is_file=True, raw=raw)
            for pid, ps in self.peers.items():
                if ps != sock:
                    send_frame(ps, "FWD_FILE", src_srv, src_cli, topic, basename,
                               raw_payload=raw)

        elif t == "SUB_FWD":
            if len(fields) < 4: return
            cli, srv, topic = fields[1], fields[2], fields[3]
            fstr = fields[4] if len(fields) > 4 else ""
            fp = parse_filter(fstr) if fstr else None
            self.peer_subs.append({'clientid': cli, 'peerid': peer_id,
                                   'serverid': srv, 'topic': topic, 'filter': fp})
            for pid, ps in self.peers.items():
                if ps != sock:
                    send_frame(ps, "SUB_FWD", cli, srv, topic, fstr)

        elif t == "UNSUB_FWD":
            if len(fields) < 4: return
            cli, srv, topic = fields[1], fields[2], fields[3]
            self.peer_subs = [s for s in self.peer_subs
                              if not (s['clientid'] == cli and s['topic'] == topic
                                      and s['peerid'] == peer_id)]
            for pid, ps in self.peers.items():
                if ps != sock:
                    send_frame(ps, "UNSUB_FWD", cli, srv, topic)

        elif t == "SHUTDOWN":
            self._drop_peer(sock, peer_id, orderly=True)

        elif t == "CLIENTS_REQ":
            entries = [f"{self.serverid}:{c}" for c in self.clients.keys()]
            send_frame(sock, "CLIENTS_RESP", "\n".join(entries))

        elif t == "CLIENTS_RESP":
            data = fields[1] if len(fields) > 1 else ""
            if not hasattr(self, '_cl_acc'):
                self._cl_acc = []
            if data:
                self._cl_acc.extend(data.split("\n"))
            self._cl_wait = getattr(self, '_cl_wait', 1) - 1
            if self._cl_wait <= 0:
                self._finish_listclients_all()

        elif t == "PEERS_REQ":
            ids = list(self.peers.keys())
            send_frame(sock, "PEERS_RESP", "\n".join(ids))

        elif t == "PEERS_RESP":
            data = fields[1] if len(fields) > 1 else ""
            if not hasattr(self, '_pr_acc'):
                self._pr_acc = set()
            if data:
                for x in data.split("\n"):
                    if x: self._pr_acc.add(x)
            self._pr_wait = getattr(self, '_pr_wait', 1) - 1
            if self._pr_wait <= 0:
                self._finish_listpeers_all()

        elif t == "RATE_LIMIT_FWD":
            pass  # handled locally

        elif t == "NACK":
            pass  # late nack during handshake, ignore

    # -----------------------------------------------------------------------
    # Publish / deliver
    # -----------------------------------------------------------------------

    def _publish(self, src_cli, src_srv, topic, payload, is_file=False, raw=None):
        """Handle a published message originating from our server."""
        # Rate limit check (only for local clients)
        if src_srv == self.serverid:
            key = (src_cli, topic)
            if key in self.rate_limits:
                rl = self.rate_limits[key]
                if rl['n'] > 0:
                    now = time.time()
                    if rl['last'] is not None and now - rl['last'] < rl['n']:
                        csock = self.clients.get(src_cli)
                        if csock:
                            send_frame(csock, "RATE_LIMITED")
                        return
                    rl['last'] = now

        # Deliver to local subscribers
        self._deliver_local(src_srv, src_cli, topic, payload, is_file=is_file, raw=raw)

        # Forward to peers
        for pid, ps in self.peers.items():
            if is_file:
                send_frame(ps, "FWD_FILE", src_srv, src_cli, topic, payload,
                           raw_payload=raw)
            else:
                send_frame(ps, "FWD", src_srv, src_cli, topic, payload)

    def _deliver_local(self, src_srv, src_cli, topic, payload, is_file=False, raw=None):
        """Deliver a message to all matching local subscribers (deduplicated)."""
        sent = set()
        for sub in self.subs:
            if sub['topic'] != topic:
                continue
            cid = sub['clientid']
            if cid in sent:
                continue
            # Filter check (files never match filter subs per spec footnote 7)
            if sub['filter'] is not None:
                if is_file:
                    continue
                op, val = sub['filter']
                if not matches_filter(payload, op, val):
                    continue
            csock = self.clients.get(cid)
            if csock is None:
                continue
            sent.add(cid)
            if is_file:
                send_frame(csock, "FILE_MSG", topic, src_srv, src_cli, payload,
                           raw_payload=raw)
            else:
                send_frame(csock, "MSG", topic, src_srv, src_cli, payload)

    # -----------------------------------------------------------------------
    # Disconnect helpers
    # -----------------------------------------------------------------------

    def _drop_client(self, sock, client_id):
        self.subs = [s for s in self.subs if s['clientid'] != client_id]
        self.clients.pop(client_id, None)
        self.conns.pop(sock, None)
        self.bufs.pop(sock, None)
        try: sock.close()
        except Exception: pass
        sys.stdout.write(f'pubsubserver: Client "{client_id}" has disconnected\n')
        sys.stdout.flush()

    def _drop_peer(self, sock, peer_id, orderly):
        self.peers.pop(peer_id, None)
        self.peer_subs = [s for s in self.peer_subs if s['peerid'] != peer_id]
        self.conns.pop(sock, None)
        self.bufs.pop(sock, None)
        try: sock.close()
        except Exception: pass
        if orderly:
            sys.stdout.write(f'pubsubserver: Peer server "{peer_id}" shutting down\n')
            sys.stdout.flush()
        else:
            sys.stderr.write(f'pubsubserver: Peer server "{peer_id}" disconnected\n')
            sys.stderr.flush()

    # -----------------------------------------------------------------------
    # Stdin command handling
    # -----------------------------------------------------------------------

    def handle_stdin(self):
        try:
            line = sys.stdin.readline()
        except Exception:
            line = ""
        if not line:
            self._do_quit()
            return
        self._cmd(line.rstrip("\n"))

    def _cmd(self, raw):
        stripped = raw.lstrip()
        if not stripped.startswith("/"):
            sys.stderr.write("pubsubserver: unknown command\n")
            sys.stderr.flush()
            return
        tokens = tokenize(stripped)
        if tokens is None:
            sys.stderr.write("pubsubserver: unknown command\n")
            sys.stderr.flush()
            return
        if not tokens:
            sys.stderr.write("pubsubserver: unknown command\n")
            sys.stderr.flush()
            return
        cmd, args = tokens[0], tokens[1:]
        {
            "/listclients": self._cmd_listclients,
            "/listpeers":   self._cmd_listpeers,
            "/peer":        self._cmd_peer,
            "/limit":       self._cmd_limit,
            "/quit":        self._cmd_quit,
        }.get(cmd, self._cmd_unknown)(args)

    def _cmd_unknown(self, args):
        sys.stderr.write("pubsubserver: unknown command\n")
        sys.stderr.flush()

    def _cmd_listclients(self, args):
        all_flag = False
        if args:
            if args == ["--all"]:
                all_flag = True
            else:
                sys.stderr.write("pubsubserver: unknown argument(s) - usage: /listclients [--all]\n")
                sys.stderr.flush()
                return
        if not all_flag:
            if not self.clients:
                sys.stdout.write("pubsubserver: No clients connected\n")
            else:
                for l in ascii_sort([f"{self.serverid}:{c}" for c in self.clients]):
                    sys.stdout.write(l + "\n")
            sys.stdout.flush()
        else:
            local = [f"{self.serverid}:{c}" for c in self.clients]
            if not self.peers:
                lines = ascii_sort(local)
                if not lines:
                    sys.stdout.write("pubsubserver: No clients connected\n")
                else:
                    for l in lines: sys.stdout.write(l + "\n")
                sys.stdout.flush()
                return
            self._cl_acc = list(local)
            self._cl_wait = len(self.peers)
            for pid, ps in self.peers.items():
                send_frame(ps, "CLIENTS_REQ")

    def _finish_listclients_all(self):
        lines = ascii_sort(list(set(getattr(self, '_cl_acc', []))))
        if not lines:
            sys.stdout.write("pubsubserver: No clients connected\n")
        else:
            for l in lines: sys.stdout.write(l + "\n")
        sys.stdout.flush()
        self._cl_acc = []

    def _cmd_listpeers(self, args):
        all_flag = False
        if args:
            if args == ["--all"]:
                all_flag = True
            else:
                sys.stderr.write("pubsubserver: unknown argument(s) - usage: /listpeers [--all]\n")
                sys.stderr.flush()
                return
        if not all_flag:
            if not self.peers:
                sys.stdout.write("pubsubserver: No peer servers connected\n")
            else:
                for p in ascii_sort(list(self.peers)): sys.stdout.write(p + "\n")
            sys.stdout.flush()
        else:
            known = set(self.peers.keys())
            if not self.peers:
                if not known:
                    sys.stdout.write("pubsubserver: No peer servers connected\n")
                sys.stdout.flush()
                return
            self._pr_acc = known.copy()
            self._pr_wait = len(self.peers)
            for pid, ps in self.peers.items():
                send_frame(ps, "PEERS_REQ")

    def _finish_listpeers_all(self):
        ids = ascii_sort(list(getattr(self, '_pr_acc', set())))
        if not ids:
            sys.stdout.write("pubsubserver: No peer servers connected\n")
        else:
            for p in ids: sys.stdout.write(p + "\n")
        sys.stdout.flush()
        self._pr_acc = set()

    def _cmd_peer(self, args):
        if len(args) != 1 or not args[0]:
            sys.stderr.write("pubsubserver: unknown argument(s) - usage: /peer [server]:port\n")
            sys.stderr.flush()
            return
        self.connect_peer(args[0])

    def _cmd_limit(self, args):
        if len(args) != 3:
            sys.stderr.write("pubsubserver: unknown argument(s) - usage: /limit clientid topic N\n")
            sys.stderr.flush()
            return
        cid, topic, n_s = args
        if not validate_id(cid) or cid not in self.clients:
            sys.stderr.write(f'pubsubserver: Client "{cid}" is unknown\n')
            sys.stderr.flush()
            return
        if not validate_topic(topic):
            sys.stderr.write(f'pubsubserver: Topic "{topic}" is not valid\n')
            sys.stderr.flush()
            return
        try:
            n = int(n_s)
            if n < 0 or n > 3600: raise ValueError
        except ValueError:
            sys.stderr.write("pubsubserver: Rate limit must be 0 to 3600 seconds inclusive\n")
            sys.stderr.flush()
            return
        key = (cid, topic)
        self.rate_limits[key] = {'n': n, 'last': None}
        csock = self.clients.get(cid)
        if csock:
            send_frame(csock, "RATE_LIMIT", topic, str(n))

    def _cmd_quit(self, args):
        if args:
            sys.stderr.write("pubsubserver: unknown argument(s) - usage: /quit\n")
            sys.stderr.flush()
            return
        self._do_quit()

    def _do_quit(self):
        for csock in list(self.clients.values()):
            send_frame(csock, "SHUTDOWN")
        for psock in list(self.peers.values()):
            send_frame(psock, "SHUTDOWN")
        sys.exit(0)

    # -----------------------------------------------------------------------
    # Main select loop
    # -----------------------------------------------------------------------

    def run(self):
        while True:
            rlist = [self.listen_sock, sys.stdin] + list(self.conns.keys())
            try:
                readable, _, exceptional = select.select(rlist, [], rlist, 0.1)
            except (ValueError, OSError):
                # Clean up bad sockets
                for s in list(self.conns.keys()):
                    try:
                        select.select([s], [], [], 0)
                    except Exception:
                        info = self.conns.get(s)
                        if info:
                            if info['type'] == 'client':
                                self._drop_client(s, info['id'])
                            else:
                                self._drop_peer(s, info['id'], orderly=False)
                continue

            for s in readable:
                if s is sys.stdin:
                    self.handle_stdin()
                elif s is self.listen_sock:
                    self.accept()
                else:
                    self.dispatch(s)

            for s in exceptional:
                info = self.conns.get(s)
                if info:
                    if info['type'] == 'client':
                        self._drop_client(s, info['id'])
                    else:
                        self._drop_peer(s, info['id'], orderly=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    peers, listenon, serverid = parse_args(sys.argv[1:])

    if not validate_id(serverid):
        sys.stderr.write(f'pubsubserver: bad server ID "{serverid}"\n')
        sys.stderr.flush()
        sys.exit(2)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if listenon is not None:
        try:
            port = int(listenon)
        except ValueError:
            try:
                port = socket.getservbyname(listenon)
            except Exception:
                sys.stderr.write(f'pubsubserver: can\'t listen on port "{listenon}"\n')
                sys.stderr.flush()
                sys.exit(3)
        try:
            sock.bind(("", port if port != 0 else 0))
        except Exception:
            sys.stderr.write(f'pubsubserver: can\'t listen on port "{listenon}"\n')
            sys.stderr.flush()
            sys.exit(3)
    else:
        sock.bind(("", 0))

    try:
        sock.listen(10)
    except Exception:
        sys.stderr.write(f'pubsubserver: can\'t listen on port "{listenon}"\n')
        sys.stderr.flush()
        sys.exit(3)

    actual_port = sock.getsockname()[1]
    sock.setblocking(False)

    sys.stderr.write(f"pubsubserver: listening on port {actual_port}\n")
    sys.stderr.flush()

    server = Server(serverid, sock, actual_port)

    for p in peers:
        server.connect_peer(p)

    server.run()


if __name__ == "__main__":
    main()
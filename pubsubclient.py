"""! @file pubsubclient.py
@author Kien Win Low
@ai Wrote Code
@ai Debugging
@aitool Claude
@aidetails AI tools were used during development to help work through some
of the more difficult parts of the assignment, particularly socket
communication, threading behaviour, JSON message handling, and parts of the
publish/subscribe logic. Some AI-suggested code snippets and approaches were
adapted into the final implementation after modification and testing.

AI was also used as a debugging aid when certain features were not behaving
as expected, including connection handling, subscription behaviour, and rate
limit edge cases.
"""

import sys
import os
import select
import socket
import time

SEP = "\x1F"
USAGE = "Usage: pubsubclient [--topic topic] [server]:port clientid [message]\n"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv):
    i = 0
    default_topic = None
    seen_topic = False

    while i < len(argv) and argv[i].startswith("--"):
        opt = argv[i]
        if opt == "--topic":
            if seen_topic:
                sys.stderr.write(USAGE); sys.exit(1)
            if i + 1 >= len(argv) or argv[i + 1] == "":
                sys.stderr.write(USAGE); sys.exit(1)
            default_topic = argv[i + 1]
            seen_topic = True
            i += 2
        else:
            sys.stderr.write(USAGE); sys.exit(1)

    remaining = argv[i:]
    if len(remaining) < 2 or len(remaining) > 3:
        sys.stderr.write(USAGE); sys.exit(1)

    server_port = remaining[0]
    clientid    = remaining[1]
    message     = remaining[2] if len(remaining) == 3 else None

    if ":" not in server_port:
        sys.stderr.write(USAGE); sys.exit(1)

    colon = server_port.rfind(":")
    if server_port[colon + 1:] == "":
        sys.stderr.write(USAGE); sys.exit(1)

    if clientid == "":
        sys.stderr.write(USAGE); sys.exit(1)

    if message is not None and default_topic is None:
        sys.stderr.write(USAGE); sys.exit(1)

    return default_topic, server_port, clientid, message


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_id(s):
    return 2 <= len(s) <= 32 and all(c.isalpha() or c.isdigit() for c in s)


def validate_topic(t):
    if not t or not t[0].isalpha():
        return False
    return all(c.isalpha() or c.isdigit() or c == ' ' or c == '/' for c in t)


def validate_printable(s):
    return all(c.isprintable() for c in s)


def parse_filter(s):
    for op in ("<=", ">=", "!=", "==", "<", ">"):
        if s.strip().startswith(op):
            try:
                return op, float(s.strip()[len(op):].strip())
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# Framing
# ---------------------------------------------------------------------------

def send_frame(sock, *fields, raw_payload=None):
    parts = [str(f) for f in fields]
    if raw_payload is not None:
        parts.append(raw_payload.hex())
    try:
        sock.sendall((SEP.join(parts) + "\n").encode("utf-8"))
        return True
    except Exception:
        return False


def parse_frame(line):
    return line.split(SEP)


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

def tokenize(line):
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
# Connection buffer
# ---------------------------------------------------------------------------

class ConnBuf:
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

    def read_with_timeout(self, sock, timeout):
        """Read until newline or timeout. Returns line or None."""
        deadline = time.time() + timeout
        while not self.has_line():
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
            self.feed(chunk)
        return self.take_line()


# ---------------------------------------------------------------------------
# Subscription / rate-limit state
# ---------------------------------------------------------------------------

class Subscription:
    def __init__(self, topic, filter_parsed, filter_str_orig):
        self.topic = topic
        self.filter_parsed = filter_parsed
        self.filter_str_orig = filter_str_orig

    def identical_to(self, topic, fp):
        if self.topic != topic:
            return False
        if self.filter_parsed is None and fp is None:
            return True
        if self.filter_parsed is None or fp is None:
            return False
        return (self.filter_parsed[0] == fp[0] and
                float(self.filter_parsed[1]) == float(fp[1]))

    def list_str(self):
        tp = f'"{self.topic}"' if ' ' in self.topic else self.topic
        if self.filter_parsed is None:
            return f"/subscribe {tp}"
        fs = self.filter_str_orig
        fp = f'"{fs}"' if ' ' in fs else fs
        return f"/subscribe {tp} {fp}"


class RateLimit:
    def __init__(self, clientid, topic, n):
        self.clientid = clientid
        self.topic = topic
        self.n = n
        self.last = None

    def allowed(self):
        if self.n == 0:
            return True
        now = time.time()
        if self.last is not None and now - self.last < self.n:
            return False
        self.last = now
        return True

    def list_str(self):
        tp = f'"{self.topic}"' if ' ' in self.topic else self.topic
        return f"/limit {self.clientid} {tp} {self.n}"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, sock, buf, display, clientid, default_topic):
        self.sock = sock
        self.buf = buf
        self.display = display
        self.clientid = clientid
        self.default_topic = default_topic
        self.subs = []
        self.limits = []
        self.file_count = 0

    def run(self):
        sys.stdout.write("Welcome to pubsubclient!\n")
        sys.stdout.flush()

        while True:
            rlist = [sys.stdin, self.sock]
            try:
                readable, _, exceptional = select.select(rlist, [], rlist, 0.1)
            except Exception:
                self._server_disconnected()

            for s in exceptional:
                self._server_disconnected()

            for s in readable:
                if s is sys.stdin:
                    self._read_stdin()
                else:
                    self._read_server()

    def _read_stdin(self):
        try:
            line = sys.stdin.readline()
        except Exception:
            sys.exit(0)
        if not line:
            sys.exit(0)
        self._process(line.rstrip("\n"))

    def _read_server(self):
        try:
            chunk = self.sock.recv(4096)
        except BlockingIOError:
            return
        except Exception:
            self._server_disconnected()
            return
        if not chunk:
            self._server_disconnected()
            return
        self.buf.feed(chunk)
        while self.buf.has_line():
            line = self.buf.take_line()
            self._handle_server_msg(line)

    def _server_disconnected(self):
        sys.stderr.write("pubsubclient: server disconnected - exiting\n")
        sys.stderr.flush()
        sys.exit(10)

    # -----------------------------------------------------------------------
    # Stdin processing
    # -----------------------------------------------------------------------

    def _process(self, raw):
        stripped = raw.lstrip()
        if not stripped:
            return
        if stripped.startswith("/"):
            self._command(stripped)
        else:
            self._publish_plain(raw)

    def _publish_plain(self, raw):
        msg = raw.strip()
        if not msg:
            return
        if self.default_topic is None:
            sys.stderr.write("pubsubclient: no default topic set\n")
            sys.stderr.flush()
            return
        if not validate_printable(msg):
            sys.stderr.write("pubsubclient: messages must only contain printable characters\n")
            sys.stderr.flush()
            return
        if self._rate_limited(self.default_topic):
            sys.stderr.write("pubsubclient: message publication failed due to rate limit\n")
            sys.stderr.flush()
            return
        send_frame(self.sock, "PUBLISH", self.default_topic, msg)

    def _command(self, line):
        tokens = tokenize(line)
        if tokens is None:
            sys.stderr.write("pubsubclient: unknown command\n")
            sys.stderr.flush()
            return
        if not tokens:
            sys.stderr.write("pubsubclient: unknown command\n")
            sys.stderr.flush()
            return
        cmd, args = tokens[0], tokens[1:]
        dispatch = {
            "/subscribe":   self._cmd_subscribe,
            "/unsubscribe": self._cmd_unsubscribe,
            "/topic":       self._cmd_topic,
            "/sendfile":    self._cmd_sendfile,
            "/listsubs":    self._cmd_listsubs,
            "/listlimits":  self._cmd_listlimits,
            "/publish":     self._cmd_publish,
            "/quit":        self._cmd_quit,
        }
        handler = dispatch.get(cmd)
        if handler is None:
            sys.stderr.write("pubsubclient: unknown command\n")
            sys.stderr.flush()
            return
        handler(args)

    def _cmd_subscribe(self, args):
        if len(args) < 1 or len(args) > 2:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /subscribe topic [filter]\n")
            sys.stderr.flush()
            return
        topic = args[0]
        fstr  = args[1] if len(args) == 2 else None
        if not validate_topic(topic):
            sys.stderr.write(f'pubsubclient: invalid topic string "{topic}"\n')
            sys.stderr.flush()
            return
        fp = None
        if fstr is not None:
            fp = parse_filter(fstr)
            if fp is None:
                sys.stderr.write(f'pubsubclient: invalid filter string "{fstr}"\n')
                sys.stderr.flush()
                return
        for s in self.subs:
            if s.identical_to(topic, fp):
                sys.stderr.write("pubsubclient: identical subscription ignored\n")
                sys.stderr.flush()
                return
        self.subs.append(Subscription(topic, fp, fstr or ""))
        send_frame(self.sock, "SUBSCRIBE", topic, fstr or "")

    def _cmd_unsubscribe(self, args):
        if len(args) != 1:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /unsubscribe topic\n")
            sys.stderr.flush()
            return
        topic = args[0]
        if not validate_topic(topic):
            sys.stderr.write(f'pubsubclient: invalid topic string "{topic}"\n')
            sys.stderr.flush()
            return
        matching = [s for s in self.subs if s.topic == topic]
        if not matching:
            sys.stderr.write(f'pubsubclient: not subscribed to messages about "{topic}"\n')
            sys.stderr.flush()
            return
        self.subs = [s for s in self.subs if s.topic != topic]
        sys.stdout.write(f'pubsubclient: unsubscribed from messages about "{topic}"\n')
        sys.stdout.flush()
        send_frame(self.sock, "UNSUBSCRIBE", topic)

    def _cmd_topic(self, args):
        if len(args) != 1:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /topic topic\n")
            sys.stderr.flush()
            return
        if not validate_topic(args[0]):
            sys.stderr.write(f'pubsubclient: invalid topic string "{args[0]}"\n')
            sys.stderr.flush()
            return
        self.default_topic = args[0]

    def _cmd_sendfile(self, args):
        if len(args) < 1 or len(args) > 2:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /sendfile filename [topic]\n")
            sys.stderr.flush()
            return
        filename  = args[0]
        topic_arg = args[1] if len(args) == 2 else None
        try:
            with open(filename, "rb") as fh:
                raw = fh.read()
        except Exception:
            sys.stderr.write(f'pubsubclient: unable to open file "{filename}"\n')
            sys.stderr.flush()
            return
        if topic_arg is not None:
            if not validate_topic(topic_arg):
                sys.stderr.write(f'pubsubclient: invalid topic string "{topic_arg}"\n')
                sys.stderr.flush()
                return
            use_topic = topic_arg
        else:
            if self.default_topic is None:
                sys.stderr.write("pubsubclient: no default topic set\n")
                sys.stderr.flush()
                return
            use_topic = self.default_topic
        if self._rate_limited(use_topic):
            sys.stderr.write("pubsubclient: message publication failed due to rate limit\n")
            sys.stderr.flush()
            return
        basename = os.path.basename(filename)
        send_frame(self.sock, "PUBLISH_FILE", use_topic, basename, raw_payload=raw)

    def _cmd_listsubs(self, args):
        if args:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /listsubs\n")
            sys.stderr.flush()
            return
        if not self.subs:
            sys.stdout.write("No subscriptions\n")
        else:
            for s in self.subs:
                sys.stdout.write(s.list_str() + "\n")
        sys.stdout.flush()

    def _cmd_listlimits(self, args):
        if args:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /listlimits\n")
            sys.stderr.flush()
            return
        if not self.limits:
            sys.stdout.write("No limits\n")
        else:
            for l in self.limits:
                sys.stdout.write(l.list_str() + "\n")
        sys.stdout.flush()

    def _cmd_publish(self, args):
        if len(args) < 2:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /publish topic message\n")
            sys.stderr.flush()
            return
        topic = args[0]
        msg   = " ".join(args[1:])
        if not validate_topic(topic):
            sys.stderr.write(f'pubsubclient: invalid topic string "{topic}"\n')
            sys.stderr.flush()
            return
        if not validate_printable(msg):
            sys.stderr.write("pubsubclient: messages must only contain printable characters\n")
            sys.stderr.flush()
            return
        if self._rate_limited(topic):
            sys.stderr.write("pubsubclient: message publication failed due to rate limit\n")
            sys.stderr.flush()
            return
        send_frame(self.sock, "PUBLISH", topic, msg)

    def _cmd_quit(self, args):
        if args:
            sys.stderr.write("pubsubclient: unknown argument(s) - usage: /quit\n")
            sys.stderr.flush()
            return
        sys.exit(0)

    # -----------------------------------------------------------------------
    # Rate limit helpers
    # -----------------------------------------------------------------------

    def _rate_limited(self, topic):
        for rl in self.limits:
            if rl.topic == topic:
                return not rl.allowed()
        return False

    def _set_rate_limit(self, topic, n):
        for rl in self.limits:
            if rl.topic == topic:
                rl.n = n
                rl.last = None
                return
        self.limits.append(RateLimit(self.clientid, topic, n))

    # -----------------------------------------------------------------------
    # Server message handling
    # -----------------------------------------------------------------------

    def _handle_server_msg(self, line):
        fields = parse_frame(line)
        if not fields:
            return
        t = fields[0]

        if t == "MSG":
            # MSG SEP topic SEP src_server SEP src_client SEP message
            if len(fields) < 5: return
            topic, srv, cli, msg = fields[1], fields[2], fields[3], fields[4]
            sys.stdout.write(f"{topic}: {msg} ({srv}:{cli})\n")
            sys.stdout.flush()

        elif t == "FILE_MSG":
            # FILE_MSG SEP topic SEP src_server SEP src_client SEP basename SEP hexdata
            if len(fields) < 6: return
            topic, srv, cli = fields[1], fields[2], fields[3]
            basename, hexdata = fields[4], fields[5]
            raw = bytes.fromhex(hexdata)
            self.file_count += 1
            save_name = f"{self.file_count}_{basename}"
            try:
                with open(save_name, "wb") as fh:
                    fh.write(raw)
                sys.stdout.write(
                    f'{topic}: received file "{save_name}" from {srv}:{cli} ({len(raw)} bytes)\n'
                )
                sys.stdout.flush()
            except Exception:
                sys.stderr.write(f'pubsubclient: cannot save file "{save_name}"\n')
                sys.stderr.flush()

        elif t == "RATE_LIMIT":
            # RATE_LIMIT SEP topic SEP N
            if len(fields) < 3: return
            topic = fields[1]
            try:
                n = int(fields[2])
            except ValueError:
                return
            self._set_rate_limit(topic, n)
            sys.stdout.write(
                f'pubsubclient: you are rate limited on topic "{topic}" to {n} seconds between messages\n'
            )
            sys.stdout.flush()

        elif t == "RATE_LIMITED":
            sys.stderr.write("pubsubclient: message publication failed due to rate limit\n")
            sys.stderr.flush()

        elif t == "SHUTDOWN":
            sys.stdout.write("pubsubclient: exiting due to server shutdown\n")
            sys.stdout.flush()
            sys.exit(0)

        # ACK/NACK handled during connect; ignore here


# ---------------------------------------------------------------------------
# Connection / handshake
# ---------------------------------------------------------------------------

def connect(server_port_str, clientid):
    colon = server_port_str.rfind(":")
    host  = server_port_str[:colon] or "localhost"
    port_s = server_port_str[colon + 1:]
    display = f"{host}:{port_s}"

    try:
        port = int(port_s)
    except ValueError:
        try:
            port = socket.getservbyname(port_s)
        except Exception:
            sys.stderr.write(f'pubsubclient: unable to connect to "{display}"\n')
            sys.stderr.flush()
            sys.exit(7)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, port))
        sock.setblocking(False)
    except Exception:
        sys.stderr.write(f'pubsubclient: unable to connect to "{display}"\n')
        sys.stderr.flush()
        sys.exit(7)

    buf = ConnBuf()

    send_frame(sock, "HELLO_CLIENT")
    send_frame(sock, "ID", clientid)

    line = buf.read_with_timeout(sock, 1.0)
    if line is None:
        sys.stderr.write(f'pubsubclient: server at "{display}" is not a valid server\n')
        sys.stderr.flush()
        sys.exit(8)

    fields = parse_frame(line)
    if fields[0] == "NACK":
        reason = fields[1] if len(fields) > 1 else ""
        if reason == "duplicate":
            sys.stderr.write(f'pubsubclient: client ID "{clientid}" is not unique\n')
            sys.stderr.flush()
            sys.exit(9)
        sys.stderr.write(f'pubsubclient: server at "{display}" is not a valid server\n')
        sys.stderr.flush()
        sys.exit(8)

    if fields[0] != "ACK":
        sys.stderr.write(f'pubsubclient: server at "{display}" is not a valid server\n')
        sys.stderr.flush()
        sys.exit(8)

    return sock, buf, display


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    default_topic, server_port, clientid, message = parse_args(sys.argv[1:])

    if not validate_id(clientid):
        sys.stderr.write(f'pubsubclient: bad client ID "{clientid}"\n')
        sys.stderr.flush()
        sys.exit(4)

    if default_topic is not None and not validate_topic(default_topic):
        sys.stderr.write(f'pubsubclient: invalid topic string "{default_topic}"\n')
        sys.stderr.flush()
        sys.exit(5)

    if message is not None and not validate_printable(message):
        sys.stderr.write("pubsubclient: messages must only contain printable characters\n")
        sys.stderr.flush()
        sys.exit(6)

    sock, buf, display = connect(server_port, clientid)

    if message is not None:
        send_frame(sock, "PUBLISH", default_topic, message)
        time.sleep(0.05)
        sys.exit(0)

    client = Client(sock, buf, display, clientid, default_topic)
    client.run()


if __name__ == "__main__":
    main()
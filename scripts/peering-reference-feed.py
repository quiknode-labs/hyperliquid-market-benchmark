#!/usr/bin/env python3
"""Reference feed for the peering benchmark: tail a Hyperliquid node's
replica_cmds output and serve one compact NDJSON line per applied round to
TCP subscribers:

  {"r": <round>, "t": "<producer time>", "b": [[bundle_hash, first_sig_r, n_actions], ...]}

Every field is deterministic chain data — byte-identical on every Hyperliquid
node — so any node reproduces the same feed. Arrival timestamps are never
taken from this feed; the collector stamps arrivals live at its own socket and
uses the feed only to close samples after the fact. Standard library only.

Usage:
  HL_REPLICA_BASE=/path/to/hl/data/replica_cmds ./peering-reference-feed.py
"""
import json
import os
import socket
import threading
import time

BASE = os.environ.get("HL_REPLICA_BASE", "/root/hl/data/replica_cmds")
BIND = os.environ.get("FEED_BIND", "0.0.0.0")
PORT = int(os.environ.get("FEED_PORT", "9464"))
MAX_CLIENTS = int(os.environ.get("FEED_MAX_CLIENTS", "32"))

clients = []
clients_lock = threading.Lock()


def newest_file():
    try:
        sess = os.path.join(BASE, sorted(os.listdir(BASE))[-1])
        day = os.path.join(sess, sorted(os.listdir(sess))[-1])
        files = sorted(os.listdir(day), key=int)
        return os.path.join(day, files[-1])
    except (OSError, IndexError, ValueError):
        return None


def compact(raw):
    try:
        block = json.loads(raw)["abci_block"]
        out = {"r": block["round"], "t": block["time"], "b": []}
        for entry in block.get("signed_action_bundles", []):
            actions = entry[1].get("signed_actions", [])
            first_r = actions[0]["signature"]["r"] if actions else None
            out["b"].append([entry[0], first_r, len(actions)])
        return (json.dumps(out, separators=(",", ":")) + "\n").encode()
    except (KeyError, ValueError, TypeError, IndexError):
        return None


def broadcast(line):
    dead = []
    with clients_lock:
        for client in clients:
            try:
                client.sendall(line)
            except OSError:
                dead.append(client)
        for client in dead:
            clients.remove(client)
            try:
                client.close()
            except OSError:
                pass


def acceptor():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((BIND, PORT))
    server.listen(8)
    while True:
        conn, _addr = server.accept()
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with clients_lock:
            if len(clients) >= MAX_CLIENTS:
                conn.close()
                continue
            clients.append(conn)


def main():
    threading.Thread(target=acceptor, daemon=True).start()
    path = None
    handle = None
    buffer = b""
    while True:
        if handle is None:
            path = newest_file()
            if path is None:
                time.sleep(1)
                continue
            handle = open(path, "rb")
            # Live tail only at startup; rotated files are read from the start.
            handle.seek(0, 2)
            buffer = b""
        chunk = handle.read()
        if not chunk:
            # Only rotate once the current file is fully drained, so the tail
            # of the finished file is never dropped.
            latest = newest_file()
            if latest is not None and latest != path:
                handle.close()
                path = latest
                handle = open(path, "rb")
                buffer = b""
            else:
                time.sleep(0.02)
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            compacted = compact(line)
            if compacted:
                broadcast(compacted)


if __name__ == "__main__":
    main()

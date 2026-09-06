#!/usr/bin/env python3
"""Tier B peering exporter: continuous node-applied latency to Axiom.

The daemon sibling of peering-node-latency.py. It tails a node's replica_cmds
and, for every applied block, computes (disk-arrival wall clock - producer
timestamp): the node-applied boundary of docs/PEERING_TEST.md. It publishes
two event shapes to Axiom:

  node_applied_block   one row per consensus round (enables paired per-round
                       deltas across two nodes: same round, producer clock
                       cancels exactly)
  node_applied_window  one row per aligned 30s window with the quantile line
                       (p10/p50/p90/p95/p99), sample and negative counts, and
                       the observed round span

Rows carry provider/runner/cloud/region/metro identity like the collector's
rows, plus boundary="node-applied" so no query can silently mix these with
the collector's block-ready dataset. Comparisons across boundaries are
invalid; see the measurement-boundary table in docs/PEERING_TEST.md.

Standard library only. A disciplined clock is required for absolute numbers;
paired deltas survive a bad producer clock but not a bad local one.

When MEMPOOL_BASE is set (a node running with split_client_blocks), a second
tailer follows ~/hl/data/mempool_txs and emits, for every pre-consensus bundle
touching the BTC perp (asset index 0 in order/cancel/batchModify actions):

  node_mempool_btc_bundle   one row per bundle: the node's own first-seen
                            timestamp and the BTC action count. tx_hash makes
                            rows joinable across feeds and against inclusion.
  node_mempool_window       one row per aligned 30s window: bundle counts,
                            BTC share, and ingest lag (wall - stamped time).

Environment:
  HL_REPLICA_BASE       replica_cmds root (e.g. ~/hl/data/replica_cmds)
  MEMPOOL_BASE          optional mempool_txs root (e.g. ~/hl/data/mempool_txs)
  AXIOM_API_TOKEN       ingest token (required)
  AXIOM_URL             default https://api.axiom.co
  AXIOM_DATASET         default hyperliquid-market-benchmark
  NODE_PROVIDER         provider label for this node's sole gossip source
                        (e.g. quicknode | hydromancer) (required)
  BENCHMARK_RUNNER_ID   runner identity (e.g. oracle-nrt-02) (required)
  BENCHMARK_CLOUD / BENCHMARK_REGION / BENCHMARK_METRO
  EXPORTER_NETWORK      default mainnet
"""

import datetime
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

SCHEMA = "hyperliquid-market-benchmark-node-v1"
METRIC_KIND = "block_time_to_node_applied"
BOUNDARY = "node-applied"
WINDOW_SECONDS = 30
BLOCK_FLUSH_SECONDS = 5.0
# Bounded ingest backlog: newest wins, a dead Axiom never grows memory.
MAX_PENDING_EVENTS = 50_000


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None or value == "":
        print(f"missing required environment variable {name}", file=sys.stderr)
        sys.exit(2)
    return value


def parse_time(text):
    return (
        datetime.datetime.fromisoformat(text[:26])
        .replace(tzinfo=datetime.timezone.utc)
        .timestamp()
    )


def iso(ts):
    return (
        datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


class AxiomSink:
    """Non-blocking sink: producers push under a lock; a daemon thread posts.

    Ingest must NEVER share a thread with a tailer — a slow POST would stall
    reading and inflate every arrival timestamp taken afterwards (measured:
    seconds of skew). Producers only append; the flusher owns the network.
    """

    def __init__(self):
        self.url = os.environ.get("AXIOM_URL", "https://api.axiom.co").rstrip("/")
        self.dataset = os.environ.get("AXIOM_DATASET", "hyperliquid-market-benchmark")
        self.token = env("AXIOM_API_TOKEN")
        self.pending = []
        self.dropped = 0
        self.lock = threading.Lock()
        threading.Thread(target=self._flush_loop, daemon=True).start()

    def push(self, events):
        with self.lock:
            self.pending.extend(events)
            if len(self.pending) > MAX_PENDING_EVENTS:
                overflow = len(self.pending) - MAX_PENDING_EVENTS
                del self.pending[:overflow]
                self.dropped += overflow

    def _flush_loop(self):
        while True:
            time.sleep(BLOCK_FLUSH_SECONDS)
            with self.lock:
                batch = self.pending
                self.pending = []
            if not batch:
                continue
            body = json.dumps(batch).encode()
            request = urllib.request.Request(
                f"{self.url}/v1/datasets/{self.dataset}/ingest",
                data=body,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    response.read()
            except (urllib.error.URLError, OSError, TimeoutError) as error:
                print(
                    f"axiom ingest failed, requeueing batch: {error}",
                    file=sys.stderr,
                )
                self.push(batch)


def quantile(ordered, p):
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def window_row(identity, window_start, samples, negatives, round_min, round_max):
    ordered = sorted(samples)
    expected = (round_max - round_min + 1) if round_max >= round_min else 0
    return {
        "_time": iso(window_start + WINDOW_SECONDS),
        "event_type": "node_applied_window",
        "window_start": iso(window_start),
        "window_seconds": WINDOW_SECONDS,
        "sample_count": len(ordered),
        "negative_count": negatives,
        "round_min": round_min,
        "round_max": round_max,
        "missing_rounds": max(expected - len(ordered) - negatives, 0),
        "p10_ms": quantile(ordered, 0.10),
        "p50_ms": quantile(ordered, 0.50),
        "p90_ms": quantile(ordered, 0.90),
        "p95_ms": quantile(ordered, 0.95),
        "p99_ms": quantile(ordered, 0.99),
        "max_ms": ordered[-1],
        **identity,
    }


BTC_ASSET_INDEX = 0


def btc_action_count(bundle):
    count = 0
    for signed in bundle.get("signed_actions", []):
        action = signed.get("action", {})
        kind = action.get("type")
        if kind == "order":
            count += sum(
                1 for order in action.get("orders", []) if order.get("a") == BTC_ASSET_INDEX
            )
        elif kind == "cancel":
            count += sum(
                1 for cancel in action.get("cancels", []) if cancel.get("a") == BTC_ASSET_INDEX
            )
        elif kind == "batchModify":
            count += sum(
                1
                for modify in action.get("modifies", [])
                if modify.get("order", {}).get("a") == BTC_ASSET_INDEX
            )
    return count


def tail_hourly(base):
    """Follow the newest file of an hourly/{date}/{hour} tree, yielding lines."""

    def newest():
        try:
            day = os.path.join(base, sorted(os.listdir(base))[-1])
            return os.path.join(day, sorted(os.listdir(day), key=int)[-1])
        except (OSError, IndexError, ValueError):
            return None

    path = None
    handle = None
    buffer = b""
    while True:
        if handle is None:
            path = newest()
            if path is None:
                time.sleep(0.5)
                continue
            handle = open(path, "rb")
            handle.seek(0, 2)
            buffer = b""
        chunk = handle.read()
        if not chunk:
            latest = newest()
            if latest is not None and latest != path:
                handle.close()
                path = latest
                handle = open(path, "rb")
                buffer = b""
            time.sleep(0.02)
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line


def mempool_loop(base, identity):
    sink = AxiomSink()
    seen = {}
    window_start = None
    window_bundles = 0
    window_btc_bundles = 0
    window_btc_actions = 0
    window_lag_ms_max = 0.0

    def roll_window(now):
        nonlocal window_start, window_bundles, window_btc_bundles
        nonlocal window_btc_actions, window_lag_ms_max
        if window_start is not None and window_bundles:
            sink.push([{
                "_time": iso(window_start + WINDOW_SECONDS),
                "event_type": "node_mempool_window",
                "window_start": iso(window_start),
                "window_seconds": WINDOW_SECONDS,
                "bundle_count": window_bundles,
                "btc_bundle_count": window_btc_bundles,
                "btc_action_count": window_btc_actions,
                "ingest_lag_ms_max": window_lag_ms_max,
                **identity,
            }])
        window_start = now - (now % WINDOW_SECONDS)
        window_bundles = 0
        window_btc_bundles = 0
        window_btc_actions = 0
        window_lag_ms_max = 0.0

    roll_window(time.time())
    print(f"exporting BTC mempool first-seen from {base}", file=sys.stderr)
    for line in tail_hourly(base):
        now = time.time()
        if now - window_start >= WINDOW_SECONDS:
            roll_window(now)
        try:
            stamped, bundle = json.loads(line)
            first_seen = parse_time(stamped)
            tx_hash = bundle["tx_hash"]
        except (KeyError, ValueError, TypeError):
            continue
        if tx_hash in seen:
            continue
        seen[tx_hash] = now
        if len(seen) > 200_000:
            cutoff = now - 600
            for key in [k for k, v in seen.items() if v < cutoff]:
                del seen[key]
        window_bundles += 1
        window_lag_ms_max = max(window_lag_ms_max, (now - first_seen) * 1000.0)
        btc_actions = btc_action_count(bundle)
        if btc_actions == 0:
            continue
        window_btc_bundles += 1
        window_btc_actions += btc_actions
        sink.push([{
            "_time": iso(first_seen),
            "event_type": "node_mempool_btc_bundle",
            "tx_hash": tx_hash,
            "btc_action_count": btc_actions,
            **identity,
        }])


def main():
    base = os.path.expanduser(env("HL_REPLICA_BASE"))
    identity = {
        "schema": SCHEMA,
        "metric_kind": METRIC_KIND,
        "boundary": BOUNDARY,
        "provider": env("NODE_PROVIDER"),
        "runner": env("BENCHMARK_RUNNER_ID"),
        "cloud": os.environ.get("BENCHMARK_CLOUD", ""),
        "region": os.environ.get("BENCHMARK_REGION", ""),
        "metro": os.environ.get("BENCHMARK_METRO", ""),
        "network": os.environ.get("EXPORTER_NETWORK", "mainnet"),
        "exporter_version": 2,
    }
    sink = AxiomSink()
    mempool_base = os.environ.get("MEMPOOL_BASE", "")
    if mempool_base:
        threading.Thread(
            target=mempool_loop,
            args=(os.path.expanduser(mempool_base), dict(identity)),
            daemon=True,
        ).start()

    def newest():
        try:
            sess = os.path.join(base, sorted(os.listdir(base))[-1])
            day = os.path.join(sess, sorted(os.listdir(sess))[-1])
            return os.path.join(day, sorted(os.listdir(day), key=int)[-1])
        except (OSError, IndexError, ValueError):
            return None

    path = None
    handle = None
    buffer = b""
    window_start = None
    window_samples = []
    window_negatives = 0
    window_round_min = None
    window_round_max = None

    def roll_window(now):
        nonlocal window_start, window_samples, window_negatives
        nonlocal window_round_min, window_round_max
        if window_start is not None and (window_samples or window_negatives):
            sink.push([
                window_row(
                    identity,
                    window_start,
                    window_samples,
                    window_negatives,
                    window_round_min or 0,
                    window_round_max or 0,
                )
            ])
        window_start = now - (now % WINDOW_SECONDS)
        window_samples = []
        window_negatives = 0
        window_round_min = None
        window_round_max = None

    roll_window(time.time())
    print(
        f"exporting {identity['provider']} node-applied latency from {base}",
        file=sys.stderr,
    )
    while True:
        now = time.time()
        if now - window_start >= WINDOW_SECONDS:
            roll_window(now)
        if handle is None:
            path = newest()
            if path is None:
                time.sleep(0.1)
                continue
            handle = open(path, "rb")
            handle.seek(0, 2)
            buffer = b""
        chunk = handle.read()
        if not chunk:
            latest = newest()
            if latest is not None and latest != path:
                handle.close()
                path = latest
                handle = open(path, "rb")
                buffer = b""
            time.sleep(0.005)
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            arrival = time.time()
            try:
                block = json.loads(line)["abci_block"]
                round_number = int(block["round"])
                latency_ms = (arrival - parse_time(block["time"])) * 1000.0
                proposer = str(block["proposer"])[:10]
            except (KeyError, ValueError, TypeError):
                continue
            sink.push([{
                "_time": iso(arrival),
                "event_type": "node_applied_block",
                "round": round_number,
                "latency_ms": latency_ms,
                "proposer": proposer,
                **identity,
            }])
            if latency_ms < 0:
                window_negatives += 1
            else:
                window_samples.append(latency_ms)
            window_round_min = (
                round_number
                if window_round_min is None
                else min(window_round_min, round_number)
            )
            window_round_max = (
                round_number
                if window_round_max is None
                else max(window_round_max, round_number)
            )


if __name__ == "__main__":
    main()

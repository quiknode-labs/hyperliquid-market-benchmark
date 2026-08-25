#!/usr/bin/env python3
"""Tier B peering measurement: node-applied block latency from replica_cmds.

Capture mode — tail a node's replica_cmds and stamp each applied block at the
moment its line lands on disk, printing one JSON line per block:

  ./peering-node-latency.py capture /path/to/replica_cmds 600 > run.jsonl

Each line: {"r": round, "t": producer_unix_seconds, "w": arrival_unix_seconds,
"p": proposer_prefix}. Latency per block is (w - t): node-applied boundary,
which includes gossip transport plus this node's decode, execution, and
serialization. See docs/PEERING_TEST.md for what may and may not be compared
at this boundary.

Report mode — summarize one capture, or pair two simultaneous captures by
consensus round (paired deltas cancel the producer clock entirely):

  ./peering-node-latency.py report run.jsonl
  ./peering-node-latency.py report run_a.jsonl run_b.jsonl

Standard library only. Requires a disciplined clock (see the clock section of
docs/PEERING_TEST.md); absolute numbers from an unsynchronized box are void.
"""
import datetime
import json
import os
import sys
import time


def parse_time(text):
    return (
        datetime.datetime.fromisoformat(text[:26])
        .replace(tzinfo=datetime.timezone.utc)
        .timestamp()
    )


def capture(base, duration):
    def newest():
        sess = os.path.join(base, sorted(os.listdir(base))[-1])
        day = os.path.join(sess, sorted(os.listdir(sess))[-1])
        return os.path.join(day, sorted(os.listdir(day), key=int)[-1])

    path = newest()
    handle = open(path, "rb")
    handle.seek(0, 2)
    buffer = b""
    deadline = time.time() + duration
    while time.time() < deadline:
        chunk = handle.read()
        if not chunk:
            latest = newest()
            if latest != path:
                path = latest
                handle = open(path, "rb")
            time.sleep(0.005)
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            now = time.time()
            try:
                block = json.loads(line)["abci_block"]
                record = {
                    "r": block["round"],
                    "t": parse_time(block["time"]),
                    "w": now,
                    "p": block["proposer"][:10],
                }
            except (KeyError, ValueError, TypeError):
                continue
            print(json.dumps(record), flush=True)


def quantiles(values):
    ordered = sorted(values)

    def q(p):
        return ordered[min(len(ordered) - 1, int(p * len(ordered)))]

    return (
        f"n={len(ordered)} p10={q(0.10):.1f} p50={q(0.50):.1f} "
        f"p90={q(0.90):.1f} p99={q(0.99):.1f} max={ordered[-1]:.1f}"
    )


def load(path):
    return {row["r"]: row for row in (json.loads(l) for l in open(path))}


def report(paths):
    runs = [load(p) for p in paths]
    for path, run in zip(paths, runs):
        ages = [(row["w"] - row["t"]) * 1000 for row in run.values()]
        negatives = sum(1 for a in ages if a < 0)
        print(f"{path}: age_ms {quantiles(ages)} negatives={negatives}")
        if negatives:
            print(
                f"{path}: WARNING {negatives} blocks carry producer timestamps"
                " later than arrival — a validator clock anomaly or a bad"
                " local clock; absolute numbers are unreliable for this run"
            )
    if len(runs) == 2:
        common = sorted(set(runs[0]) & set(runs[1]))
        if not common:
            print("no common rounds — captures did not overlap")
            return
        deltas = [(runs[0][r]["w"] - runs[1][r]["w"]) * 1000 for r in common]
        first = sum(1 for d in deltas if d < 0)
        print(f"paired delta_ms (first - second, same round): {quantiles(deltas)}")
        print(
            f"first capture ahead on {first}/{len(common)} rounds"
            f" ({100 * first / len(common):.1f}%)"
        )


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "capture":
        capture(sys.argv[2], float(sys.argv[3]) if len(sys.argv) > 3 else 600.0)
    elif len(sys.argv) >= 3 and sys.argv[1] == "report":
        report(sys.argv[2:4])
    else:
        sys.stderr.write(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()

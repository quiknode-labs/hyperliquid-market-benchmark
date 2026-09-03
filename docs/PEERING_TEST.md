# The peering test

A standardized, reproducible procedure for measuring Hyperliquid gossip
peering delivery. Anyone can run it against any peering service they are
admitted to, with no involvement from the service operator or from Quicknode.
Results produced by this procedure are comparable; latency claims that do not
state a measurement boundary, a clock discipline, and a vantage are not.

Peering is, mechanically, a small thing: your own node lists the service's
peer addresses in `override_gossip_config.json`, and your node begins hearing
about consensus blocks earlier. Because the product is that simple, the test
can be too — and because the test is simple, it can be a standard.

## Measurement boundaries

Every latency number on this chain is "some boundary minus the block
producer's timestamp." Name the boundary or the number is meaningless.

| Boundary | What has happened | Who measures here |
| --- | --- | --- |
| **wire arrival** | last byte of the block's frames reached the socket | network engineers |
| **block-ready** | frames received, decompressed, validated, all referenced bundles assembled | this collector's `peering` dataset (Tier A) |
| **node-applied** | a full node has executed the block and serialized it to `replica_cmds` | Tier B below; most operators' log-based numbers |
| **application** | your own code has consumed the data | you |

The step from block-ready to node-applied is the node's own execution and
serialization time. It is identical no matter whose feed the node consumes,
so it belongs to the node, not to the feed. Comparing one party's block-ready
number against another party's node-applied number overstates the difference
by exactly that step; this document exists largely to prevent that comparison.

## Disclosure requirements

A published result must state:

1. **Boundary** — block-ready (Tier A) or node-applied (Tier B).
2. **Clock discipline** — the observer's NTP offset bound at capture time
   (for example `chronyc tracking` System time). Absolute latencies from an
   undisciplined clock are void. Paired A/B deltas (below) survive a bad
   clock; absolute values do not.
3. **Vantage and hardware** — region, provider, instance class, and whether
   the boxes in a comparison are identical.
4. **Service tier** — which admission class or product tier each measured
   endpoint served. A provider quietly admitting a known benchmark address to
   a premium tier invalidates the result.
5. **Duration and conditions** — capture length (minimum ten minutes or
   5,000 blocks, whichever is longer) and whether it spans a market-hours
   burst or a quiet period.

## Tier A — feed delivery (block-ready)

Tier A isolates the feed itself: it runs this collector's `peering` dataset,
which speaks the native gossip wire directly and stamps each round when its
ordering record and every referenced bundle have been received, decompressed,
and validated. Node execution never enters the number.

Producer timestamps and bundle identities come from a reference feed of
deterministic chain data, reproducible from any Hyperliquid node with
[`scripts/peering-reference-feed.py`](../scripts/peering-reference-feed.py) —
run it on your own node; do not trust anyone else's:

```bash
HL_REPLICA_BASE=/path/to/hl/data/replica_cmds ./scripts/peering-reference-feed.py
```

Then run the collector against the endpoint under test:

```bash
hyperliquid-market-benchmark \
  --dataset peering \
  --peering-endpoint <service host:port> \
  --peering-reference <your node host:9464>
```

The collector applies its own integrity gates: rounds with future producer
timestamps, gaps, or incomplete bundle sets are counted and excluded, never
guessed (see [METHODOLOGY.md](METHODOLOGY.md)).

### Tier A comparison — two services, one observer, identical rounds

Admission is the whole game: a peering service serves registered node
addresses only, so the collector's subscribe wire is accepted from an
admitted address and refused from any other. Once one observer is admitted by
two services, the strongest form of Tier A is to dial both from the same
process:

```bash
PEERING_PROVIDER=comparison \
QUICKNODE_PEERING_ENDPOINT=<quicknode host:port> \
HYDROMANCER_PEERING_ENDPOINT=<hydromancer host:port> \
PEERING_REFERENCE_FEED=<your node host:9464> \
hyperliquid-market-benchmark --dataset peering
```

Each consensus round becomes one two-source cohort: the same block, the same
clock, the same reference feed, the same block-ready boundary. Only rounds both
services delivered enter the latency distributions; a round one service never
delivered is a miss for that service, not a dropped round, and the
fastest-provider share is the strictly-first service per round. This removes
every box asymmetry that Protocol 2 below has to argue away, at the cost of
requiring dual admission. Disclose the observer's network distance to each
service endpoint (RTT); it is part of the result.

## Tier B — node-applied, or "anyone can referee"

Tier B needs nothing but a stock node and one standard-library script. It
measures at the node-applied boundary — the block's producer timestamp versus
the wall-clock moment the node writes the block to `replica_cmds`. It
includes your node's execution time, which is fine as long as both sides of a
comparison include the same node's execution time.

Point the node at the service under test:

```json
{
    "chain": "Mainnet",
    "root_node_ips": [{ "Ip": "<service peer IP>" }],
    "reserved_peer_ips": ["<service peer IP>"],
    "try_new_peers": false
}
```

`try_new_peers: false` matters: with it, the node hears gossip only from the
service under test, so the measurement cannot be contaminated by a lucky
public peer.

Capture and summarize:

```bash
./scripts/peering-node-latency.py capture /path/to/replica_cmds 600 > run.jsonl
./scripts/peering-node-latency.py report run.jsonl
```

### Protocol 1 — same box, sequential A/B

Test service A, then swap the override config to service B on the same box
and test again. Hardware, node version, and clock are held constant; the
compared captures contain different blocks, so run each leg long enough
(disclosure rule 5) and prefer adjacent time windows at similar market
conditions. This protocol produces comparable **absolute** distributions.

### Protocol 2 — two boxes, paired blocks

Run two nodes simultaneously, one per service, and pair captures by consensus
round:

```bash
./scripts/peering-node-latency.py report run_a.jsonl run_b.jsonl
```

The paired per-round delta compares identical blocks, which makes it immune
to producer-clock error entirely — but it inherits any hardware, provider,
or intra-metro distance difference between the two boxes. Disclose both
machines. Identical instance types in the same facility make this the
strongest protocol; mismatched boxes make it merely suggestive.

## Interpreting results

**Producer clocks are not trustworthy.** Block timestamps are stamped by the
proposing validator's clock. A single validator running fast inflates its own
blocks' timestamps, and because block time is monotonic, it also pins the
next several honest blocks to +1ms steps until real time catches up — so one
bad clock contaminates several times its own share of blocks. Symptoms: a
`negatives` warning from the report script, or long runs of blocks 1ms apart.
During such an episode, absolute latencies are unreliable for everyone;
paired deltas (Protocol 2) remain valid.

**Expect asymmetric tails.** Feed quality shows up at p90/p99 more than at
p50. Report the full quantile line, never a single average.

**The vanilla baseline.** To see what peering buys at all, run Tier B once
with a stock configuration (default public roots, no reserved peers,
`try_new_peers: true`) on the same box. This baseline varies with peer luck —
disclose that it is one draw, not a distribution over topologies.

## Reporting template

```text
boundary:        node-applied (Tier B, protocol 2)
services:        A = <name/tier>, B = <name/tier>
vantage:         2x <instance type>, <provider>, <metro>, same facility: yes/no
clock:           chrony |offset| < X ms on both boxes at capture
duration:        <minutes>, <n> blocks, market conditions: <quiet/burst>
absolute age ms: A p10/p50/p90/p99 = ...   B p10/p50/p90/p99 = ...
paired delta ms: p10/p50/p90 = ...; A first on N/M rounds (P%)
anomalies:       negatives=0/0; clamp runs observed: none
```

Quicknode operates both a peering service and this benchmark; that is
disclosed here as in [METHODOLOGY.md](METHODOLOGY.md), and it is exactly why
this procedure requires nothing from Quicknode to run.

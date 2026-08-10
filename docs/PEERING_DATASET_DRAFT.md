# Peering dataset — methodology draft (NOT LIVE)

Status: DRAFT for review, 2026-08-10. This document proposes the `peering`
dataset following the mempool precedent (one-source absolute measurement). It
becomes part of `METHODOLOGY.md`/`LIMITATIONS.md` only after review, and no
collector or dashboard work begins before the draft is approved.

CONFIDENTIALITY NOTE FOR REVIEW (strip before publication): public materials
describe the measured service as "the Quicknode peering service" — a network
endpoint — and say nothing about how it is implemented. This draft has been
scrubbed to that standard; keep it that way in every future edit.

## Measurement question

For a Hyperliquid consensus block, how old is the block's producer timestamp
when a gossip-peering subscriber has the complete, decoded block — ordering
data plus all referenced transaction content — available at the observer?

Peering measures one path: a plain TCP subscriber connected to a Quicknode
peering service endpoint, consuming Hyperliquid's native gossip wire stream
exactly as any peered node would. No other public source exposes the raw
gossip block stream with the producer timestamp, so peering, like mempool, is
an absolute delivery measurement and does not manufacture a cross-provider
cohort or fastest-provider result.

```text
peering: observer block-ready wall clock - block producer timestamp
```

## Receipt boundary

The collector maintains one long-lived TCP subscription and parses
Hyperliquid's gossip wire envelopes. A block is **ready** when:

- its ordering data has been received and parsed, yielding the consensus round
  and the producer timestamp; and
- every transaction bundle referenced by that round has been received,
  decompressed, and validated as decodable.

The observer wall-clock timestamp is captured immediately after those checks
and before the bounded event queue. Transport, framing, decompression, and
validation are therefore included, consistent with the other datasets. One
consensus round contributes at most one sample; rounds whose content never
completes within the five-second deadline are counted as incomplete and never
enter a latency distribution.

## Block reference data

Two per-round facts are not directly decodable from the wire stream: the
block's producer timestamp, and the mapping between a referenced transaction
bundle and the bundle's content (the network references bundles by a hash
whose preimage construction is not public). Both facts are, however,
**deterministic chain data**: every Hyperliquid node's replay output
(`replica_cmds`) records, for every round, the producer timestamp and each
referenced bundle's content — byte-identical on every node in the world.

The collector therefore consumes a small reference feed derived from a node's
replay output: per round, the producer timestamp and each referenced bundle's
leading signature value (a 32-byte field that is trivially readable in the
wire bundle's first record). Bundle completeness is established by matching
arrival-buffered bundles to the reference list by that signature value;
validation on 553 consecutive mainnet rounds showed the wire ordering data's
reference list agreeing with node replay output exactly (553/553).

Arrival timestamps are always captured live at the socket; the reference feed
only closes the sample after the fact, so feed lag delays reporting by under
a second and never distorts a latency value. Anyone can reproduce the
reference feed from any node's replay output; the methodology publishes its
format. If the feed stalls beyond the five-second cohort deadline, samples
are counted as incomplete rather than guessed.

## Service-tier disclosure

The measured subscription uses the anonymous public tier: any node operator
who discovers the endpoint through normal Hyperliquid gossip can connect with
no registration and no relationship with Quicknode. That tier applies a fixed,
disclosed release delay (currently 500 ms) to live data. The collector
connects exactly that way — as a stranger — so the measurement is what any
anonymous subscriber actually experiences, delay included; the delay is stated
on the dashboard, never subtracted. If a paid fast-lane series is ever
published alongside it, each series must carry its tier label and delay
constant explicitly.

## Operator conflict

Quicknode operates both the measured service and this benchmark. The peering
dataset must disclose that plainly wherever it is rendered. The mitigations
are the same as the rest of the benchmark: open-source collector,
deterministic event identities, provenance fields, and the clock gate — but
the conflict is stated, not argued away.

## Reused machinery (unchanged)

Rolling five-minute exact ring; 30-second publication; nearest-rank P50/P95/P99
with the 1,000-sample P99 gate; Chrony clock gate with the 5 ms error bound;
fsynced outbox with at-least-once delivery and deterministic `event_id`;
provenance fields. `outcome_count_scope` is `not-applicable` as with mempool:
no fastest/tie counts, and dashboards must not render a fastest-share view.

Capacity: Hyperliquid produces ~15 rounds/second; a 5-minute window holds
~4,500 complete-round samples — well inside the existing mempool-class limits
(75,000 rolling cohorts). The P99 gate is reached in under two minutes of
healthy stream.

## Proposed limitations entries

- Peering is a one-source Quicknode measurement; no public comparator exposes
  the raw gossip block stream. It cannot support a fastest-provider share.
- The block producer timestamp is supplied by the block's proposer. Proposer
  clock error affects every sample; the collector cannot correct for it.
- The reported latency includes the public tier's fixed release delay
  (disclosed on the dashboard). It is a measurement of that product tier.
- Quicknode operates both the measured service and the benchmark. The dataset
  is published with that disclosure; readers who require an adversarial
  measurement should reproduce it with the open-source collector from their
  own vantage.
- A block-ready sample says nothing about execution, application state, or
  order lifecycle latency — it measures delivery of raw consensus data only.

## Review decisions (2026-08-10)

1. **Measured path:** the anonymous public tier only (500 ms disclosed delay).
   A paid fast-lane series may be added later as a separately labeled series.
2. **Completeness/public gating:** the public page shows latency percentiles
   plus two service-health percentages — stream completeness (complete blocks
   ÷ rounds observed) and continuity (windows with an advancing stream,
   excluding Hyperliquid's marked network-upgrade maintenance windows).
   Percentiles are suppressed only when the collector itself fails its clock
   gate. No raw incomplete-blocks counter on the public page. The detailed
   service-health definition is maintained internally by Quicknode.
3. **Runner placement (revised 2026-08-10): global from day one.** Peering
   runs on the existing benchmark fleet across all metros (NRT, IAD, US-West,
   FRA, SIN), each runner its own labeled series per the existing convention —
   never mixed. The Tokyo series is the service floor; remote series show what
   a customer in that geography experiences of the (currently Tokyo-hosted)
   service. Mempool expands to the same global set at the same time. Rollout
   order: canary the new dataset on one Tokyo runner first (the repo's
   standard canary proof), then enable fleet-wide in one wave.

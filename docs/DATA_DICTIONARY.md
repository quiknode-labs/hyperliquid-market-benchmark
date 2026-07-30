# Axiom data dictionary

The collector emits one JSON object per active provider and coin for every
successfully prepared publication boundary. Rows for one window are an atomic
logical record. BBO and L2Book emit three rows with
`schema=hyperliquid-market-benchmark-v1` and
`metric_kind=event_to_canonical_book_ready`. Fills emits Quicknode and
Foundation rows with `schema=hyperliquid-market-benchmark-v2` and
`metric_kind=event_to_canonical_trade_ready`. Mempool emits one Quicknode row
with `schema=hyperliquid-market-benchmark-v3` and
`metric_kind=mempool_first_seen_to_bundle_ready`. Every dataset uses
`event_type=latency_window`.

Optional numeric fields are omitted when the exact value is unavailable. A
consumer must not replace an omitted value with zero.

## Envelope and provenance

| Field | Type | Meaning |
| --- | --- | --- |
| `_time` | RFC 3339 string | UTC-aligned publication boundary; same as `window_end`. |
| `schema` | string | Machine-readable event schema. |
| `event_type` | string | Always `latency_window` for chartable events. |
| `metric_kind` | string | `event_to_canonical_book_ready` for books, `event_to_canonical_trade_ready` for fills, or `mempool_first_seen_to_bundle_ready` for mempool. |
| `benchmark_version` | string | Collector package version. |
| `measurement_version` | string | Independent semantic measurement contract version. |
| `source_commit` | hex string or `unavailable` | Exact source revision embedded at build time. |
| `artifact_sha256` | 64-char hex or `unavailable` | Operator-verified binary digest supplied at runtime. |
| `event_id` | string | Deterministic schema/run/window/provider identity used for replay deduplication. |
| `window_id` | string | Public runner, dataset, coin, and aligned boundary identity shared by all expected rows. |
| `run_id` | UUID string | New process-run identity. A restart never silently merges runs. |

## Observer, source, and scope

| Field | Type | Meaning |
| --- | --- | --- |
| `provider` | enum | `quicknode`, `hyperliquid`, or `hydromancer`. |
| `protocol` | enum | `grpc` for Quicknode; `ws` for Foundation and Hydromancer. |
| `source` | enum | `quicknode-grpc`, `hyperliquid-ws`, or `hydromancer-ws`. |
| `dataset` | enum | `bbo`, depth-20 `l2book`, executed-trade `fills`, or filtered-bundle `mempool`. |
| `coin` | string | Uppercase Hyperliquid coin identifier, initially `BTC`. |
| `cloud` | string | Public observer cloud. Current deployment: `aws`, `gcp`, or `oracle`. |
| `region` | string | Logical comparison region: `iad`, `us-west`, `fra`, `nrt`, or `sin`. |
| `metro` | string | Physical observer metro; distinct from logical region in US West. |
| `runner` | string | Stable public observer ID such as `gcp-usw-lax-01`; never a private hostname. |
| `location` | string | Display-safe cloud/region/metro composite. |
| `runner_uptime_seconds` | integer | Monotonic process uptime at publication. |
| `window_end` | RFC 3339 string | End of the rolling distribution. |
| `window_seconds` | integer | Rolling distribution duration; currently 300. |
| `publish_interval_seconds` | integer | Nominal publication cadence; currently 30. |
| `cohort` | string | Exact dataset-specific source set. Mempool uses the one-source value `quicknode-grpc`. |
| `coverage_count_scope` | string | `rolling-window` for coverage counts. |
| `health_count_scope` | string | `run-lifetime` for cumulative health counters. |

## Rolling distribution

| Field | Type | Meaning |
| --- | --- | --- |
| `cohort_complete` | boolean | The rolling cohort passed state, queue, connection, freshness, and clock gates and has samples. |
| `sample_count` | integer | Exact complete cohorts in the five-minute ring; identical across all expected rows. |
| `min_ready_samples` | integer | P99 readiness threshold; currently 1,000. |
| `ready` | boolean | `cohort_complete` and `sample_count >= min_ready_samples`. |
| `readiness` | enum string | `ready`, `warming-up`, or an explicit connection/clock/integrity reason. |
| `p50_ms`, `p95_ms`, `p99_ms` | optional number | Empirical nearest-rank absolute latency. |
| `min_ms`, `max_ms`, `mean_ms` | optional number | Distribution extrema and arithmetic mean. |
| `standard_deviation_ms` | optional number | Population standard deviation of raw rolling latencies. |
| `p99_p50_spread_ms` | optional number | Auditable tail spread, P99 minus P50. |
| `latest_ms` | optional number | Most recently committed complete cohort latency for this source. |
| `latest_event_at` | optional RFC 3339 string | Hyperliquid timestamp for `latest_ms`. |
| `cohort_commit_delay_p99_ms` | optional number | P99 delay from first arrival until cohort settlement; not provider latency. |

## Non-overlapping outcome interval

The fields below are identical on all expected source rows. Consumers must
require three identical copies for books or two for fills and then count one
interval, not one per row.

Mempool has no provider race. It emits `outcome_count_scope=not-applicable`,
`outcome_interval_complete=false`, and zero for every outcome/fastest/tie count.

| Field | Type | Meaning |
| --- | --- | --- |
| `outcome_count_scope` | string | `non-overlapping-publication-interval` for comparison datasets or `not-applicable` for mempool. |
| `outcome_interval_id` | string | Deterministic run/runner/dataset/coin/start/end identity. |
| `outcome_interval_start`, `outcome_interval_end` | RFC 3339 string | Half-open outcome interval boundaries. |
| `outcome_interval_duration_ms` | integer | Actual elapsed boundary span. |
| `outcome_interval_complete` | boolean | Exact nominal duration after startup and all cohort-health gates passed. |
| `outcome_complete_cohort_count` | integer | Complete exact cohorts committed in this interval. |
| `outcome_quicknode_strict_fastest_count` | integer | Cohorts with Quicknode as the unique minimum absolute latency. |
| `outcome_foundation_strict_fastest_count` | integer | Cohorts with Foundation as the unique minimum. |
| `outcome_hydromancer_strict_fastest_count` | integer | Cohorts with Hydromancer as the unique minimum. |
| `outcome_tie_count` | integer | Cohorts whose minimum was shared by at least two paths. |
| `outcome_{source}_tied_fastest_count` | integer | For transparency, how often that active source participated in a tie. |

For every valid interval:

```text
Quicknode strict + Foundation strict + Hydromancer strict + ties
= outcome_complete_cohort_count
```

For fills, the Hydromancer count is zero and the valid denominator is Quicknode
strict + Foundation strict + ties. The invariant does not apply to mempool.

The first interval after a process starts and any interval spanning a rejected
outbox admission are incomplete. They are visible for diagnosis but excluded
from selected-range share.

## Coverage and integrity counters

| Field group | Scope | Meaning |
| --- | --- | --- |
| `observed_count` | run lifetime | Valid canonical source observations before exact matching. |
| `matched_count`, `missing_count`, `mismatch_count` | rolling five minutes | For comparison datasets, match to the Foundation reference. For mempool, `matched_count` is valid admitted Quicknode bundles; the one-source `missing_count` and `mismatch_count` remain zero. |
| `matched_total`, `missing_total`, `mismatch_total` | run lifetime | Cumulative counterparts. |
| `duplicate_count`, `late_count`, `orphaned_count` | run lifetime | Sealed repeats, arrivals after deadline, and non-reference revisions. |
| `negative_latency_count`, `future_timestamp_count` | run lifetime | Rejected wall-clock/event-time anomalies. |
| `signature_overflow_count` | run lifetime | Canonical revision cap exceeded for one base timestamp. |
| `complete_cohort_count` | run lifetime | Complete exact cohorts committed for the coin. |
| `pending_cohort_count` | instant | Candidate revisions awaiting settlement. |
| `state_eviction_count`, `rolling_eviction_count`, `coverage_eviction_count` | run lifetime | Bounded-state losses; each invalidates integrity for its declared window. |
| `reconnects`, `sequence_gaps`, `replay_count`, `replay_gap_count` | run lifetime/source | Transport continuity evidence. |
| `queue_drops` | run lifetime/source | Events dropped at the bounded receipt queue. |

`missing_count` includes the `mismatch_count` subset. The consumer should show
Foundation as “Reference set” and show other coverage as “Match to Foundation.”
For mempool, the consumer should instead label `matched_count` as valid bundles
and must not imply a Foundation comparison.

## Runtime clock

| Field | Type | Meaning |
| --- | --- | --- |
| `clock_healthy` | boolean | Runtime clock passed every readiness condition. |
| `clock_status` | enum string | `healthy` or an explicit unavailable, unsynchronized, stale, or error-bound reason. |
| `clock_source` | string | Currently `chrony`. |
| `clock_synchronized` | boolean | Valid external Chrony source, stratum, and normal leap state. |
| `clock_offset_ms` | optional number | Signed Chrony system-time offset. |
| `clock_error_bound_ms` | optional number | Conservative offset + dispersion + half root-delay estimate. |
| `clock_max_offset_ms` | number | Configured conservative error-bound readiness limit; production uses 5. |
| `clock_checked_at` | RFC 3339 string | Clock sample wall time. |
| `clock_check_age_ms` | optional integer | Clock sample age when the event was prepared. |

## Connection and Axiom delivery health

| Field group | Meaning |
| --- | --- |
| `connection_state`, `last_message_at`, `last_message_age_ms` | Per-source live connection and freshness. |
| `ingest_pending_batches`, `ingest_pending_bytes` | Durable outbox backlog. |
| `ingest_attempts`, `ingest_batches_succeeded`, `ingest_batches_failed`, `ingest_batches_dropped` | Axiom batch lifecycle. |
| `ingest_events_succeeded`, `ingest_events_dropped` | Event delivery totals. |
| `ingest_outbox_write_failures`, `ingest_outbox_delete_failures`, `ingest_outbox_cap_rejections` | Local persistence integrity. |
| `ingest_last_success_at`, `ingest_last_success_age_ms` | Most recent complete Axiom acknowledgement. |

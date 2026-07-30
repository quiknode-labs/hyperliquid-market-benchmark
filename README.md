# Hyperliquid market benchmark

## What this is

A continuous, open-source latency observer for Hyperliquid BBO, depth-20
L2Book, executed BTC trades, and BTC pre-consensus mempool delivery. Book
datasets compare three paths on the same exact event:

- Quicknode gRPC
- Hyperliquid Foundation WebSocket
- Hydromancer WebSocket

The `fills` dataset compares Quicknode gRPC with the Hyperliquid Foundation
`trades` WebSocket. Hydromancer is intentionally outside that first fills
comparison because its public API exposes user-scoped fills, not the same
market-wide trade feed.

The `mempool` dataset measures the Quicknode public gRPC path on its own. It
subscribes to `MEMPOOL_TXS` through `StreamData` with the production
`coin=BTC` server-side filter. There is no equivalent public source with the
same pre-consensus bundle and first-seen timestamp, so mempool is an absolute
delivery measurement rather than a provider race.

The benchmark reports absolute delivery latency, not a race delta, synthetic
score, order-to-fill latency, or matching-engine execution time:

```text
books: local canonical-book-ready wall clock - Hyperliquid event timestamp
fills: local canonical-trade-ready wall clock - Hyperliquid trade timestamp
mempool: local decoded-bundle-ready wall clock - embedded first-seen timestamp
```

Each path is timestamped after its dataset-specific transport decoding and
validation. Books and trades also complete numeric normalization and canonical
construction before that boundary. A book is admitted only when all three book
paths produced the same canonical content. A trade is admitted only when
Quicknode and Foundation produced the same coin, timestamp, trade ID, taker
side, price, size, hash, buyer, and seller. For mempool, the complete JSON bundle
must decode and contain a valid first-seen timestamp, transaction hash, and
non-empty signed-action list before the local receipt timestamp is captured.

The [deployed dashboard](https://hyperliquid-market-benchmark-web.quicknode.workers.dev)
is designed for provider evaluation. This repository is the auditable producer
behind that data. Start with
[the methodology](docs/METHODOLOGY.md) and
[the limitations](docs/LIMITATIONS.md) before interpreting a chart.

## What is measured

Every process maintains an exact five-minute ring of admitted observations and
publishes one logical Axiom window every 30 seconds: three matched rows for a
book dataset, two matched rows for fills, and one valid-bundle row for mempool.
Each source row contains empirical nearest-rank P50, P95, and P99 plus coverage,
health, clock, provenance, and gap counters. Comparison datasets also repeat one
exact non-overlapping publication-interval outcome record so selected-range
fastest shares can be summed without double counting. Equal millisecond
latencies are ties; they are never assigned to a provider. Mempool has no
fastest-share record because it has no equivalent comparison source.

These scopes are deliberately different:

- headline quantiles: the latest exact five-minute rolling window;
- chart points: the selected quantile from successive five-minute windows;
- fastest share: exact complete comparison cohorts in non-overlapping 30-second
  intervals; not applicable to mempool.

The dashboard does not relabel a five-minute P99 as a one-hour P99 and does not
average stored percentiles into a new percentile.

## Honesty rules this codebase enforces

- A latency enters a comparison only when every source in that dataset produced
  the same canonical content for the same event or trade.
- A mempool latency enters its single-source distribution only after the
  filtered bundle is fully decoded and validated; raw payloads and transaction
  hashes are never published to Axiom.
- P50, P95, and P99 are exact nearest-rank values over the collector's raw
  five-minute sample ring; the dashboard never averages stored percentiles.
- Missing, late, stale, or integrity-invalid data remains an explicit gap.
- Below-threshold P99 is visible and labeled warming rather than hidden or ranked
  as equally trustworthy.
- Clock health, source commit, and artifact digest travel with every trustworthy
  window.

## Repository layout

This is the public benchmark source repository. The branded dashboard lives in
the private `hyperliquid-market-benchmark-web` repository, and shared inventory
plus both product playbooks live in private `benchmark-fleet-ops`.

```text
src/       Rust collector, providers, cohort admission, rollups, and outbox
proto/     Quicknode Hyperliquid gRPC contract used by the collector
docs/      methodology, limitations, operations, and data dictionary
scripts/   public-surface and release checks
.github/   release, provenance, dependency, and public-surface workflows
```

## Quick start

Rust 1.94.1 is pinned in `rust-toolchain.toml`. The Protobuf compiler is bundled
for the build; no separate `protoc` installation is needed.

Tagged Linux amd64 releases are static musl executables. The published raw
binary, SHA-256 file, and GitHub build-provenance attestation identify the exact
bytes intended for every observer.

### Credentials

Set credentials in the process environment. There are intentionally no CLI
flags for secrets, and the Quicknode tenant endpoint has no source default.

```bash
export HYDROMANCER_API_KEY='<secret>'
export QUICKNODE_HYPERLIQUID_TOKEN='<secret>'
export QUICKNODE_HYPERLIQUID_GRPC_URL='https://your-endpoint.hype-mainnet.quiknode.pro:10000'
export AXIOM_API_TOKEN='<dataset-ingest-token>'

export BENCHMARK_RUNNER_ID='aws-nrt-01'
export BENCHMARK_CLOUD='aws'
export BENCHMARK_REGION='nrt'
export BENCHMARK_METRO='nrt'
export BENCHMARK_SOURCE_COMMIT="$(git rev-parse HEAD)"
```

### Run

Run each dataset in a separate process so its load and failure state remain
isolated:

```bash
cargo run --release --locked -- --dataset bbo --coins BTC
cargo run --release --locked -- --dataset l2book --coins BTC
cargo run --release --locked -- --dataset fills --coins BTC
cargo run --release --locked -- --dataset mempool --coins BTC
```

The `mempool-bundle-ready-v1` measurement contract requires exactly
`--coins BTC`; other datasets retain the general comma-separated coin option.

`AXIOM_DATASET` defaults to `hyperliquid-market-benchmark`. The token must be a
dataset-scoped ingest token; personal/query tokens are rejected. Persistent
outbox data defaults to
`/var/lib/hyperliquid-market-benchmark/{bbo|l2book|fills|mempool}`.
Use `BENCHMARK_OUTBOX_DIR` for an unprivileged local run.

Each provider reconnects independently, so one unavailable source produces an
explicit incomplete cohort without stopping the other streams. Every
30-second window must be durably admitted within 20 seconds; if local
publication stalls, the process exits so its service supervisor can restart it.

## Public observer identity

Telemetry contains a stable public runner ID, cloud, logical comparison region,
and physical metro. It never contains an SSH hostname, IP address, or internal
inventory name. West-coast observers use the explicit model below:

| Public runner | Cloud | Logical region | Physical metro |
| --- | --- | --- | --- |
| `aws-usw-sjc-01` | AWS | `us-west` | SJC |
| `gcp-usw-lax-01` | GCP | `us-west` | LAX |
| `oracle-usw-sjc-01` | Oracle | `us-west` | SJC |

The current fleet contains AWS, GCP, and Oracle observers in five logical
regions. Adding a future source requires a measurement-version change rather
than a hidden compatibility path.

## Checks

```bash
cargo fmt --all --check
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
```

## Documentation

- [Methodology](docs/METHODOLOGY.md)
- [Limitations](docs/LIMITATIONS.md)
- [Operations](docs/OPERATIONS.md)
- [Data dictionary](docs/DATA_DICTIONARY.md)

## License

The collector is licensed under Apache-2.0. Provider names and trademarks belong
to their respective owners. A separate license will accompany any bulk public
data export; the software license does not silently define data-reuse terms.

# Hyperliquid market benchmark

A continuous, open-source latency observer for Hyperliquid BBO and depth-20
L2Book data. It compares three currently available paths on the same exact book
events:

- Quicknode gRPC
- Hyperliquid Foundation WebSocket
- Hydromancer WebSocket

The benchmark reports absolute latency, not a race delta and not a synthetic
score:

```text
local canonical-book-ready wall clock - Hyperliquid event timestamp
```

Each path is timestamped at the same semantic boundary: transport decoding,
validation, numeric normalization, and canonical book construction have all
completed. A book is admitted only when all three paths produced the same
canonical content for the same coin and event timestamp.

The [deployed dashboard](https://hyperliquid-markets.quicknode.workers.dev/hyperliquid-markets)
is designed for provider evaluation. The intended branded route is
`https://quicknode.com/hyperliquid-markets`. This repository is
the auditable producer behind that data. Start with
[the methodology](docs/METHODOLOGY.md) and
[the limitations](docs/LIMITATIONS.md) before interpreting a chart.

## What is published

Every process maintains an exact five-minute ring of complete three-source
cohorts and publishes one logical three-row Axiom window every 30 seconds.
Each source row contains empirical nearest-rank P50, P95, and P99 plus coverage,
health, clock, provenance, and gap counters. It also repeats one exact
non-overlapping publication-interval outcome record so selected-range fastest
shares can be summed without double counting. Equal millisecond latencies are
ties; they are never assigned to a provider.

These scopes are deliberately different:

- headline quantiles: the latest exact five-minute rolling window;
- chart points: the selected quantile from successive five-minute windows;
- fastest share: exact complete cohorts in non-overlapping 30-second intervals.

The dashboard does not relabel a five-minute P99 as a one-hour P99 and does not
average stored percentiles into a new percentile.

## Run locally

Rust 1.94.1 is pinned in `rust-toolchain.toml`. The Protobuf compiler is bundled
for the build; no separate `protoc` installation is needed.

Tagged Linux amd64 releases are static musl executables. The published raw
binary, SHA-256 file, and GitHub build-provenance attestation identify the exact
bytes intended for every observer.

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

Run BBO and L2Book in separate processes so their load and failure state remain
isolated:

```bash
cargo run --release --locked -- --dataset bbo --coins BTC
cargo run --release --locked -- --dataset l2book --coins BTC
```

`AXIOM_DATASET` defaults to `hyperliquid-market-benchmark`. The token must be a
dataset-scoped ingest token; personal/query tokens are rejected. Persistent
outbox data defaults to `/var/lib/hyperliquid-market-benchmark/{bbo|l2book}`.
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

## Reproduce the checks

```bash
cargo fmt --all --check
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
```

See [operations](docs/OPERATIONS.md) for runtime clock and canary checks and
[the data dictionary](docs/DATA_DICTIONARY.md) for the complete event contract.

## License

The collector is licensed under Apache-2.0. Provider names and trademarks belong
to their respective owners. A separate license will accompany any bulk public
data export; the software license does not silently define data-reuse terms.

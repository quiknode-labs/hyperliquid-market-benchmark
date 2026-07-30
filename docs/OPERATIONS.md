# Operations

This repository intentionally omits Quicknode's private SSH inventory and fleet
automation. Operators can deploy the single binary with their own configuration
management system.

## Build an immutable artifact

Build from a clean commit and embed that identity:

```bash
export BENCHMARK_SOURCE_COMMIT="$(git rev-parse --verify HEAD)"
cargo build --release --locked
sha256sum target/release/hyperliquid-market-benchmark
```

Cross-build or compile inside a Linux amd64 environment when the target fleet is
x86-64. Approve one SHA-256 and deploy the exact same bytes to canary and rollout
hosts. Supply that SHA through `BENCHMARK_ARTIFACT_SHA256` so it appears in every
event.

## Runtime configuration

Required secrets:

- `HYDROMANCER_API_KEY` (required for BBO and L2Book; not required for fills or mempool)
- `QUICKNODE_HYPERLIQUID_TOKEN`
- `AXIOM_API_TOKEN` (dataset-scoped ingest token)

Required public Quicknode endpoint configuration:

- `QUICKNODE_HYPERLIQUID_GRPC_URL`, an HTTPS
  `*.hype-mainnet.quiknode.pro:10000` origin. Direct, internal, plaintext,
  testnet, and wrong-port origins are refused.

Required public observer configuration:

- `BENCHMARK_RUNNER_ID`
- `BENCHMARK_CLOUD`
- `BENCHMARK_REGION`
- `BENCHMARK_METRO`

Keep BBO, L2Book, fills, and mempool in separate service processes. Run as an
unprivileged user, restrict the writable filesystem to the outbox directory,
and set a restart policy. Never pass secrets in CLI arguments.

## Clock prerequisite

Use Chrony, verify `NTPSynchronized=yes`, and ensure `chronyc -c tracking`
reports an external reference, a normal leap state, and a conservative error
bound at or below 5 ms. The collector computes that bound as absolute
system-time offset plus root dispersion plus half root delay. It repeats the
check at publication time and withholds ready status if it fails. Treat repeated
clock-gate failures as invalid benchmark data, not as a provider latency
regression.

## Canary proof

Before a fleet rollout, prove `bbo`, `l2book`, `fills`, and the opt-in `mempool`
process on one observer:

1. every process remains active and keeps its expected persistent connections
   (three for books, two for fills, one for mempool);
2. runtime clock health is valid;
3. outbox files are acknowledged and removed without drops;
4. Axiom contains exactly three provider rows per book window, two per fills
   window, and one per mempool window;
5. all rows agree on runner, run, window, interval outcome, and sample count;
6. P50 <= P95 <= P99 and no negative/zero placeholder is synthesized;
7. a deliberate credential failure is visible and recovers without data
   corruption; and
8. the public consumer shows gaps rather than bridging invalid windows.

Roll out the identical artifact in small batches and verify the cloud/region/
metro matrix after each batch.

## Bounded failure behavior

The event queue, cohort state, rolling rings, protocol message sizes, Axiom
response body, and persistent outbox are bounded. Queue drops, evictions,
reconnects, sequence gaps, replay gaps, outbox write failures, and cap rejections
are emitted. An operator should alert on these integrity signals even when the
process itself remains active.

Each BBO, L2Book, fills, or mempool process must own a distinct outbox
directory. The collector enforces this with an operating-system lock. A
permanently rejected oldest batch intentionally blocks later batches to
preserve strict FIFO order; alert on a non-draining backlog instead of deleting
it. Lowering configured caps below an existing backlog prevents startup until
the previous caps are restored or the backlog is handled deliberately.

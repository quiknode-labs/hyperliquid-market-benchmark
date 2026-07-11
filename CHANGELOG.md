# Changelog

All notable changes to the collector and measurement contract are documented
here. Versions follow Semantic Versioning; measurement-contract changes are
also identified independently in emitted events.

## 0.1.2 - 2026-07-11

- Treat a stream message observed a few milliseconds after the publication
  clock snapshot as age zero, eliminating false freshness gaps at aligned
  30-second boundaries.
- Refuse direct, internal, testnet, wrong-port, or plaintext gRPC origins. The
  Quicknode source now accepts only a public mainnet
  `*.hype-mainnet.quiknode.pro:10000` endpoint over HTTPS.

## 0.1.1 - 2026-07-11

- Publish a static Linux amd64 musl artifact that does not depend on the fleet's
  glibc version.
- Expose `--version` so deployment can execute the artifact as its service user
  before enabling either collector process.

## 0.1.0 - 2026-07-11

- Initial public three-source Hyperliquid BBO and depth-20 L2Book benchmark.
- Absolute event-to-canonical-book-ready P50, P95, and P99.
- Exact three-source cohorts with a five-second admission deadline.
- Five-minute rolling distributions published every 30 seconds.
- Non-overlapping interval fastest/tie outcomes for selected-range evidence.
- Runtime Chrony synchronization and offset readiness gate.
- Persistent bounded Axiom outbox with deterministic event identities.

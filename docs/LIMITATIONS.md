# Limitations

- The observer measures upstream timestamp to application-visible book, trade,
  or mempool-bundle readiness at the runner. It is not a pure network RTT,
  server processing time, order-to-fill latency, or exchange matching-engine
  latency measurement.
- Hyperliquid supplies the event timestamps. Any error or semantic change in
  those timestamps affects every path.
- For comparison datasets, only complete canonical events that match the
  Foundation-defined reference set on every path enter latency distributions.
  Coverage and mismatch counters are necessary context for that conditional
  sample.
- BBO and L2Book include Quicknode gRPC, Foundation WebSocket, and Hydromancer
  WebSocket. Fills initially includes Quicknode gRPC and Foundation WebSocket;
  Hydromancer's user-scoped fills API is not treated as an equivalent
  market-wide trade feed.
- A fills sample measures delivery of an already executed public trade. It says
  nothing about when a customer's order was submitted, acknowledged, or filled.
- Mempool is a one-source Quicknode measurement because no equivalent public
  feed exposes the same pre-consensus bundle and first-seen timestamp. It cannot
  support a fastest-provider share.
- A BTC mempool filter admits the production bundle containing a matching
  action; it does not normalize the response into one action. The metric is
  bundle-ready latency and does not claim per-action latency or remaining time
  before consensus.
- Mempool latency includes the public endpoint, routing, TLS, gRPC, Protobuf,
  full JSON decoding, and validation at the observer. It cannot isolate any one
  of those components.
- P50/P95/P99 are exact for each rolling five-minute window. The dashboard does
  not claim that an aggregate of those windows is an exact selected-range
  percentile.
- For comparison datasets, selected-range fastest share uses non-overlapping
  cohorts and integer milliseconds. Legitimate equal minima are reported as
  ties.
- Runtime clock gating reduces observer-clock error; it cannot prove that every
  upstream event timestamp is correct.
- Axiom delivery is at least once. Public consumers must collapse equivalent
  deterministic event IDs and reject an ID whose eligibility- or output-relevant
  fields have conflicting variants before aggregating data.
- Cash-market overlays are contextual annotations. They do not establish that a
  market open caused a latency change. Holidays and half days require a separate
  verified calendar and are not inferred from weekday templates.
- Hyperliquid maintenance is represented only by dated announcements. A
  provisional marker is not a recurring rule and must not be read as confirmed
  downtime.
- Public runner IDs identify cloud, logical region, and physical metro, not an
  exact host, availability zone, IP address, or customer tenant.

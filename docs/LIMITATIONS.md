# Limitations

- The observer measures event timestamp to application-visible canonical book
  readiness at the runner. It is not a pure network RTT, server processing time,
  or exchange matching-engine latency measurement.
- Hyperliquid supplies the event timestamps. Any error or semantic change in
  those timestamps affects every path.
- Only complete books that match the Foundation-defined reference set on all
  three paths enter latency distributions. Coverage and mismatch counters are
  necessary context for that conditional sample.
- The current comparison includes Quicknode gRPC, Foundation WebSocket, and
  Hydromancer WebSocket.
- P50/P95/P99 are exact for each rolling five-minute window. The dashboard does
  not claim that an aggregate of those windows is an exact selected-range
  percentile.
- Selected-range fastest share uses non-overlapping cohorts and integer
  milliseconds. Legitimate equal minima are reported as ties.
- Runtime clock gating reduces observer-clock error; it cannot prove that every
  upstream event timestamp is correct.
- Axiom delivery is at least once. Public consumers must collapse identical
  deterministic event IDs and reject an ID whose payload has conflicting
  variants before aggregating data.
- Cash-market overlays are contextual annotations. They do not establish that a
  market open caused a latency change. Holidays and half days require a separate
  verified calendar and are not inferred from weekday templates.
- Hyperliquid maintenance is represented only by dated announcements. A
  provisional marker is not a recurring rule and must not be read as confirmed
  downtime.
- Public runner IDs identify cloud, logical region, and physical metro, not an
  exact host, availability zone, IP address, or customer tenant.

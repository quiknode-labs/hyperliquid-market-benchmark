# Ordering-frame fixtures (Hyperliquid mainnet gossip)

Raw gossip frames exactly as received on the wire (`[u32 BE body_len][u8 kind][body]`; kind
`0x01` = LZ4, decompressed payload tag `0x00` = ordering). Network-public data, captured by a
Quicknode relay. They pin the ordering-record layout across two protocol changes:

| file | frames | what it pins |
|---|---|---|
| `frame-pre-upgrade-r1446876004.bin` | 1 | last layout before the 2026-09-13 ~08:37Z upgrade (record leads with `fc round`) |
| `frames-20260914.bin` | 5 | post-upgrade layout; frame rounds 1448491973..=1448491977, parent slots of 1448491972..=76 carry 2,2,2,2,1 bundles (checked against a node's `replica_cmds`) |
| `frames-empty-parent.bin` | 4 | frame rounds 1447134309..=1447134312; the second frame's parent round produced no block (0 hashes) |
| `frames-fb-tail.bin` | 2 | the certificate tail's second field as `fb u16` (about 1 frame in 16,000) |
| `frames-tc-tail.bin` | 5 | proposals that follow a timed-out round carry its timeout certificate in the tail (2026-09-21); the record's round is the timed-out round + 1 |

Layout (see `src/ordering.rs`): since 2026-09-13 a record is
`[signers][proposer: 20][count][32B × count][fc u32][varint][tc_count][fc timed_out × tc][small][fc activation][fc qc_parent]`;
before it, `[proposer: 20][fc round][count][32B × count]`.

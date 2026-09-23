//! Ordering-record parser for Hyperliquid gossip `tag 0x00` payloads, in both layout epochs.
//!
//! Ported from the Quicknode relay's parser, where it is validated against mainnet node output
//! (`replica_cmds`). Until this port the collector located records only by a leading `0xfc round`
//! marker, which the 2026-09-13 ~08:37Z upgrade removed: from then on it recognised about one
//! round in four (peering samples per 5-minute window fell from ~2,570 to ~660), and the rest were
//! counted as gaps.
//!
//! A tag-0x00 frame is the PROPOSAL for round R and carries the record of round R-1 in a fixed
//! "parent slot" and its own record for R further on:
//!
//! ```text
//! [0x00][8 × varint u64][varint signers][PARENT SLOT: record R-1][… quorum material …][record R]
//! ```
//!
//! Before the upgrade every record led with its round:
//! `[proposer: 20][fc round][count][32B × count]`. Since the upgrade the round closes the record's
//! certificate tail instead:
//!
//! ```text
//! [proposer: 20][count][32B × count][fc u32][varint][tc_count][fc timed_out × tc_count][small…][fc activation][fc qc_parent]
//! ```
//!
//! The tail's second field is a varint (`fb u16` on ~1 frame in 16,000); a proposal that follows
//! a timed-out round carries that round's timeout certificate, and its round is then the highest
//! timed-out round + 1 rather than `qc_parent + 1`. The parent slot is parsed structurally; the own
//! record is found by its unique tail and verified forward; anything ambiguous yields the parent
//! slot alone rather than a guess. Fixtures: `tests/fixtures/ordering/`.

/// bincode-fork varint: <0xfb single byte; 0xfb=u16, 0xfc=u32, 0xfd=u64 (LE payload).
/// Returns (value, next_index).
pub fn read_varint(d: &[u8], i: usize) -> Option<(u64, usize)> {
    let m = *d.get(i)?;
    match m {
        0xfb => Some((
            u16::from_le_bytes([*d.get(i + 1)?, *d.get(i + 2)?]) as u64,
            i + 3,
        )),
        0xfc => Some((
            u32::from_le_bytes([
                *d.get(i + 1)?,
                *d.get(i + 2)?,
                *d.get(i + 3)?,
                *d.get(i + 4)?,
            ]) as u64,
            i + 5,
        )),
        0xfd => {
            let b = d.get(i + 1..i + 9)?;
            let mut a = [0u8; 8];
            a.copy_from_slice(b);
            Some((u64::from_le_bytes(a), i + 9))
        }
        0xfe | 0xff => None, // u128 / reserved — not expected for counts
        v => Some((v as u64, i + 1)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub proposer: [u8; 20],
    pub round: u32,
    pub tx_hashes: Vec<[u8; 32]>,
}

/// After the tag byte, the eight u64 varints and the small signer-count varint: the index of the
/// parent slot's 20-byte proposer. None when the payload is not a tag-0x00 ordering envelope.
fn slot_start(payload: &[u8]) -> Option<usize> {
    if payload.first() != Some(&0x00) {
        return None;
    }
    let mut p = 1usize;
    for _ in 0..8 {
        let (_, next) = read_varint(payload, p)?;
        p = next;
    }
    let (_signers, next) = read_varint(payload, p)?;
    Some(next)
}

/// Timeout certificates one record's tail may carry (one per timed-out round its proposal steps
/// over). Mainnet has shown exactly one on every such frame (module doc); the cap bounds the
/// structural search, it is not a protocol limit we know.
pub const TAIL_TC_MAX: usize = 8;

/// Longest certificate tail `record_tail` accepts: `fc u32` (5) + a varint (≤ 9) + a count byte
/// (1) + up to `TAIL_TC_MAX` timeout certificates (5 each) + up to 4 small bytes + `fc activation`
/// (5) + `fc qc_parent` (5). Sizes the prefix `parent_record_prefix` decodes.
pub const RECORD_TAIL_MAX: usize = 5 + 9 + 1 + 5 * TAIL_TC_MAX + 4 + 5 + 5;

/// A record's certificate tail, read out (module doc, LAYOUT EPOCHS + TIMEOUT CERTIFICATES).
struct Tail {
    activation: u32,
    /// The record's own round: highest timed-out round + 1, else `qc_parent + 1`.
    round: u32,
    /// Index one past the tail.
    end: usize,
}

/// The certificate tail that closes every record since the 2026-09-13 upgrade:
/// `[fc u32][varint][tc_count][fc timed_out_round × tc_count][small…(1–4 bytes)][fc activation]
/// [fc qc_parent]` starting at `at`. The second field is `fc u32` on most frames and `fb u16` on
/// ~1/16k (module doc); a single-byte value would read as one more small byte, which the loop
/// below absorbs. A count byte that is not followed by `fc` is read as a small byte (the plain
/// layout: count 0 then one byte) — the same bytes the parser accepted before 2026-09-21.
fn parse_tail(payload: &[u8], at: usize) -> Option<Tail> {
    if *payload.get(at)? != 0xfc {
        return None;
    }
    let second = *payload.get(at + 5)?;
    if second == 0xfe || second == 0xff {
        return None;
    }
    let (_, mut j) = read_varint(payload, at + 5)?;
    let u32_at = |i: usize| -> Option<u32> {
        Some(u32::from_le_bytes([
            *payload.get(i)?,
            *payload.get(i + 1)?,
            *payload.get(i + 2)?,
            *payload.get(i + 3)?,
        ]))
    };
    // TIMEOUT CERTIFICATES: a small count, then one `fc u32` round per certificate.
    let mut timeout_rounds = Vec::new();
    if let Some((count, k)) = read_varint(payload, j)
        && (1..=TAIL_TC_MAX as u64).contains(&count)
        && payload.get(k) == Some(&0xfc)
    {
        {
            let mut k = k;
            for _ in 0..count {
                if *payload.get(k)? != 0xfc {
                    return None;
                }
                timeout_rounds.push(u32_at(k + 1)?);
                k += 5;
            }
            j = k;
        }
    }
    let mut smalls = 0usize;
    while *payload.get(j)? < 0xfb && smalls < 4 {
        j += 1;
        smalls += 1;
    }
    if smalls == 0 || *payload.get(j)? != 0xfc || *payload.get(j + 5)? != 0xfc {
        return None;
    }
    let activation = u32_at(j + 1)?;
    let qc_parent = u32_at(j + 6)?;
    let round = timeout_rounds
        .iter()
        .copied()
        .max()
        .unwrap_or(qc_parent)
        .wrapping_add(1);
    Some(Tail {
        activation,
        round,
        end: j + 10,
    })
}

/// `parse_tail` reduced to what the record locators key on:
/// (activation round, the record's own round, index one past the tail).
fn record_tail(payload: &[u8], at: usize) -> Option<(u32, u32, usize)> {
    parse_tail(payload, at).map(|t| (t.activation, t.round, t.end))
}

/// The parsed parent slot plus what the own-record locator needs.
struct Slot {
    header: BlockHeader,
    /// pre-2026-09-13 layout (leading `fc round`); `activation`/`end` are unused then
    legacy: bool,
    activation: u32,
    /// index one past the slot (its tail in the new layout, its hashes in the old one)
    end: usize,
}

/// Bundle-hash count a STRUCTURALLY parsed record may carry (parent slot / own record on the
/// 2026-09-13 layout). Was 64 (parity with the old marker scan); raised 2026-09-14 after a mainnet
/// frame (round 1448954117, 23:30:18Z) failed to parse on every relay at once — a busy block can
/// carry more bundles than that. Bounded by the payload length either way. The legacy marker scan
/// keeps its own 1..=64 heuristic.
pub const MAX_BUNDLE_HASHES: usize = 1024;

fn read_hashes(payload: &[u8], mut k: usize, count: usize) -> Option<Vec<[u8; 32]>> {
    if count > MAX_BUNDLE_HASHES || k.checked_add(count * 32)? > payload.len() {
        return None;
    }
    let mut tx_hashes = Vec::with_capacity(count);
    for _ in 0..count {
        let mut a = [0u8; 32];
        a.copy_from_slice(&payload[k..k + 32]);
        tx_hashes.push(a);
        k += 32;
    }
    Some(tx_hashes)
}

/// Structural parse of the parent slot in either layout epoch; no round window (callers apply it).
fn parse_slot(payload: &[u8]) -> Option<Slot> {
    let p = slot_start(payload)?;
    let proposer_end = p.checked_add(20)?;
    let mut proposer = [0u8; 20];
    proposer.copy_from_slice(payload.get(p..proposer_end)?);
    if *payload.get(proposer_end)? == 0xfc {
        // pre-2026-09-13: the record leads with its round
        let round = u32::from_le_bytes([
            *payload.get(proposer_end + 1)?,
            *payload.get(proposer_end + 2)?,
            *payload.get(proposer_end + 3)?,
            *payload.get(proposer_end + 4)?,
        ]);
        let (count, k) = read_varint(payload, proposer_end + 5)?;
        let tx_hashes = read_hashes(payload, k, count as usize)?;
        let end = k + tx_hashes.len() * 32;
        return Some(Slot {
            header: BlockHeader {
                proposer,
                round,
                tx_hashes,
            },
            legacy: true,
            activation: 0,
            end,
        });
    }
    // since 2026-09-13: `[count][hashes]` then the certificate tail carrying the round
    let (count, k) = read_varint(payload, proposer_end)?;
    let tx_hashes = read_hashes(payload, k, count as usize)?;
    let tail = parse_tail(payload, k + tx_hashes.len() * 32)?;
    Some(Slot {
        header: BlockHeader {
            proposer,
            round: tail.round,
            tx_hashes,
        },
        legacy: false,
        activation: tail.activation,
        end: tail.end,
    })
}

/// New-layout frames: the parent slot record plus the frame's own record for R, located by the
/// unique `[fc activation][fc R−1]` tail after the slot. A record with zero hashes is not
/// returned . The count byte is searched over the single-byte varint
/// range (0..=250; a larger count is a multi-byte varint and is not located — parent slot only).
/// Anything ambiguous — two tails,
/// two count bytes that both satisfy the structural check — yields the parent slot alone rather
/// than a guess.
fn tailed_records(payload: &[u8], slot: &Slot) -> Vec<BlockHeader> {
    let mut out = Vec::with_capacity(2);
    if !slot.header.tx_hashes.is_empty() {
        out.push(slot.header.clone());
    }
    // The own record's tail ends `[fc activation][fc qc_parent]`: `qc_parent` is the slot's round
    // on a plain frame and up to TAIL_TC_MAX rounds below it when the own record carries timeout
    // certificates (the slot round itself timed out — module doc). Every candidate end is verified
    // FORWARD below, so the wider window costs a few compares and admits no guess.
    let own_round = slot.header.round.wrapping_add(1);
    let act = slot.activation.to_le_bytes();
    let lo_parent = slot.header.round.wrapping_sub(TAIL_TC_MAX as u32);
    let mut ends: Vec<usize> = Vec::new();
    let mut j = slot.end;
    while j + 10 <= payload.len() {
        if payload[j] == 0xfc && payload[j + 1..j + 5] == act && payload[j + 5] == 0xfc {
            let parent = u32::from_le_bytes([
                payload[j + 6],
                payload[j + 7],
                payload[j + 8],
                payload[j + 9],
            ]);
            if parent >= lo_parent && parent <= slot.header.round {
                ends.push(j + 10);
            }
        }
        j += 1;
    }
    if ends.is_empty() {
        return out;
    }
    // The record's tail `[fc u32][varint][tc…][small][fc activation][fc qc_parent]` ends at `end`;
    // its width varies (module doc), so each count candidate is verified FORWARD: the count byte,
    // then 32 × count hashes, then a tail `record_tail` parses to exactly this end with this round.
    let mut count_idx: Option<usize> = None;
    for &end in &ends {
        for c in 0..=250usize {
            // the tail starts at most RECORD_TAIL_MAX bytes before its end and at least 20 after it began
            for tail_len in 20..=RECORD_TAIL_MAX {
                let Some(hashes_end) = end.checked_sub(tail_len) else {
                    continue;
                };
                let Some(idx) = hashes_end.checked_sub(1 + 32 * c) else {
                    continue;
                };
                if idx < 30 || payload[idx] as usize != c {
                    continue;
                }
                // the count byte, preceded by the 20-byte proposer, a small signer-count varint and
                // the `0xfd` that opens the u64 before it
                if payload[idx - 21] >= 0xfb || payload[idx - 30] != 0xfd {
                    continue;
                }
                if record_tail(payload, hashes_end) != Some((slot.activation, own_round, end)) {
                    continue;
                }
                if count_idx.is_some_and(|i| i != idx) {
                    return out; // ambiguous count: parent slot only
                }
                count_idx = Some(idx);
            }
        }
    }
    let Some(idx) = count_idx else { return out };
    let count = payload[idx] as usize;
    if count == 0 {
        return out;
    }
    let Some(tx_hashes) = read_hashes(payload, idx + 1, count) else {
        return out;
    };
    let mut proposer = [0u8; 20];
    proposer.copy_from_slice(&payload[idx - 20..idx]);
    out.push(BlockHeader {
        proposer,
        round: slot.header.round.wrapping_add(1),
        tx_hashes,
    });
    out
}

/// Extract block records from a decompressed tag=0x00 frame payload: on a post-2026-09-13 frame
/// the parent slot and the frame's own record (structural, see the module doc); on an older
/// frame the round-marker scan below.
pub fn find_block_records(payload: &[u8], round_lo: u32, round_hi: u32) -> Vec<BlockHeader> {
    // The layout epoch is decided structurally, before the window: a new-layout frame whose
    // parent round is out of window yields nothing, never the marker scan's garbage. A payload
    // that parses as neither (an old frame, a synthetic record) keeps the scan.
    match parse_slot(payload) {
        Some(slot) if !slot.legacy => {
            if slot.header.round < round_lo || slot.header.round > round_hi {
                Vec::new()
            } else {
                tailed_records(payload, &slot)
            }
        }
        _ => scan_block_records(payload, round_lo, round_hi),
    }
}

/// Pre-2026-09-13 layout: locate records by their leading round marker.
fn scan_block_records(payload: &[u8], round_lo: u32, round_hi: u32) -> Vec<BlockHeader> {
    let mut out = Vec::new();
    let n = payload.len();
    let mut p = 20usize; // proposer occupies the 20 bytes before the round marker
    while p + 5 <= n {
        if payload[p] == 0xfc {
            let round = u32::from_le_bytes([
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
                payload[p + 4],
            ]);
            if round >= round_lo
                && round <= round_hi
                && let Some((count, k)) = read_varint(payload, p + 5)
            {
                {
                    let count = count as usize;
                    // The marker scan is a heuristic over old-layout frames: its 1..=64 bound is
                    // part of what keeps spurious markers out (raising it to MAX_BUNDLE_HASHES
                    // admitted garbage on the 2026-09 fixtures). Structural parsing has no such need.
                    if (1..=64).contains(&count) && k + count * 32 <= n {
                        let mut proposer = [0u8; 20];
                        proposer.copy_from_slice(&payload[p - 20..p]);
                        let mut tx_hashes = Vec::with_capacity(count);
                        for h in 0..count {
                            let mut a = [0u8; 32];
                            a.copy_from_slice(&payload[k + h * 32..k + h * 32 + 32]);
                            tx_hashes.push(a);
                        }
                        out.push(BlockHeader {
                            proposer,
                            round,
                            tx_hashes,
                        });
                        p = k + count * 32;
                        continue;
                    }
                }
            }
        }
        p += 1;
    }
    out
}

/// The rounds (with bundle counts) of the records an ordering payload carries, bounded to
/// `anchor ± window` like the old locator. Records with no bundles are not returned.
pub fn ordering_rounds(payload: &[u8], anchor: u64, window: u64) -> Vec<(u64, usize)> {
    if anchor == 0 {
        return Vec::new();
    }
    let lo = anchor.saturating_sub(window).min(u64::from(u32::MAX)) as u32;
    let hi = anchor.saturating_add(window).min(u64::from(u32::MAX)) as u32;
    find_block_records(payload, lo, hi)
        .into_iter()
        .map(|r| (u64::from(r.round), r.tx_hashes.len()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decompressed tag-0x00 payloads of a raw wire capture, in order.
    fn ordering_payloads(mut buf: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while buf.len() >= 5 {
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let (kind, body) = (buf[4], &buf[5..5 + len]);
            if kind == 0x01 {
                let size = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
                let payload = lz4_flex::block::decompress(&body[4..], size).expect("lz4");
                if payload.first() == Some(&0x00) {
                    out.push(payload);
                }
            }
            buf = &buf[5 + len..];
        }
        out
    }

    fn rounds(payload: &[u8], lo: u32, hi: u32) -> Vec<u32> {
        let mut r: Vec<u32> = find_block_records(payload, lo, hi)
            .iter()
            .map(|x| x.round)
            .collect();
        r.sort_unstable();
        r
    }

    #[test]
    fn post_upgrade_frames_give_the_parent_and_own_record() {
        let frames = ordering_payloads(include_bytes!(
            "../tests/fixtures/ordering/frames-20260914.bin"
        ));
        let (lo, hi) = (1_448_486_000, 1_448_496_000);
        assert_eq!(frames.len(), 5);
        let got: Vec<Vec<u32>> = frames.iter().map(|p| rounds(p, lo, hi)).collect();
        // frame R carries R-1 (parent slot) and R (own record)
        assert_eq!(got[0], vec![1_448_491_972, 1_448_491_973]);
        assert_eq!(got[4], vec![1_448_491_976, 1_448_491_977]);
        let parent_counts: Vec<usize> = frames
            .iter()
            .map(|p| {
                find_block_records(p, lo, hi)
                    .iter()
                    .min_by_key(|x| x.round)
                    .unwrap()
                    .tx_hashes
                    .len()
            })
            .collect();
        assert_eq!(
            parent_counts,
            vec![2, 2, 2, 2, 1],
            "node truth (replica_cmds)"
        );
        // the own record of frame R equals the next frame's parent slot
        for w in frames.windows(2) {
            let own = find_block_records(&w[0], lo, hi)
                .into_iter()
                .max_by_key(|x| x.round)
                .unwrap();
            let next_parent = find_block_records(&w[1], lo, hi)
                .into_iter()
                .min_by_key(|x| x.round)
                .unwrap();
            assert_eq!(
                (own.round, own.tx_hashes),
                (next_parent.round, next_parent.tx_hashes)
            );
        }
        assert!(
            find_block_records(&frames[0], 1_440_000_000, 1_440_001_000).is_empty(),
            "out of window: nothing, never a guess"
        );
    }

    #[test]
    fn an_empty_parent_round_is_not_a_record_but_the_own_record_is_found() {
        let frames = ordering_payloads(include_bytes!(
            "../tests/fixtures/ordering/frames-empty-parent.bin"
        ));
        let (lo, hi) = (1_447_129_000, 1_447_139_000);
        assert_eq!(frames.len(), 4);
        let r = rounds(&frames[1], lo, hi);
        assert!(
            !r.contains(&1_447_134_309),
            "the empty round produced no block"
        );
        assert!(r.contains(&1_447_134_310));
    }

    #[test]
    fn every_fixture_frame_yields_its_frame_round() {
        let sets: &[(&[u8], u32, u32)] = &[
            (
                include_bytes!("../tests/fixtures/ordering/frame-pre-upgrade-r1446876004.bin"),
                1_446_871_000,
                1_446_881_000,
            ),
            (
                include_bytes!("../tests/fixtures/ordering/frames-20260914.bin"),
                1_448_486_000,
                1_448_496_000,
            ),
            (
                include_bytes!("../tests/fixtures/ordering/frames-empty-parent.bin"),
                1_447_129_000,
                1_447_139_000,
            ),
            (
                include_bytes!("../tests/fixtures/ordering/frames-fb-tail.bin"),
                1_449_447_000,
                1_449_501_000,
            ),
            (
                include_bytes!("../tests/fixtures/ordering/frames-tc-tail.bin"),
                1_456_980_000,
                1_456_990_000,
            ),
        ];
        let mut frames = 0;
        for &(buf, lo, hi) in sets {
            for p in ordering_payloads(buf) {
                frames += 1;
                assert!(
                    !find_block_records(&p, lo, hi).is_empty(),
                    "every fixture ordering yields a record"
                );
            }
        }
        assert_eq!(frames, 17);
    }

    #[test]
    fn ordering_rounds_applies_the_anchor_window() {
        let frames = ordering_payloads(include_bytes!(
            "../tests/fixtures/ordering/frames-20260914.bin"
        ));
        assert_eq!(ordering_rounds(&frames[0], 1_448_491_000, 5_000).len(), 2);
        assert!(ordering_rounds(&frames[0], 1_300_000_000, 5_000).is_empty());
        assert!(
            ordering_rounds(&frames[0], 0, 5_000).is_empty(),
            "no anchor yet: nothing"
        );
    }
}

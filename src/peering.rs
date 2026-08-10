//! Peering dataset collector: a plain TCP subscriber to the Quicknode peering
//! service, consuming Hyperliquid's native gossip wire stream exactly as any
//! peered node would.
//!
//! A block is READY when its ordering record has been parsed (consensus round +
//! the list of referenced transaction bundles) and every referenced bundle has
//! been received and decompressed. Two per-round facts are not decodable from
//! the wire and come from the reference feed instead (deterministic chain data,
//! reproducible from any Hyperliquid node's replay output — see METHODOLOGY):
//! the block's producer timestamp, and each referenced bundle's leading
//! signature value, which is matched against the first 32 bytes of the first
//! record in each wire bundle.
//!
//! Arrival timestamps are captured live at the socket; the reference feed only
//! closes a sample after the fact, so feed lag delays reporting but never
//! distorts a latency value. Rounds whose content does not complete within the
//! cohort deadline are reported through the sequence-gap counter, never guessed.
//!
//! Wire protocol (Hyperliquid gossip, network-public):
//!   frame:    [u32 BE body_len][u8 kind][body]
//!   kind 1:   body = [u32 LE uncompressed_size][raw LZ4 block]
//!   payload[0] == 0x00 -> block/ordering data (round + referenced bundle list)
//!   payload[0] == 0x01 -> transaction bundle (length-indexed signed records)

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::warn;

use crate::model::{ContentKey, EventKey, MarketEvent, ProbeEvent, ProbeSender, Provider};
use crate::streams::{ReconnectBackoff, now_ms};

/// Gossip subscribe hello: frame len=3 kind=0 body=[00 01 00].
const SUBSCRIBE_HELLO: [u8; 8] = [0, 0, 0, 3, 0, 0x00, 0x01, 0x00];
/// Frames larger than this indicate desync, not data (largest observed ~1 MiB).
const MAX_FRAME_BODY: u32 = 8 * 1024 * 1024;
/// Uncompressed payloads are bounded the same way.
const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
/// No frame for this long = dead subscription (live cadence is ~15 blocks/s).
const READ_DEADLINE: Duration = Duration::from_secs(30);
/// Ordering rounds are only tracked within this window of the newest reference
/// round: the round marker is located by scanning, and the window is what makes
/// a stray in-hash byte pattern implausible (validated on 553 consecutive
/// mainnet rounds with zero mismatches).
const ROUND_WINDOW: u64 = 5_000;
/// State pruning: drop tracked rounds this far behind the newest, and bundle
/// arrivals older than the buffer horizon.
const ROUND_RETAIN: u64 = 2_000;
const BUNDLE_RETAIN: Duration = Duration::from_secs(120);
/// A tracked round that has not completed within the cohort deadline is
/// counted as a gap (it can never enter a latency distribution late anyway).
const INCOMPLETE_DEADLINE: Duration = Duration::from_secs(5);

/// One reference-feed line: round, producer time, and per-bundle
/// [bundle_hash_hex, first_signature_r_hex, action_count].
#[derive(Debug, Deserialize)]
struct ReferenceLine {
    r: u64,
    t: String,
    b: Vec<(String, Option<String>, u64)>,
}

#[derive(Debug, Clone)]
struct ReferenceRound {
    time_ms: u64,
    bundle_sigs: Vec<[u8; 32]>,
}

#[derive(Debug, Default)]
struct TrackedRound {
    ordering: Option<(Instant, u64)>,
    reference: Option<ReferenceRound>,
    first_seen: Option<Instant>,
    reported_gap: bool,
}

struct Assembly {
    coin: String,
    rounds: HashMap<u64, TrackedRound>,
    /// first-record sig.r -> (arrival instant, arrival wall ms, seen at)
    bundles: HashMap<[u8; 32], (Instant, u64, Instant)>,
    max_ref_round: u64,
    emitted_high_water: u64,
}

impl Assembly {
    fn new(coin: String) -> Self {
        Self {
            coin,
            rounds: HashMap::new(),
            bundles: HashMap::new(),
            max_ref_round: 0,
            emitted_high_water: 0,
        }
    }

    fn track_ordering(&mut self, round: u64, arrival: Instant, wall_ms: u64) {
        if self.max_ref_round == 0 || round.abs_diff(self.max_ref_round) > ROUND_WINDOW {
            return;
        }
        let entry = self.rounds.entry(round).or_default();
        entry.first_seen.get_or_insert(arrival);
        // First ordering copy wins; a re-served duplicate does not move the boundary.
        if entry.ordering.is_none() {
            entry.ordering = Some((arrival, wall_ms));
        }
    }

    fn track_bundle(&mut self, sig_r: [u8; 32], arrival: Instant, wall_ms: u64) {
        self.bundles
            .entry(sig_r)
            .or_insert((arrival, wall_ms, Instant::now()));
    }

    fn track_reference(&mut self, line: ReferenceLine) {
        let Some(time_ms) = parse_reference_time_ms(&line.t) else {
            return;
        };
        let mut bundle_sigs = Vec::with_capacity(line.b.len());
        for (_hash, first_r, _count) in &line.b {
            let Some(sig) = first_r.as_deref().and_then(parse_hex32) else {
                // A bundle with no signed actions cannot be matched; treat the
                // round as unmatchable rather than pretending completeness.
                return;
            };
            bundle_sigs.push(sig);
        }
        self.max_ref_round = self.max_ref_round.max(line.r);
        let entry = self.rounds.entry(line.r).or_default();
        entry.first_seen.get_or_insert(Instant::now());
        entry.reference = Some(ReferenceRound {
            time_ms,
            bundle_sigs,
        });
    }

    /// Complete every round whose ordering, reference, and all referenced
    /// bundles are present. Returns ready events plus the count of rounds that
    /// expired incomplete (each counted once).
    fn drain_ready(&mut self, now: Instant) -> (Vec<MarketEvent>, u64) {
        let mut ready = Vec::new();
        let mut expired_gaps = 0u64;
        let coin = self.coin.clone();
        for (round, tracked) in self.rounds.iter_mut() {
            if tracked.reported_gap {
                continue;
            }
            let (Some((ord_at, ord_wall)), Some(reference)) =
                (tracked.ordering, tracked.reference.as_ref())
            else {
                if tracked
                    .first_seen
                    .is_some_and(|seen| now.duration_since(seen) > INCOMPLETE_DEADLINE)
                {
                    tracked.reported_gap = true;
                    expired_gaps += 1;
                }
                continue;
            };
            let mut ready_at = ord_at;
            let mut ready_wall = ord_wall;
            let mut complete = true;
            for sig in &reference.bundle_sigs {
                match self.bundles.get(sig) {
                    Some((arrived, wall, _)) => {
                        if *arrived > ready_at {
                            ready_at = *arrived;
                            ready_wall = *wall;
                        }
                    }
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            if !complete {
                if tracked
                    .first_seen
                    .is_some_and(|seen| now.duration_since(seen) > INCOMPLETE_DEADLINE)
                {
                    tracked.reported_gap = true;
                    expired_gaps += 1;
                }
                continue;
            }
            tracked.reported_gap = true; // completed; never revisit
            ready.push(MarketEvent {
                provider: Provider::QuickNodePeeringTcp,
                key: EventKey {
                    coin: coin.clone(),
                    event_ms: reference.time_ms,
                    content: ContentKey::Peering { round: *round },
                },
                received: ready_at,
                received_wall_ms: ready_wall,
            });
            self.emitted_high_water = self.emitted_high_water.max(*round);
        }
        self.prune(now);
        (ready, expired_gaps)
    }

    fn prune(&mut self, now: Instant) {
        let floor = self.max_ref_round.saturating_sub(ROUND_RETAIN);
        self.rounds.retain(|round, _| *round >= floor);
        self.bundles
            .retain(|_, (_, _, seen)| now.duration_since(*seen) < BUNDLE_RETAIN);
    }
}

pub async fn run_peering(
    endpoint: String,
    reference_endpoint: String,
    coin: String,
    sender: ProbeSender,
) {
    let mut backoff = ReconnectBackoff::default();
    loop {
        let started = Instant::now();
        match run_peering_once(&endpoint, &reference_endpoint, &coin, &sender).await {
            Ok(()) => warn!(%coin, "peering stream ended"),
            Err(error) => warn!(%coin, ?error, "peering stream disconnected"),
        }
        if !sender
            .send(ProbeEvent::Reconnect {
                provider: Provider::QuickNodePeeringTcp,
                coin: coin.clone(),
            })
            .await
        {
            return;
        }
        tokio::time::sleep(backoff.after_connection(started.elapsed())).await;
    }
}

async fn run_peering_once(
    endpoint: &str,
    reference_endpoint: &str,
    coin: &str,
    sender: &ProbeSender,
) -> Result<()> {
    // Reference feed first: samples cannot close without it, and its rounds
    // anchor the plausibility window for ordering extraction.
    let reference = TcpStream::connect(reference_endpoint)
        .await
        .context("connect peering reference feed")?;
    let (ref_tx, mut ref_rx) = mpsc::channel::<ReferenceLine>(4096);
    let reference_task = tokio::spawn(async move {
        let mut lines = BufReader::new(reference).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(parsed) = serde_json::from_str::<ReferenceLine>(&line) else {
                continue;
            };
            if ref_tx.send(parsed).await.is_err() {
                break;
            }
        }
    });

    let mut stream = TcpStream::connect(endpoint)
        .await
        .context("connect peering endpoint")?;
    stream
        .write_all(&SUBSCRIBE_HELLO)
        .await
        .context("send peering subscribe hello")?;
    let _connection = sender.connected(Provider::QuickNodePeeringTcp, coin);

    let mut assembly = Assembly::new(coin.to_owned());
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut chunk = vec![0u8; 256 * 1024];
    let mut maintenance = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            read = tokio::time::timeout(READ_DEADLINE, stream.read(&mut chunk)) => {
                let n = read.context("peering read deadline exceeded")??;
                if n == 0 {
                    anyhow::bail!("peering endpoint closed the subscription");
                }
                let arrival = Instant::now();
                let wall_ms = now_ms();
                buf.extend_from_slice(&chunk[..n]);
                consume_frames(&mut buf, &mut assembly, arrival, wall_ms)?;
            }
            line = ref_rx.recv() => {
                let Some(line) = line else {
                    anyhow::bail!("peering reference feed ended");
                };
                assembly.track_reference(line);
            }
            _ = maintenance.tick() => {}
        }
        let (ready, expired) = assembly.drain_ready(Instant::now());
        for event in ready {
            if !sender.send(ProbeEvent::Market(event)).await {
                reference_task.abort();
                return Ok(());
            }
        }
        if expired > 0
            && !sender
                .send(ProbeEvent::SequenceGap {
                    provider: Provider::QuickNodePeeringTcp,
                    coin: coin.to_owned(),
                    missing: expired,
                })
                .await
        {
            reference_task.abort();
            return Ok(());
        }
    }
}

/// Drain complete frames from the reassembly buffer, feeding the assembler.
fn consume_frames(
    buf: &mut Vec<u8>,
    assembly: &mut Assembly,
    arrival: Instant,
    wall_ms: u64,
) -> Result<()> {
    let mut offset = 0usize;
    while buf.len() - offset >= 5 {
        let body_len = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        if body_len > MAX_FRAME_BODY {
            anyhow::bail!("peering frame oversize ({body_len} bytes): stream desync");
        }
        let total = 5 + body_len as usize;
        if buf.len() - offset < total {
            break;
        }
        let kind = buf[offset + 4];
        let body = &buf[offset + 5..offset + total];
        if kind == 0x01
            && let Some(payload) = decompress_data_frame(body)
        {
            match payload.first() {
                Some(0x00) => {
                    for (round, _refs) in ordering_records(&payload, assembly.max_ref_round) {
                        assembly.track_ordering(round, arrival, wall_ms);
                    }
                }
                Some(0x01) => {
                    if let Some(sig) = bundle_first_sig(&payload) {
                        assembly.track_bundle(sig, arrival, wall_ms);
                    }
                }
                _ => {}
            }
        }
        offset += total;
    }
    buf.drain(..offset);
    Ok(())
}

/// kind=0x01 body: [u32 LE uncompressed_size][raw LZ4 block].
fn decompress_data_frame(body: &[u8]) -> Option<Vec<u8>> {
    if body.len() < 5 {
        return None;
    }
    let size = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    if size == 0 || size > MAX_PAYLOAD {
        return None;
    }
    lz4_flex::block::decompress(&body[4..], size).ok()
}

/// Locate ordering records: [proposer 20B][0xfc + u32 LE round][varint count]
/// [32B bundle_hash x count]. Rounds are only accepted within ROUND_WINDOW of
/// the reference anchor, which is what makes the scan-based locator sound.
fn ordering_records(payload: &[u8], anchor_round: u64) -> Vec<(u64, usize)> {
    let mut out = Vec::new();
    if anchor_round == 0 {
        return out;
    }
    let mut i = 20usize;
    while i + 5 <= payload.len() {
        if payload[i] == 0xFC {
            let round = u32::from_le_bytes([
                payload[i + 1],
                payload[i + 2],
                payload[i + 3],
                payload[i + 4],
            ]) as u64;
            if round.abs_diff(anchor_round) <= ROUND_WINDOW
                && let Some((count, next)) = read_bincode_varint(payload, i + 5)
                && count <= 64
                && next + 32 * count as usize <= payload.len()
            {
                out.push((round, count as usize));
                i = next + 32 * count as usize;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// A bundle payload is [0x01][bincode-varint body_len][~4B prelude]
/// [msgpack array of record byte-lengths][records...]; each record starts with
/// the 32-byte signature `r`. Returns the first record's signature.
fn bundle_first_sig(payload: &[u8]) -> Option<[u8; 32]> {
    let (_, body_start) = read_bincode_varint(payload, 1)?;
    let hi = (body_start + 16).min(payload.len());
    for anchor in body_start..hi {
        if let Some(record_start) = validate_length_index(payload, anchor) {
            let sig = payload.get(record_start..record_start + 32)?;
            let mut out = [0u8; 32];
            out.copy_from_slice(sig);
            return Some(out);
        }
    }
    None
}

/// Validate a candidate msgpack length index at `anchor`; on success return
/// the offset of the first record. The index is self-validating: N record
/// lengths must fill the payload up to a small trailer.
fn validate_length_index(payload: &[u8], anchor: usize) -> Option<usize> {
    let tag = *payload.get(anchor)?;
    let (count, mut pos) = match tag {
        0x90..=0x9F => ((tag & 0x0F) as usize, anchor + 1),
        0xDC => (
            u16::from_be_bytes([*payload.get(anchor + 1)?, *payload.get(anchor + 2)?]) as usize,
            anchor + 3,
        ),
        0xDD => (
            u32::from_be_bytes([
                *payload.get(anchor + 1)?,
                *payload.get(anchor + 2)?,
                *payload.get(anchor + 3)?,
                *payload.get(anchor + 4)?,
            ]) as usize,
            anchor + 5,
        ),
        _ => return None,
    };
    if !(1..=100_000).contains(&count) {
        return None;
    }
    let mut sum = 0usize;
    for _ in 0..count {
        let (len, next) = read_msgpack_uint(payload, pos)?;
        if !(1..=200_000).contains(&len) {
            return None;
        }
        sum = sum.checked_add(len as usize)?;
        pos = next;
    }
    let total = pos.checked_add(sum)?;
    if total > payload.len() || payload.len() - total > 256 {
        return None;
    }
    Some(pos)
}

/// Bincode-style varint: <0xFB inline; 0xFB=u16 LE, 0xFC=u32 LE, 0xFD=u64 LE.
fn read_bincode_varint(data: &[u8], i: usize) -> Option<(u64, usize)> {
    let tag = *data.get(i)?;
    Some(match tag {
        0..=0xFA => (tag as u64, i + 1),
        0xFB => (
            u16::from_le_bytes([*data.get(i + 1)?, *data.get(i + 2)?]) as u64,
            i + 3,
        ),
        0xFC => (
            u32::from_le_bytes([
                *data.get(i + 1)?,
                *data.get(i + 2)?,
                *data.get(i + 3)?,
                *data.get(i + 4)?,
            ]) as u64,
            i + 5,
        ),
        0xFD => (
            u64::from_le_bytes([
                *data.get(i + 1)?,
                *data.get(i + 2)?,
                *data.get(i + 3)?,
                *data.get(i + 4)?,
                *data.get(i + 5)?,
                *data.get(i + 6)?,
                *data.get(i + 7)?,
                *data.get(i + 8)?,
            ]),
            i + 9,
        ),
        _ => return None,
    })
}

/// Non-negative msgpack integer (fixint / uint8 / uint16 / uint32 / uint64).
fn read_msgpack_uint(data: &[u8], i: usize) -> Option<(u64, usize)> {
    let tag = *data.get(i)?;
    Some(match tag {
        0..=0x7F => (tag as u64, i + 1),
        0xCC => (*data.get(i + 1)? as u64, i + 2),
        0xCD => (
            u16::from_be_bytes([*data.get(i + 1)?, *data.get(i + 2)?]) as u64,
            i + 3,
        ),
        0xCE => (
            u32::from_be_bytes([
                *data.get(i + 1)?,
                *data.get(i + 2)?,
                *data.get(i + 3)?,
                *data.get(i + 4)?,
            ]) as u64,
            i + 5,
        ),
        0xCF => (
            u64::from_be_bytes([
                *data.get(i + 1)?,
                *data.get(i + 2)?,
                *data.get(i + 3)?,
                *data.get(i + 4)?,
                *data.get(i + 5)?,
                *data.get(i + 6)?,
                *data.get(i + 7)?,
                *data.get(i + 8)?,
            ]),
            i + 9,
        ),
        _ => return None,
    })
}

/// Reference times are RFC3339-like with nanosecond precision and an implied
/// UTC ("2026-08-10T14:43:15.093322286"); interpreted as UTC, reported in ms.
fn parse_reference_time_ms(text: &str) -> Option<u64> {
    let naive = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    let ms = naive.and_utc().timestamp_millis();
    u64::try_from(ms).ok()
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    let hex = text.strip_prefix("0x").unwrap_or(text);
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// The peering endpoint is a plain host:port with no credentials; unlike the
/// gRPC/WS endpoints there is no secret to leak, so validation only rejects
/// obviously malformed values.
pub fn validate_peering_endpoint(endpoint: &str, label: &str) -> Result<()> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .with_context(|| format!("{label} must be host:port"))?;
    if host.is_empty() || host.contains('/') || host.contains('@') {
        anyhow::bail!("{label} must be a bare host:port with no scheme or credentials");
    }
    port.parse::<u16>()
        .with_context(|| format!("{label} port must be a valid TCP port"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lz4_frame(payload: &[u8]) -> Vec<u8> {
        let compressed = lz4_flex::block::compress(payload);
        let mut body = (payload.len() as u32).to_le_bytes().to_vec();
        body.extend_from_slice(&compressed);
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.push(0x01);
        frame.extend_from_slice(&body);
        frame
    }

    fn ordering_payload(round: u32, bundle_hashes: &[[u8; 32]]) -> Vec<u8> {
        let mut p = vec![0x00];
        p.extend_from_slice(&[0xAA; 24]); // preamble stand-in
        p.extend_from_slice(&[0xBB; 20]); // proposer
        p.push(0xFC);
        p.extend_from_slice(&round.to_le_bytes());
        p.push(bundle_hashes.len() as u8);
        for hash in bundle_hashes {
            p.extend_from_slice(hash);
        }
        p
    }

    fn bundle_payload(records: &[&[u8]]) -> Vec<u8> {
        let mut body = vec![0u8; 4]; // prelude stand-in
        body.push(0x90 | records.len() as u8);
        for record in records {
            assert!(record.len() < 0x80);
            body.push(record.len() as u8);
        }
        for record in records {
            body.extend_from_slice(record);
        }
        let mut p = vec![0x01];
        assert!(body.len() < 0xFB);
        p.push(body.len() as u8);
        p.extend_from_slice(&body);
        p
    }

    fn record_with_sig(sig: [u8; 32]) -> Vec<u8> {
        let mut record = sig.to_vec();
        record.extend_from_slice(&[0u8; 42]); // s(32 truncated stand-in)+v+nonce…
        record
    }

    fn reference_line(round: u64, time: &str, sigs: &[[u8; 32]]) -> ReferenceLine {
        ReferenceLine {
            r: round,
            t: time.to_owned(),
            b: sigs
                .iter()
                .map(|sig| {
                    (
                        format!("0x{}", "ab".repeat(32)),
                        Some(format!(
                            "0x{}",
                            sig.iter().map(|b| format!("{b:02x}")).collect::<String>()
                        )),
                        1u64,
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn block_ready_boundary_is_the_last_required_arrival() {
        let mut assembly = Assembly::new("BLOCKS".to_owned());
        let sig_a = [0x11; 32];
        let sig_b = [0x22; 32];
        assembly.track_reference(reference_line(
            1_000_100,
            "2026-08-10T14:43:15.093322286",
            &[sig_a, sig_b],
        ));

        let t0 = Instant::now();
        assembly.track_bundle(sig_a, t0, 5_000);
        assembly.track_ordering(1_000_100, t0, 5_010);
        let (ready, gaps) = assembly.drain_ready(t0);
        assert!(ready.is_empty(), "must not complete before every bundle");
        assert_eq!(gaps, 0);

        let later = t0 + Duration::from_millis(40);
        assembly.track_bundle(sig_b, later, 5_050);
        let (ready, _) = assembly.drain_ready(later);
        assert_eq!(ready.len(), 1);
        let event = &ready[0];
        // The boundary is the LAST required arrival — the late bundle, not the ordering.
        assert_eq!(event.received_wall_ms, 5_050);
        assert_eq!(event.received, later);
        assert_eq!(event.key.content, ContentKey::Peering { round: 1_000_100 });
        // Producer time comes from the reference feed, parsed as UTC ms.
        assert_eq!(event.key.event_ms, 1_786_372_995_093);

        // A completed round is emitted exactly once.
        let (again, _) = assembly.drain_ready(later);
        assert!(again.is_empty());
    }

    #[test]
    fn incomplete_round_expires_into_exactly_one_gap() {
        let mut assembly = Assembly::new("BLOCKS".to_owned());
        assembly.track_reference(reference_line(
            2_000_000,
            "2026-08-10T14:43:15.0",
            &[[0x33; 32]],
        ));
        let t0 = Instant::now();
        assembly.track_ordering(2_000_000, t0, 1);
        let (ready, gaps) = assembly.drain_ready(t0);
        assert!(ready.is_empty());
        assert_eq!(gaps, 0);
        let expired = t0 + INCOMPLETE_DEADLINE + Duration::from_millis(1);
        let (ready, gaps) = assembly.drain_ready(expired);
        assert!(ready.is_empty());
        assert_eq!(gaps, 1, "missing bundle after the deadline is one gap");
        let (_, gaps_again) = assembly.drain_ready(expired + Duration::from_secs(1));
        assert_eq!(gaps_again, 0, "a gap is never double-counted");
    }

    #[test]
    fn ordering_rounds_outside_the_reference_window_are_ignored() {
        let mut assembly = Assembly::new("BLOCKS".to_owned());
        assembly.track_reference(reference_line(
            1_000_000,
            "2026-08-10T14:43:15.0",
            &[[0x44; 32]],
        ));
        let t0 = Instant::now();
        assembly.track_ordering(1_000_000 + ROUND_WINDOW + 1, t0, 1);
        assert!(
            !assembly
                .rounds
                .contains_key(&(1_000_000 + ROUND_WINDOW + 1))
        );
        // And with no reference anchor at all, nothing is tracked.
        let mut cold = Assembly::new("BLOCKS".to_owned());
        cold.track_ordering(1_000_000, t0, 1);
        assert!(cold.rounds.is_empty());
    }

    #[test]
    fn wire_frames_round_trip_through_the_parsers() {
        let sig = [0x55; 32];
        let ordering = ordering_payload(1_500_000, &[[0xCC; 32]]);
        let bundle = bundle_payload(&[&record_with_sig(sig)]);

        let mut buf = lz4_frame(&ordering);
        buf.extend_from_slice(&lz4_frame(&bundle));
        let mut assembly = Assembly::new("BLOCKS".to_owned());
        assembly.track_reference(reference_line(1_500_000, "2026-08-10T14:43:15.0", &[sig]));
        consume_frames(&mut buf, &mut assembly, Instant::now(), 42).unwrap();
        assert!(buf.is_empty(), "both frames fully consumed");
        let (ready, _) = assembly.drain_ready(Instant::now());
        assert_eq!(
            ready.len(),
            1,
            "ordering + matched bundle completes the round"
        );
    }

    #[test]
    fn oversize_frame_is_a_desync_error_and_partial_frames_wait() {
        let mut assembly = Assembly::new("BLOCKS".to_owned());
        let mut oversize = (MAX_FRAME_BODY + 1).to_be_bytes().to_vec();
        oversize.push(0x01);
        assert!(consume_frames(&mut oversize, &mut assembly, Instant::now(), 0).is_err());

        let frame = lz4_frame(&[0x01, 0x00]);
        let mut partial = frame[..frame.len() - 1].to_vec();
        let kept = partial.clone();
        consume_frames(&mut partial, &mut assembly, Instant::now(), 0).unwrap();
        assert_eq!(partial, kept, "incomplete frame stays buffered");
    }

    #[test]
    fn endpoint_validation_rejects_schemes_and_credentials() {
        assert!(validate_peering_endpoint("example.test:4001", "peering endpoint").is_ok());
        assert!(validate_peering_endpoint("tcp://example.test:4001", "peering endpoint").is_err());
        assert!(validate_peering_endpoint("user@example.test:4001", "peering endpoint").is_err());
        assert!(validate_peering_endpoint("example.test", "peering endpoint").is_err());
        assert!(validate_peering_endpoint("example.test:99999", "peering endpoint").is_err());
    }

    #[test]
    fn reference_time_is_parsed_as_utc_milliseconds() {
        assert_eq!(
            parse_reference_time_ms("2026-08-10T14:43:15.093322286"),
            Some(1_786_372_995_093)
        );
        assert!(parse_reference_time_ms("not a time").is_none());
    }
}

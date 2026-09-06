//! Mempool on the Quicknode VPC box: the endpoint and the box, one reference per bundle.
//!
//! Off the box the mempool dataset is one source (Quicknode gRPC) timed from the embedded
//! first-seen timestamp of the node behind the endpoint. On the box a second leg exists — the
//! co-located sentry's `bundle` line, written when the bundle body is decoded and its signers are
//! recovered, about 140 ms before the ordering that includes it — and the two legs cannot share
//! the node's embedded timestamp (the sentry line does not carry it, and it is another node's
//! clock). So both legs are referenced to the **box's first sight** of the bundle: the sentry's
//! receipt time from the `bundle` line, or the arrival of the same bundle at this process if that
//! came first. Every BTC bundle the box saw is one two-source cohort keyed by tx hash; a bundle
//! the endpoint delivered but the sentry never saw is a gap charged to the VPC leg, never a
//! sample; a bundle the sentry saw and the endpoint never sent is scored missing for the endpoint
//! by the cohort machinery.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::warn;

use crate::model::{ContentKey, EventKey, MarketEvent, ProbeEvent, ProbeSender, Provider};
use crate::peering::read_bounded_line;
use crate::streams::{
    ReconnectBackoff, mempool_action_matches_asset, mempool_asset_id, now_ms, valid_mempool_tx_hash,
};

pub const JOIN_QUEUE: usize = 8192;
/// A single leg older than this is emitted alone (the other leg is then scored missing).
const JOIN_DEADLINE: Duration = Duration::from_secs(5);
/// Finished hashes are remembered this long so re-sent copies never reopen a cohort.
const DONE_RETAIN: Duration = Duration::from_secs(120);
const READ_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum JoinInput {
    /// The sentry's `bundle` line: the VPC leg, plus the box's receipt time of the bundle.
    Bundle {
        tx_hash: String,
        first_recv_ms: u64,
        received: Instant,
        received_wall_ms: u64,
    },
    /// The Quicknode gRPC mempool message for the same bundle.
    Grpc {
        tx_hash: String,
        received: Instant,
        received_wall_ms: u64,
    },
}

#[derive(Debug, Default)]
struct Entry {
    first_recv_ms: Option<u64>,
    vpc: Option<(Instant, u64)>,
    grpc: Option<(Instant, u64)>,
    first_seen: Option<Instant>,
}

impl Entry {
    fn complete(&self) -> bool {
        self.vpc.is_some() && self.grpc.is_some()
    }

    /// The box's first sight of the bundle: the sentry's receipt, or an earlier arrival of the
    /// same bundle at this process. Never later than any leg, so no leg is negative.
    fn reference_ms(&self) -> u64 {
        [
            self.first_recv_ms,
            self.vpc.map(|(_, wall)| wall),
            self.grpc.map(|(_, wall)| wall),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(0)
    }

    fn events(&self, coin: &str, tx_hash: &str) -> Vec<MarketEvent> {
        let event_ms = self.reference_ms();
        let key = || EventKey {
            coin: coin.to_owned(),
            event_ms,
            content: ContentKey::Mempool {
                tx_hash: tx_hash.to_owned(),
            },
        };
        let mut out = Vec::with_capacity(2);
        if let Some((received, received_wall_ms)) = self.grpc {
            out.push(MarketEvent {
                provider: Provider::QuickNodeGrpc,
                key: key(),
                received,
                received_wall_ms,
            });
        }
        if let Some((received, received_wall_ms)) = self.vpc {
            out.push(MarketEvent {
                provider: Provider::QuickNodeVpc,
                key: key(),
                received,
                received_wall_ms,
            });
        }
        out
    }
}

pub struct MempoolJoin {
    coin: String,
    open: HashMap<String, Entry>,
    done: HashMap<String, Instant>,
}

impl MempoolJoin {
    pub fn new(coin: String) -> Self {
        Self {
            coin,
            open: HashMap::new(),
            done: HashMap::new(),
        }
    }

    /// Record one leg; returns both legs' events the moment the second arrives.
    pub fn offer(&mut self, input: JoinInput, now: Instant) -> Vec<MarketEvent> {
        let (tx_hash, received, wall) = match &input {
            JoinInput::Bundle {
                tx_hash,
                received,
                received_wall_ms,
                ..
            }
            | JoinInput::Grpc {
                tx_hash,
                received,
                received_wall_ms,
            } => (tx_hash.to_ascii_lowercase(), *received, *received_wall_ms),
        };
        if self.done.contains_key(&tx_hash) {
            return Vec::new();
        }
        let entry = self.open.entry(tx_hash.clone()).or_default();
        entry.first_seen.get_or_insert(now);
        match input {
            JoinInput::Bundle { first_recv_ms, .. } => {
                // First copy wins; the sentry writes a bundle once, a re-dial may replay it.
                if entry.vpc.is_none() {
                    entry.vpc = Some((received, wall));
                    entry.first_recv_ms = Some(first_recv_ms);
                }
            }
            JoinInput::Grpc { .. } => {
                if entry.grpc.is_none() {
                    entry.grpc = Some((received, wall));
                }
            }
        }
        if entry.complete() {
            let events = entry.events(&self.coin, &tx_hash);
            self.open.remove(&tx_hash);
            self.done.insert(tx_hash, now);
            return events;
        }
        Vec::new()
    }

    /// Single-leg entries past the deadline are emitted alone. Returns the events plus the number
    /// of bundles the endpoint delivered that the box's sentry never saw (a VPC-leg gap).
    pub fn expire(&mut self, now: Instant) -> (Vec<MarketEvent>, u64) {
        let mut events = Vec::new();
        let mut vpc_gaps = 0u64;
        let expired: Vec<String> = self
            .open
            .iter()
            .filter(|(_, entry)| {
                entry
                    .first_seen
                    .is_some_and(|seen| now.duration_since(seen) >= JOIN_DEADLINE)
            })
            .map(|(hash, _)| hash.clone())
            .collect();
        for hash in expired {
            if let Some(entry) = self.open.remove(&hash) {
                if entry.vpc.is_none() {
                    vpc_gaps += 1;
                }
                events.extend(entry.events(&self.coin, &hash));
                self.done.insert(hash, now);
            }
        }
        self.done
            .retain(|_, finished| now.duration_since(*finished) < DONE_RETAIN);
        (events, vpc_gaps)
    }
}

/// The join task: one per coin, fed by the gRPC leg and the bundle-line reader.
pub async fn run_join(mut rx: mpsc::Receiver<JoinInput>, coin: String, sender: ProbeSender) {
    let mut join = MempoolJoin::new(coin.clone());
    let mut maintenance = tokio::time::interval(Duration::from_millis(500));
    loop {
        let events = tokio::select! {
            input = rx.recv() => {
                let Some(input) = input else { return };
                join.offer(input, Instant::now())
            }
            _ = maintenance.tick() => {
                let (events, vpc_gaps) = join.expire(Instant::now());
                if vpc_gaps > 0
                    && !sender
                        .send(ProbeEvent::SequenceGap {
                            provider: Provider::QuickNodeVpc,
                            coin: coin.clone(),
                            missing: vpc_gaps,
                        })
                        .await
                {
                    return;
                }
                events
            }
        };
        for event in events {
            if !sender.send(ProbeEvent::Market(event)).await {
                return;
            }
        }
    }
}

/// One `bundle` line of the sentry's decoded stream, read for what the join needs. Every other
/// line type is skipped before parsing (a `block` line can be megabytes).
#[derive(Debug, Deserialize)]
struct BundleLine {
    hash: String,
    first_recv_ns: u64,
    #[serde(default)]
    signed_actions: Vec<serde_json::Value>,
}

const BUNDLE_PREFIX: &[u8] = b"{\"type\":\"bundle\",";

/// Parse a decoded-stream line into a join input when it is a `bundle` line whose actions touch
/// the coin's asset; `None` for every other line.
fn bundle_input(
    line: &[u8],
    coin: &str,
    received: Instant,
    received_wall_ms: u64,
) -> Option<JoinInput> {
    if !line.starts_with(BUNDLE_PREFIX) {
        return None;
    }
    let parsed = serde_json::from_slice::<BundleLine>(line).ok()?;
    let expected_asset = mempool_asset_id(coin)?;
    if !valid_mempool_tx_hash(&parsed.hash)
        || parsed.signed_actions.is_empty()
        || !parsed
            .signed_actions
            .iter()
            .any(|signed| mempool_action_matches_asset(signed.get("action"), expected_asset))
    {
        return None;
    }
    Some(JoinInput::Bundle {
        tx_hash: parsed.hash.to_ascii_lowercase(),
        first_recv_ms: parsed.first_recv_ns / 1_000_000,
        received,
        received_wall_ms,
    })
}

/// The bundle-line reader: subscribe to the sentry's decoded socket and feed BTC bundles to the join.
pub async fn run_bundle_lines(
    endpoint: String,
    coin: String,
    sender: ProbeSender,
    join: mpsc::Sender<JoinInput>,
) {
    let mut backoff = ReconnectBackoff::default();
    loop {
        let started = Instant::now();
        match run_bundle_lines_once(&endpoint, &coin, &sender, &join).await {
            Ok(()) => warn!(%coin, "decoded stream ended"),
            Err(error) => warn!(%coin, ?error, "decoded stream disconnected"),
        }
        if !sender
            .send(ProbeEvent::Reconnect {
                provider: Provider::QuickNodeVpc,
                coin: coin.clone(),
            })
            .await
        {
            return;
        }
        tokio::time::sleep(backoff.after_connection(started.elapsed())).await;
    }
}

async fn run_bundle_lines_once(
    endpoint: &str,
    coin: &str,
    sender: &ProbeSender,
    join: &mpsc::Sender<JoinInput>,
) -> Result<()> {
    let stream = TcpStream::connect(endpoint)
        .await
        .context("connect decoded stream socket")?;
    let _connection = sender.connected(Provider::QuickNodeVpc, coin);
    let mut reader = BufReader::with_capacity(1 << 20, stream);
    let mut line: Vec<u8> = Vec::with_capacity(1 << 16);
    loop {
        let complete =
            tokio::time::timeout(READ_DEADLINE, read_bounded_line(&mut reader, &mut line))
                .await
                .context("decoded stream read deadline exceeded")??;
        if !complete {
            anyhow::bail!("decoded stream socket closed");
        }
        // The boundary: the whole bundle line is readable by a client on the box.
        let received = Instant::now();
        let received_wall_ms = now_ms();
        if let Some(input) = bundle_input(&line, coin, received, received_wall_ms)
            && join.send(input).await.is_err()
        {
            return Ok(());
        }
        line.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle_line(hash: &str, asset: u64) -> Vec<u8> {
        format!(
            "{{\"type\":\"bundle\",\"hash\":\"{hash}\",\"first_recv_ns\":1788653369000123456,\"copies\":3,\"raw_frame\":false,\"n_actions\":1,\"emit_ns\":1788653369008000000,\"signed_actions\":[{{\"signature\":{{\"r\":\"0x1\",\"s\":\"0x2\",\"v\":27}},\"user\":\"0xaa\",\"account\":\"0xbb\",\"nonce\":1,\"action\":{{\"type\":\"order\",\"orders\":[{{\"a\":{asset},\"b\":true,\"p\":\"1\",\"s\":\"1\",\"r\":false,\"t\":{{\"limit\":{{\"tif\":\"Alo\"}}}}}}],\"grouping\":\"na\"}}}}]}}"
        )
        .into_bytes()
    }

    #[test]
    fn only_btc_bundle_lines_become_join_inputs() {
        let t0 = Instant::now();
        let hash = format!("0x{}", "ab".repeat(32));
        let Some(JoinInput::Bundle {
            tx_hash,
            first_recv_ms,
            ..
        }) = bundle_input(&bundle_line(&hash, 0), "BTC", t0, 5)
        else {
            panic!("BTC bundle line is a join input");
        };
        assert_eq!(tx_hash, hash);
        assert_eq!(first_recv_ms, 1_788_653_369_000);
        assert!(bundle_input(&bundle_line(&hash, 1), "BTC", t0, 5).is_none());
        assert!(bundle_input(&bundle_line(&hash, 0), "ETH", t0, 5).is_none());
        assert!(bundle_input(b"{\"type\":\"block\",\"round\":1}", "BTC", t0, 5).is_none());
        assert!(bundle_input(b"{\"type\":\"commit\",\"round\":1}", "BTC", t0, 5).is_none());
    }

    #[test]
    fn both_legs_share_the_boxs_first_sight_and_the_second_arrival_closes_the_cohort() {
        let mut join = MempoolJoin::new("BTC".to_owned());
        let t0 = Instant::now();
        let hash = format!("0x{}", "cd".repeat(32));
        assert!(
            join.offer(
                JoinInput::Bundle {
                    tx_hash: hash.clone(),
                    first_recv_ms: 1_000,
                    received: t0 + Duration::from_millis(8),
                    received_wall_ms: 1_008,
                },
                t0 + Duration::from_millis(8),
            )
            .is_empty()
        );
        let events = join.offer(
            JoinInput::Grpc {
                tx_hash: hash.to_ascii_uppercase(),
                received: t0 + Duration::from_millis(60),
                received_wall_ms: 1_060,
            },
            t0 + Duration::from_millis(60),
        );
        assert_eq!(events.len(), 2);
        for event in &events {
            assert_eq!(event.key.event_ms, 1_000);
            assert_eq!(
                event.key.content,
                ContentKey::Mempool {
                    tx_hash: hash.clone()
                }
            );
        }
        assert_eq!(events[0].provider, Provider::QuickNodeGrpc);
        assert_eq!(events[0].received_wall_ms, 1_060);
        assert_eq!(events[1].provider, Provider::QuickNodeVpc);
        assert_eq!(events[1].received_wall_ms, 1_008);
        // A re-sent copy never reopens the cohort.
        assert!(
            join.offer(
                JoinInput::Grpc {
                    tx_hash: hash.clone(),
                    received: t0,
                    received_wall_ms: 1_070,
                },
                t0 + Duration::from_millis(70),
            )
            .is_empty()
        );
        assert_eq!(join.expire(t0 + Duration::from_secs(10)).1, 0);
    }

    #[test]
    fn an_endpoint_arrival_before_the_sentry_moves_the_reference_so_no_leg_is_negative() {
        let mut join = MempoolJoin::new("BTC".to_owned());
        let t0 = Instant::now();
        let hash = format!("0x{}", "ef".repeat(32));
        join.offer(
            JoinInput::Grpc {
                tx_hash: hash.clone(),
                received: t0,
                received_wall_ms: 900,
            },
            t0,
        );
        let events = join.offer(
            JoinInput::Bundle {
                tx_hash: hash,
                first_recv_ms: 950,
                received: t0 + Duration::from_millis(58),
                received_wall_ms: 958,
            },
            t0 + Duration::from_millis(58),
        );
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.key.event_ms == 900));
        assert!(
            events
                .iter()
                .all(|event| event.received_wall_ms >= event.key.event_ms)
        );
    }

    #[test]
    fn a_lone_leg_is_emitted_at_the_deadline_and_a_sentry_miss_is_a_vpc_gap() {
        let mut join = MempoolJoin::new("BTC".to_owned());
        let t0 = Instant::now();
        join.offer(
            JoinInput::Grpc {
                tx_hash: format!("0x{}", "01".repeat(32)),
                received: t0,
                received_wall_ms: 100,
            },
            t0,
        );
        join.offer(
            JoinInput::Bundle {
                tx_hash: format!("0x{}", "02".repeat(32)),
                first_recv_ms: 100,
                received: t0,
                received_wall_ms: 108,
            },
            t0,
        );
        assert!(join.expire(t0 + Duration::from_secs(4)).0.is_empty());
        let (events, vpc_gaps) = join.expire(t0 + Duration::from_secs(5));
        assert_eq!(events.len(), 2);
        assert_eq!(
            vpc_gaps, 1,
            "the endpoint-only bundle is a bundle the box never saw"
        );
        assert!(
            events
                .iter()
                .any(|event| event.provider == Provider::QuickNodeGrpc)
        );
        assert!(
            events
                .iter()
                .any(|event| event.provider == Provider::QuickNodeVpc)
        );
        assert_eq!(join.expire(t0 + Duration::from_secs(6)).0.len(), 0);
    }
}

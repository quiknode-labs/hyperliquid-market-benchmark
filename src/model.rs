use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use clap::ValueEnum;
use tokio::sync::mpsc;

pub const PROVIDERS: [Provider; 3] = [
    Provider::FoundationWs,
    Provider::HydromancerWs,
    Provider::QuickNodeGrpc,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum Dataset {
    Bbo,
    L2book,
}

impl Dataset {
    pub const fn channel(self) -> &'static str {
        match self {
            Self::Bbo => "bbo",
            Self::L2book => "l2Book",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Bbo => "bbo",
            Self::L2book => "l2book",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    FoundationWs,
    HydromancerWs,
    QuickNodeGrpc,
}

impl Provider {
    pub const fn index(self) -> usize {
        match self {
            Self::FoundationWs => 0,
            Self::HydromancerWs => 1,
            Self::QuickNodeGrpc => 2,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::FoundationWs => "foundation-ws",
            Self::HydromancerWs => "hydromancer-ws",
            Self::QuickNodeGrpc => "quicknode-grpc",
        }
    }

    pub const fn transport(self) -> &'static str {
        match self {
            Self::FoundationWs | Self::HydromancerWs => "ws",
            Self::QuickNodeGrpc => "grpc",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventKey {
    pub coin: String,
    pub event_ms: u64,
    pub book: BookKey,
}

impl EventKey {
    pub fn base(&self) -> BaseKey {
        BaseKey {
            coin: self.coin.clone(),
            event_ms: self.event_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BaseKey {
    pub coin: String,
    pub event_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BookKey {
    Bbo {
        bid: Option<LevelKey>,
        ask: Option<LevelKey>,
    },
    L2 {
        bids: Vec<LevelKey>,
        asks: Vec<LevelKey>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LevelKey {
    pub px: String,
    pub sz: String,
    pub n: u32,
}

#[derive(Debug)]
pub struct BookEvent {
    pub provider: Provider,
    pub key: EventKey,
    pub received: Instant,
    pub received_wall_ms: u64,
}

#[derive(Debug)]
pub enum ProbeEvent {
    Book(BookEvent),
    Reconnect {
        provider: Provider,
        coin: String,
    },
    SequenceGap {
        provider: Provider,
        coin: String,
        missing: u64,
    },
    Replay {
        provider: Provider,
        coin: String,
        messages: u64,
        has_gap: bool,
    },
}

#[derive(Default)]
pub struct StreamSignal {
    connection_up: AtomicBool,
    last_message_wall_ms: AtomicU64,
    queue_dropped: AtomicU64,
    last_queue_drop_wall_ms: AtomicU64,
    connection_generation: AtomicU64,
    connected_at_wall_ms: AtomicU64,
}

impl StreamSignal {
    pub fn snapshot(&self) -> StreamSignalSnapshot {
        StreamSignalSnapshot {
            connection_up: self.connection_up.load(Ordering::Relaxed),
            last_message_wall_ms: self.last_message_wall_ms.load(Ordering::Relaxed),
            queue_dropped: self.queue_dropped.load(Ordering::Relaxed),
            last_queue_drop_wall_ms: self.last_queue_drop_wall_ms.load(Ordering::Relaxed),
            connection_generation: self.connection_generation.load(Ordering::Relaxed),
            connected_at_wall_ms: self.connected_at_wall_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StreamSignalSnapshot {
    pub connection_up: bool,
    pub last_message_wall_ms: u64,
    pub queue_dropped: u64,
    pub last_queue_drop_wall_ms: u64,
    pub connection_generation: u64,
    pub connected_at_wall_ms: u64,
}

pub struct RuntimeSignals {
    streams: HashMap<Provider, HashMap<String, Arc<StreamSignal>>>,
}

impl RuntimeSignals {
    pub fn new(coins: &[String]) -> Self {
        let mut streams = HashMap::new();
        for provider in PROVIDERS {
            let mut provider_streams = HashMap::new();
            for coin in coins {
                provider_streams.insert(coin.clone(), Arc::new(StreamSignal::default()));
            }
            streams.insert(provider, provider_streams);
        }
        Self { streams }
    }

    pub fn stream(&self, provider: Provider, coin: &str) -> Arc<StreamSignal> {
        self.streams
            .get(&provider)
            .and_then(|streams| streams.get(coin))
            .unwrap_or_else(|| panic!("unregistered stream {} {coin}", provider.name()))
            .clone()
    }

    pub fn snapshot(&self, provider: Provider, coin: &str) -> StreamSignalSnapshot {
        self.stream(provider, coin).snapshot()
    }

    #[cfg(test)]
    pub fn set_test_state(
        &self,
        provider: Provider,
        coin: &str,
        connected: bool,
        last_message_wall_ms: u64,
        last_queue_drop_wall_ms: u64,
    ) {
        let signal = self.stream(provider, coin);
        signal.connection_up.store(connected, Ordering::Relaxed);
        signal
            .last_message_wall_ms
            .store(last_message_wall_ms, Ordering::Relaxed);
        signal
            .last_queue_drop_wall_ms
            .store(last_queue_drop_wall_ms, Ordering::Relaxed);
    }
}

pub struct ConnectionGuard {
    signal: Arc<StreamSignal>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.signal.connection_up.store(false, Ordering::Relaxed);
        self.signal.last_message_wall_ms.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct ProbeSender {
    tx: mpsc::Sender<ProbeEvent>,
    signals: Arc<RuntimeSignals>,
}

impl ProbeSender {
    pub fn new(tx: mpsc::Sender<ProbeEvent>, signals: Arc<RuntimeSignals>) -> Self {
        Self { tx, signals }
    }

    pub fn connected(&self, provider: Provider, coin: &str) -> ConnectionGuard {
        let signal = self.signals.stream(provider, coin);
        signal.last_message_wall_ms.store(0, Ordering::Relaxed);
        signal
            .connected_at_wall_ms
            .store(now_ms(), Ordering::Relaxed);
        signal.connection_generation.fetch_add(1, Ordering::Relaxed);
        signal.connection_up.store(true, Ordering::Relaxed);
        ConnectionGuard { signal }
    }

    pub fn stream_snapshot(&self, provider: Provider, coin: &str) -> StreamSignalSnapshot {
        self.signals.snapshot(provider, coin)
    }

    pub async fn send(&self, event: ProbeEvent) -> bool {
        match event {
            ProbeEvent::Book(book) => {
                let signal = self.signals.stream(book.provider, &book.key.coin);
                let received_wall_ms = book.received_wall_ms;
                signal
                    .last_message_wall_ms
                    .store(received_wall_ms, Ordering::Relaxed);
                match self.tx.try_send(ProbeEvent::Book(book)) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        signal.queue_dropped.fetch_add(1, Ordering::Relaxed);
                        signal
                            .last_queue_drop_wall_ms
                            .store(received_wall_ms, Ordering::Relaxed);
                        true
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            }
            control => {
                let (provider, coin) = match &control {
                    ProbeEvent::Book(_) => unreachable!("book handled above"),
                    ProbeEvent::Reconnect { provider, coin }
                    | ProbeEvent::SequenceGap { provider, coin, .. }
                    | ProbeEvent::Replay { provider, coin, .. } => (*provider, coin.as_str()),
                };
                let signal = self.signals.stream(provider, coin);
                match self.tx.try_send(control) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        signal.queue_dropped.fetch_add(1, Ordering::Relaxed);
                        signal
                            .last_queue_drop_wall_ms
                            .store(now_ms(), Ordering::Relaxed);
                        true
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(coin: &str) -> BookEvent {
        BookEvent {
            provider: Provider::FoundationWs,
            key: EventKey {
                coin: coin.to_owned(),
                event_ms: 1,
                book: BookKey::Bbo {
                    bid: None,
                    ask: Some(LevelKey {
                        px: "1".to_owned(),
                        sz: "1".to_owned(),
                        n: 1,
                    }),
                },
            },
            received: Instant::now(),
            received_wall_ms: 2,
        }
    }

    #[tokio::test]
    async fn full_hot_path_queue_is_counted_and_never_blocks() {
        let coins = vec!["BTC".to_owned()];
        let signals = Arc::new(RuntimeSignals::new(&coins));
        let (tx, _rx) = mpsc::channel(1);
        let sender = ProbeSender::new(tx, signals.clone());

        assert!(sender.send(ProbeEvent::Book(book("BTC"))).await);
        assert!(sender.send(ProbeEvent::Book(book("BTC"))).await);
        assert!(
            sender
                .send(ProbeEvent::Reconnect {
                    provider: Provider::FoundationWs,
                    coin: "BTC".to_owned(),
                })
                .await
        );

        let snapshot = signals.snapshot(Provider::FoundationWs, "BTC");
        assert_eq!(snapshot.queue_dropped, 2);
        assert_eq!(snapshot.last_message_wall_ms, 2);
        assert!(snapshot.last_queue_drop_wall_ms > 0);
    }

    #[test]
    fn connection_guard_cannot_leave_a_false_positive() {
        let signals = RuntimeSignals::new(&["BTC".to_owned()]);
        let (tx, _rx) = mpsc::channel(1);
        let sender = ProbeSender::new(tx, Arc::new(signals));
        sender
            .signals
            .set_test_state(Provider::QuickNodeGrpc, "BTC", false, 123, 0);

        let guard = sender.connected(Provider::QuickNodeGrpc, "BTC");
        let connected = sender.signals.snapshot(Provider::QuickNodeGrpc, "BTC");
        assert!(connected.connection_up);
        assert_eq!(connected.last_message_wall_ms, 0);
        drop(guard);
        let disconnected = sender.signals.snapshot(Provider::QuickNodeGrpc, "BTC");
        assert!(!disconnected.connection_up);
        assert_eq!(disconnected.last_message_wall_ms, 0);
    }
}

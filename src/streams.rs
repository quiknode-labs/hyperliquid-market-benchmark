use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, warn};

use crate::grpc::pb::order_book_streaming_client::OrderBookStreamingClient;
use crate::grpc::pb::streaming_client::StreamingClient;
use crate::grpc::pb::{
    BboBookRequest, FilterValues, L2BookRequest, Ping, StreamSubscribe, StreamType,
    SubscribeRequest,
};
use crate::model::{
    ContentKey, Dataset, EventKey, LevelKey, MarketEvent, ProbeEvent, ProbeSender, Provider,
};

const L2_BOOK_DEPTH: u32 = 20;
const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(15);
const HEALTHY_CONNECTION: Duration = Duration::from_secs(30);
const WS_HEARTBEAT: Duration = Duration::from_secs(20);
const WS_READ_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_WS_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_GRPC_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_FILL_GRPC_MESSAGE_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub struct StreamConfig {
    pub dataset: Dataset,
    pub coins: Vec<String>,
    pub foundation_ws: String,
    pub hydromancer_ws: String,
    pub hydromancer_token: String,
    pub quicknode_grpc: String,
    pub quicknode_token: String,
}

pub fn spawn_streams(config: StreamConfig, sender: ProbeSender) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::with_capacity(config.coins.len() * config.dataset.providers().len());
    for coin in config.coins.clone() {
        tasks.push(tokio::spawn(run_foundation(
            config.foundation_ws.clone(),
            coin.clone(),
            config.dataset,
            sender.clone(),
        )));
        if config
            .dataset
            .providers()
            .contains(&Provider::HydromancerWs)
        {
            tasks.push(tokio::spawn(run_hydromancer(
                config.hydromancer_ws.clone(),
                config.hydromancer_token.clone(),
                coin.clone(),
                config.dataset,
                sender.clone(),
            )));
        }
        tasks.push(tokio::spawn(run_quicknode(
            config.quicknode_grpc.clone(),
            config.quicknode_token.clone(),
            coin,
            config.dataset,
            sender.clone(),
        )));
    }
    tasks
}

async fn run_foundation(url: String, coin: String, dataset: Dataset, sender: ProbeSender) {
    let mut backoff = ReconnectBackoff::default();
    loop {
        let generation = sender
            .stream_snapshot(Provider::FoundationWs, &coin)
            .connection_generation;
        match run_foundation_once(&url, &coin, dataset, &sender).await {
            Ok(()) => warn!(%coin, dataset = dataset.label(), "Foundation stream ended"),
            Err(error) => {
                warn!(%coin, dataset = dataset.label(), ?error, "Foundation stream disconnected")
            }
        }
        if !sender
            .send(ProbeEvent::Reconnect {
                provider: Provider::FoundationWs,
                coin: coin.clone(),
            })
            .await
        {
            return;
        }
        tokio::time::sleep(backoff.after_connection(connection_duration(
            &sender,
            Provider::FoundationWs,
            &coin,
            generation,
        )))
        .await;
    }
}

async fn run_hydromancer(
    endpoint: String,
    token: String,
    coin: String,
    dataset: Dataset,
    sender: ProbeSender,
) {
    let mut resume = HydromancerResume::default();
    let mut backoff = ReconnectBackoff::default();
    loop {
        let generation = sender
            .stream_snapshot(Provider::HydromancerWs, &coin)
            .connection_generation;
        match run_hydromancer_once(&endpoint, &token, &coin, dataset, &sender, &mut resume).await {
            Ok(()) => warn!(%coin, dataset = dataset.label(), "Hydromancer stream ended"),
            Err(error) => {
                warn!(%coin, dataset = dataset.label(), ?error, "Hydromancer stream disconnected")
            }
        }
        if !sender
            .send(ProbeEvent::Reconnect {
                provider: Provider::HydromancerWs,
                coin: coin.clone(),
            })
            .await
        {
            return;
        }
        tokio::time::sleep(backoff.after_connection(connection_duration(
            &sender,
            Provider::HydromancerWs,
            &coin,
            generation,
        )))
        .await;
    }
}

async fn run_quicknode(
    endpoint: String,
    token: String,
    coin: String,
    dataset: Dataset,
    sender: ProbeSender,
) {
    let mut backoff = ReconnectBackoff::default();
    loop {
        let generation = sender
            .stream_snapshot(Provider::QuickNodeGrpc, &coin)
            .connection_generation;
        match run_quicknode_once(&endpoint, &token, &coin, dataset, &sender).await {
            Ok(()) => warn!(%coin, dataset = dataset.label(), "Quicknode gRPC stream ended"),
            Err(error) => {
                warn!(%coin, dataset = dataset.label(), ?error, "Quicknode gRPC stream disconnected")
            }
        }
        if !sender
            .send(ProbeEvent::Reconnect {
                provider: Provider::QuickNodeGrpc,
                coin: coin.clone(),
            })
            .await
        {
            return;
        }
        tokio::time::sleep(backoff.after_connection(connection_duration(
            &sender,
            Provider::QuickNodeGrpc,
            &coin,
            generation,
        )))
        .await;
    }
}

#[derive(Debug)]
struct ReconnectBackoff {
    next: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            next: INITIAL_RECONNECT_BACKOFF,
        }
    }
}

impl ReconnectBackoff {
    fn after_connection(&mut self, connected_for: Duration) -> Duration {
        if connected_for >= HEALTHY_CONNECTION {
            self.next = INITIAL_RECONNECT_BACKOFF;
        }
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(MAX_RECONNECT_BACKOFF);
        delay
    }
}

fn connection_duration(
    sender: &ProbeSender,
    provider: Provider,
    coin: &str,
    generation_before: u64,
) -> Duration {
    let snapshot = sender.stream_snapshot(provider, coin);
    if snapshot.connection_generation == generation_before || snapshot.connected_at_wall_ms == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(now_ms().saturating_sub(snapshot.connected_at_wall_ms))
}

async fn run_foundation_once(
    url: &str,
    coin: &str,
    dataset: Dataset,
    sender: &ProbeSender,
) -> Result<()> {
    validate_public_ws_endpoint(url, "Foundation")?;
    let (ws, _) = tokio_tungstenite::connect_async_with_config(url, Some(websocket_config()), true)
        .await
        .context("connect Foundation websocket")?;
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Text(
        websocket_subscription(dataset, coin, WsSubscriptionMode::Standard)
            .to_string()
            .into(),
    ))
    .await?;
    let _connection = sender.connected(Provider::FoundationWs, coin);
    let mut heartbeat = heartbeat_interval();
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if last_frame.elapsed() >= WS_READ_TIMEOUT {
                    anyhow::bail!("Foundation websocket read deadline exceeded");
                }
                sink.send(Message::Ping(Vec::new().into())).await?;
            }
            message = stream.next() => {
                let Some(message) = message else { return Ok(()); };
                let message = message?;
                last_frame = Instant::now();
                let text = match message {
                    Message::Text(text) => text.to_string(),
                    Message::Binary(data) => String::from_utf8_lossy(&data).into_owned(),
                    Message::Ping(data) => {
                        sink.send(Message::Pong(data)).await?;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => return Ok(()),
                    _ => continue,
                };
                let Some(frame) = parse_ws_frame(&text) else {
                    debug!("ignoring malformed Foundation message");
                    continue;
                };
                if frame.channel.as_deref() == Some("error") {
                    anyhow::bail!("Foundation rejected the {} subscription", dataset.label());
                }
                let parsed_events = if dataset == Dataset::Fills {
                    parse_ws_trades(frame)
                } else {
                    parse_ws_book(frame, dataset)
                        .map(|parsed| vec![parsed.key])
                        .unwrap_or_default()
                };
                if parsed_events.is_empty() {
                    debug!("ignoring non-benchmark Foundation message");
                    continue;
                }
                let received = Instant::now();
                let received_wall_ms = now_ms();
                for key in parsed_events {
                    if key.coin != coin {
                        warn!(expected = coin, actual = key.coin, "ignoring Foundation update for unexpected coin");
                        continue;
                    }
                    if !sender.send(ProbeEvent::Market(MarketEvent {
                        provider: Provider::FoundationWs,
                        key,
                        received,
                        received_wall_ms,
                    })).await {
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn run_hydromancer_once(
    endpoint: &str,
    token: &str,
    coin: &str,
    dataset: Dataset,
    sender: &ProbeSender,
    resume: &mut HydromancerResume,
) -> Result<()> {
    let authenticated_url = hydromancer_authenticated_url(
        endpoint,
        token,
        resume.session_id.as_deref(),
        resume.cursor.as_deref(),
    )?;
    let (ws, _) = tokio_tungstenite::connect_async_with_config(
        authenticated_url,
        Some(websocket_config()),
        true,
    )
    .await
    .map_err(|_| anyhow::anyhow!("authenticated Hydromancer connection failed"))?;
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Text(
        websocket_subscription(dataset, coin, WsSubscriptionMode::Hydromancer)
            .to_string()
            .into(),
    ))
    .await?;
    let mut connection = None;
    let mut heartbeat = heartbeat_interval();
    let mut last_frame = Instant::now();
    let mut last_seq = None;
    let mut replay_remaining = 0u64;

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if last_frame.elapsed() >= WS_READ_TIMEOUT {
                    anyhow::bail!("Hydromancer websocket read deadline exceeded");
                }
                sink.send(Message::Ping(Vec::new().into())).await?;
            }
            message = stream.next() => {
                let Some(message) = message else { return Ok(()); };
                let message = message?;
                last_frame = Instant::now();
                let text = match message {
                    Message::Text(text) => text.to_string(),
                    Message::Binary(data) => String::from_utf8_lossy(&data).into_owned(),
                    Message::Ping(data) => {
                        sink.send(Message::Pong(data)).await?;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => return Ok(()),
                    _ => continue,
                };

                let Some(frame) = parse_ws_frame(&text) else {
                    debug!("ignoring malformed Hydromancer message");
                    continue;
                };
                match frame.message_type.as_deref() {
                        Some("welcome") => {
                            if let Some(session_id) = frame.session_id {
                                resume.session_id = Some(session_id);
                            }
                            continue;
                        }
                        Some("ping") => {
                            sink.send(Message::Text(r#"{"type":"pong"}"#.into())).await?;
                            continue;
                        }
                        Some("subscriptionUpdate") => {
                            if !frame.failed.is_empty()
                                || !hydromancer_ack_includes(&frame.subscribed, dataset.channel())
                            {
                                anyhow::bail!("Hydromancer rejected the {} subscription", dataset.label());
                            }
                            if connection.is_none() {
                                connection = Some(sender.connected(Provider::HydromancerWs, coin));
                            }
                            continue;
                        }
                        Some("replay") => {
                            if let Some(cursor) = frame.cursor {
                                resume.cursor = Some(cursor);
                            }
                            replay_remaining = frame.count.unwrap_or(0);
                            if !sender.send(ProbeEvent::Replay {
                                provider: Provider::HydromancerWs,
                                coin: coin.to_owned(),
                                messages: replay_remaining,
                                has_gap: frame.has_gap,
                            }).await {
                                return Ok(());
                            }
                            continue;
                        }
                        Some("error") => {
                            *resume = HydromancerResume::default();
                            anyhow::bail!("Hydromancer server returned an error");
                        }
                        _ => {}
                    }

                let Some(parsed) = parse_ws_book(frame, dataset) else {
                    continue;
                };
                if parsed.key.coin != coin {
                    warn!(expected = coin, actual = parsed.key.coin, "ignoring Hydromancer update for unexpected coin");
                    continue;
                }
                let ParsedWsBook { key, seq, cursor } = parsed;
                // The benchmark boundary is canonical-book readiness. Sequence/replay
                // accounting is runner bookkeeping and must not be charged only to
                // Hydromancer's measured latency.
                let received = Instant::now();
                let received_wall_ms = now_ms();
                if let Some(seq) = seq {
                    if last_seq.is_some_and(|last| seq <= last) {
                        continue;
                    }
                    let missing = last_seq.map(|last| seq.saturating_sub(last + 1)).unwrap_or(0);
                    if missing > 0 && !sender.send(ProbeEvent::SequenceGap {
                        provider: Provider::HydromancerWs,
                        coin: coin.to_owned(),
                        missing,
                    }).await {
                        return Ok(());
                    }
                    last_seq = Some(seq);
                }
                if replay_remaining > 0 {
                    replay_remaining -= 1;
                    if let Some(cursor) = cursor {
                        resume.cursor = Some(cursor);
                    }
                    continue;
                }
                if !sender.send(ProbeEvent::Market(MarketEvent {
                    provider: Provider::HydromancerWs,
                    key,
                    received,
                    received_wall_ms,
                })).await {
                    return Ok(());
                }
                if let Some(cursor) = cursor {
                    resume.cursor = Some(cursor);
                }
            }
        }
    }
}

async fn run_quicknode_once(
    endpoint: &str,
    token: &str,
    coin: &str,
    dataset: Dataset,
    sender: &ProbeSender,
) -> Result<()> {
    let channel = grpc_channel(endpoint).await?;
    let mut client = OrderBookStreamingClient::new(channel.clone())
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(64 * 1024);

    match dataset {
        Dataset::Bbo => {
            let mut request = tonic::Request::new(BboBookRequest {
                coins: vec![coin.to_owned()],
            });
            request
                .metadata_mut()
                .insert("x-token", MetadataValue::try_from(token)?);
            let mut stream = client.stream_bbo_book(request).await?.into_inner();
            let _connection = sender.connected(Provider::QuickNodeGrpc, coin);
            loop {
                let update = tokio::time::timeout(WS_READ_TIMEOUT, stream.message())
                    .await
                    .context("Quicknode BBO gRPC application-message deadline exceeded")??;
                let Some(update) = update else { break };
                if update.coin != coin || update.time == 0 {
                    continue;
                }
                let bid = match update.bid {
                    Some(level) => match canonical_level(level.px, level.sz, level.n) {
                        Some((level, _)) => Some(level),
                        None => continue,
                    },
                    None => None,
                };
                let ask = match update.ask {
                    Some(level) => match canonical_level(level.px, level.sz, level.n) {
                        Some((level, _)) => Some(level),
                        None => continue,
                    },
                    None => None,
                };
                if bid.is_none() && ask.is_none() || book_is_crossed(bid.as_ref(), ask.as_ref()) {
                    continue;
                }
                let key = EventKey {
                    coin: update.coin,
                    event_ms: update.time,
                    content: ContentKey::Bbo { bid, ask },
                };
                let received = Instant::now();
                let received_wall_ms = now_ms();
                if !sender
                    .send(ProbeEvent::Market(MarketEvent {
                        provider: Provider::QuickNodeGrpc,
                        key,
                        received,
                        received_wall_ms,
                    }))
                    .await
                {
                    return Ok(());
                }
            }
        }
        Dataset::L2book => {
            let mut request = tonic::Request::new(L2BookRequest {
                coin: coin.to_owned(),
                n_levels: L2_BOOK_DEPTH,
                n_sig_figs: None,
                mantissa: None,
            });
            request
                .metadata_mut()
                .insert("x-token", MetadataValue::try_from(token)?);
            let mut stream = client.stream_l2_book(request).await?.into_inner();
            let _connection = sender.connected(Provider::QuickNodeGrpc, coin);
            loop {
                let update = tokio::time::timeout(WS_READ_TIMEOUT, stream.message())
                    .await
                    .context("Quicknode L2 gRPC application-message deadline exceeded")??;
                let Some(update) = update else { break };
                if update.coin != coin
                    || update.time == 0
                    || update.bids.len() != L2_BOOK_DEPTH as usize
                    || update.asks.len() != L2_BOOK_DEPTH as usize
                {
                    continue;
                }
                let Some(bids) = canonical_side(
                    update
                        .bids
                        .into_iter()
                        .map(|level| (level.px, level.sz, level.n)),
                    true,
                ) else {
                    continue;
                };
                let Some(asks) = canonical_side(
                    update
                        .asks
                        .into_iter()
                        .map(|level| (level.px, level.sz, level.n)),
                    false,
                ) else {
                    continue;
                };
                if book_is_crossed(bids.first(), asks.first()) {
                    continue;
                }
                let key = EventKey {
                    coin: update.coin,
                    event_ms: update.time,
                    content: ContentKey::L2 { bids, asks },
                };
                let received = Instant::now();
                let received_wall_ms = now_ms();
                if !sender
                    .send(ProbeEvent::Market(MarketEvent {
                        provider: Provider::QuickNodeGrpc,
                        key,
                        received,
                        received_wall_ms,
                    }))
                    .await
                {
                    return Ok(());
                }
            }
        }
        Dataset::Fills => {
            let mut client = StreamingClient::new(channel)
                .max_decoding_message_size(MAX_FILL_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(64 * 1024);
            let (request_tx, request_rx) = mpsc::channel(4);
            request_tx
                .send(SubscribeRequest {
                    request: Some(crate::grpc::pb::subscribe_request::Request::Subscribe(
                        StreamSubscribe {
                            stream_type: StreamType::Trades as i32,
                            start_block: 0,
                            filters: HashMap::from([(
                                "coin".to_owned(),
                                FilterValues {
                                    values: vec![coin.to_owned()],
                                },
                            )]),
                            filter_name: format!("benchmark-fills-{coin}"),
                        },
                    )),
                })
                .await
                .context("queue Quicknode fills subscription")?;
            let mut request = tonic::Request::new(ReceiverStream::new(request_rx));
            request
                .metadata_mut()
                .insert("x-token", MetadataValue::try_from(token)?);
            let mut stream = client.stream_data(request).await?.into_inner();
            let _connection = sender.connected(Provider::QuickNodeGrpc, coin);
            let mut heartbeat = heartbeat_interval();

            loop {
                tokio::select! {
                    update = stream.message() => {
                        let Some(update) = update
                            .context("Quicknode fills gRPC stream failed")?
                        else {
                            break;
                        };
                        let Some(crate::grpc::pb::subscribe_update::Update::Data(data)) =
                            update.update
                        else {
                            continue;
                        };
                        let events = parse_quicknode_fill_batch(&data.data);
                        if events.is_empty() {
                            continue;
                        }
                        // One receipt boundary covers the fully decoded and canonicalized
                        // block payload. Matching and publication happen after this point.
                        let received = Instant::now();
                        let received_wall_ms = now_ms();
                        for key in events {
                            if key.coin != coin {
                                continue;
                            }
                            if !sender
                                .send(ProbeEvent::Market(MarketEvent {
                                    provider: Provider::QuickNodeGrpc,
                                    key,
                                    received,
                                    received_wall_ms,
                                }))
                                .await
                            {
                                return Ok(());
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        request_tx
                            .send(SubscribeRequest {
                                request: Some(
                                    crate::grpc::pb::subscribe_request::Request::Ping(Ping {
                                        timestamp: chrono::Utc::now().timestamp_millis(),
                                    }),
                                ),
                            })
                            .await
                            .context("send Quicknode fills heartbeat")?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn heartbeat_interval() -> tokio::time::Interval {
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + WS_HEARTBEAT, WS_HEARTBEAT);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

async fn grpc_channel(endpoint: &str) -> Result<Channel> {
    let normalized = validate_grpc_endpoint(endpoint)?;
    let endpoint = Endpoint::from_shared(normalized.clone())?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(3))
        .keep_alive_while_idle(true)
        .connect_timeout(Duration::from_secs(10));
    if normalized.starts_with("https://") {
        endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots())?
            .connect()
            .await
            .map_err(Into::into)
    } else {
        endpoint.connect().await.map_err(Into::into)
    }
}

fn normalize_grpc_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("https://{endpoint}")
    }
}

fn validate_grpc_endpoint(endpoint: &str) -> Result<String> {
    let normalized = normalize_grpc_endpoint(endpoint);
    let url = url::Url::parse(&normalized).context("invalid Quicknode gRPC endpoint")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        anyhow::bail!("Quicknode gRPC endpoint must be an HTTP or HTTPS origin");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Quicknode gRPC endpoint must not contain credentials");
    }
    if url.query().is_some() || url.fragment().is_some() || !matches!(url.path(), "" | "/") {
        anyhow::bail!(
            "Quicknode gRPC endpoint must be an origin without a path, query, or fragment"
        );
    }
    if url.scheme() != "https" {
        anyhow::bail!("Quicknode gRPC endpoint must use HTTPS");
    }
    let host = url.host_str().expect("host checked above");
    let endpoint_name = host
        .strip_suffix(".hype-mainnet.quiknode.pro")
        .filter(|name| {
            !name.is_empty()
                && !name.starts_with('-')
                && !name.ends_with('-')
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        });
    if endpoint_name.is_none() || url.port() != Some(10_000) {
        anyhow::bail!(
            "Quicknode gRPC endpoint must be a public *.hype-mainnet.quiknode.pro origin on port 10000"
        );
    }
    Ok(normalized)
}

#[derive(Debug, Clone, Copy)]
enum WsSubscriptionMode {
    Standard,
    Hydromancer,
}

fn websocket_subscription(
    dataset: Dataset,
    coin: &str,
    mode: WsSubscriptionMode,
) -> serde_json::Value {
    match (dataset, mode) {
        (Dataset::Bbo, _) => serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": coin},
        }),
        (Dataset::L2book, WsSubscriptionMode::Hydromancer) => serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coins": [coin],
                "nLevels": L2_BOOK_DEPTH,
            },
        }),
        (Dataset::L2book, WsSubscriptionMode::Standard) => serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": coin},
        }),
        (Dataset::Fills, WsSubscriptionMode::Standard) => serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "trades", "coin": coin},
        }),
        (Dataset::Fills, WsSubscriptionMode::Hydromancer) => {
            unreachable!("fills do not use a Hydromancer comparison stream")
        }
    }
}

fn validate_public_ws_endpoint(endpoint: &str, provider: &str) -> Result<()> {
    let url = url::Url::parse(endpoint).with_context(|| format!("invalid {provider} endpoint"))?;
    if !matches!(url.scheme(), "ws" | "wss") || url.host_str().is_none() {
        anyhow::bail!("{provider} endpoint must be a websocket URL");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("{provider} endpoint must not contain credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("{provider} endpoint must not contain a query or fragment");
    }
    if url.scheme() == "ws" && !url.host_str().is_some_and(is_loopback_host) {
        anyhow::bail!("{provider} endpoint must use WSS except on loopback");
    }
    Ok(())
}

fn websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_write_buffer_size = 2 * MAX_WS_MESSAGE_BYTES;
    config.max_message_size = Some(MAX_WS_MESSAGE_BYTES);
    config.max_frame_size = Some(MAX_WS_MESSAGE_BYTES);
    config
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn validate_hydromancer_endpoint(endpoint: &str) -> Result<()> {
    validate_public_ws_endpoint(endpoint, "Hydromancer")?;
    Ok(())
}

fn hydromancer_authenticated_url(
    endpoint: &str,
    token: &str,
    session_id: Option<&str>,
    cursor: Option<&str>,
) -> Result<String> {
    validate_hydromancer_endpoint(endpoint)?;
    let mut url = url::Url::parse(endpoint).context("invalid Hydromancer endpoint")?;
    url.query_pairs_mut().append_pair("token", token);
    if let Some(session_id) = session_id {
        url.query_pairs_mut().append_pair("sessionId", session_id);
    }
    if let Some(cursor) = cursor {
        url.query_pairs_mut().append_pair("cursor", cursor);
    }
    Ok(url.into())
}

#[derive(Debug, Deserialize)]
struct WsFrame {
    #[serde(default)]
    channel: Option<String>,
    data: Option<serde_json::Value>,
    #[serde(default)]
    seq: Option<u64>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(rename = "type", default)]
    message_type: Option<String>,
    #[serde(rename = "sessionId", default)]
    session_id: Option<String>,
    #[serde(default)]
    count: Option<u64>,
    #[serde(rename = "hasGap", default)]
    has_gap: bool,
    #[serde(default)]
    failed: Vec<serde_json::Value>,
    #[serde(default)]
    subscribed: Vec<serde_json::Value>,
}

struct ParsedWsBook {
    key: EventKey,
    seq: Option<u64>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WsBboData {
    coin: String,
    time: u64,
    bbo: [Option<WsLevel>; 2],
}

#[derive(Debug, Deserialize)]
struct WsL2Data {
    coin: String,
    time: u64,
    levels: [Vec<WsLevel>; 2],
}

#[derive(Debug, Deserialize)]
struct WsTrade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [String; 2],
}

#[derive(Debug, Deserialize)]
struct QuickNodeFillBatch {
    events: Vec<(String, QuickNodeFill)>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuickNodeFill {
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
    hash: String,
    crossed: bool,
    tid: u64,
}

type QuickNodeFillPair = [Option<(String, QuickNodeFill)>; 2];

#[derive(Debug, Deserialize)]
struct WsLevel {
    px: String,
    sz: String,
    n: u32,
}

#[derive(Default)]
struct HydromancerResume {
    session_id: Option<String>,
    cursor: Option<String>,
}

fn parse_ws_frame(text: &str) -> Option<WsFrame> {
    serde_json::from_str(text).ok()
}

fn parse_ws_book(frame: WsFrame, dataset: Dataset) -> Option<ParsedWsBook> {
    if frame.channel.as_deref() != Some(dataset.channel()) {
        return None;
    }
    let raw_data = frame.data?;
    let key = match dataset {
        Dataset::Bbo => {
            let data = serde_json::from_value::<WsBboData>(raw_data).ok()?;
            if data.coin.is_empty() || data.time == 0 {
                return None;
            }
            let [bid, ask] = data.bbo;
            let bid = match bid {
                Some(level) => Some(canonical_level(level.px, level.sz, level.n)?.0),
                None => None,
            };
            let ask = match ask {
                Some(level) => Some(canonical_level(level.px, level.sz, level.n)?.0),
                None => None,
            };
            if bid.is_none() && ask.is_none() || book_is_crossed(bid.as_ref(), ask.as_ref()) {
                return None;
            }
            EventKey {
                coin: data.coin,
                event_ms: data.time,
                content: ContentKey::Bbo { bid, ask },
            }
        }
        Dataset::L2book => {
            let raw_data = match raw_data {
                serde_json::Value::Array(mut batch) => {
                    if batch.len() != 1 {
                        return None;
                    }
                    batch.pop()?
                }
                value => value,
            };
            let data = serde_json::from_value::<WsL2Data>(raw_data).ok()?;
            if data.coin.is_empty()
                || data.time == 0
                || data.levels[0].len() != L2_BOOK_DEPTH as usize
                || data.levels[1].len() != L2_BOOK_DEPTH as usize
            {
                return None;
            }
            let [bids, asks] = data.levels;
            let bids = canonical_side(
                bids.into_iter().map(|level| (level.px, level.sz, level.n)),
                true,
            )?;
            let asks = canonical_side(
                asks.into_iter().map(|level| (level.px, level.sz, level.n)),
                false,
            )?;
            if book_is_crossed(bids.first(), asks.first()) {
                return None;
            }
            EventKey {
                coin: data.coin,
                event_ms: data.time,
                content: ContentKey::L2 { bids, asks },
            }
        }
        Dataset::Fills => return None,
    };
    Some(ParsedWsBook {
        key,
        seq: frame.seq,
        cursor: frame.cursor,
    })
}

fn parse_ws_trades(frame: WsFrame) -> Vec<EventKey> {
    if frame.channel.as_deref() != Some(Dataset::Fills.channel()) {
        return Vec::new();
    }
    let Some(data) = frame.data else {
        return Vec::new();
    };
    let Ok(trades) = serde_json::from_value::<Vec<WsTrade>>(data) else {
        return Vec::new();
    };
    trades.into_iter().filter_map(canonical_ws_trade).collect()
}

fn canonical_ws_trade(trade: WsTrade) -> Option<EventKey> {
    canonical_trade(
        trade.coin,
        trade.side,
        trade.px,
        trade.sz,
        trade.hash,
        trade.time,
        trade.tid,
        trade.users,
    )
}

fn parse_quicknode_fill_batch(payload: &str) -> Vec<EventKey> {
    let Ok(batch) = serde_json::from_str::<QuickNodeFillBatch>(payload) else {
        return Vec::new();
    };
    let mut pairs: HashMap<(String, u64, u64), QuickNodeFillPair> = HashMap::new();
    for (user, fill) in batch.events {
        let side_index = match fill.side.as_str() {
            "A" => 0,
            "B" => 1,
            _ => continue,
        };
        let key = (fill.coin.clone(), fill.time, fill.tid);
        let pair = pairs.entry(key).or_insert_with(|| [None, None]);
        if pair[side_index].is_none() {
            pair[side_index] = Some((user, fill));
        }
    }

    pairs
        .into_values()
        .filter_map(|[ask, bid]| {
            let (seller, ask) = ask?;
            let (buyer, bid) = bid?;
            let ask_px = canonical_positive_decimal(ask.px)?;
            let bid_px = canonical_positive_decimal(bid.px)?;
            let ask_sz = canonical_positive_decimal(ask.sz)?;
            let bid_sz = canonical_positive_decimal(bid.sz)?;
            if ask.coin != bid.coin
                || ask.time != bid.time
                || ask.tid != bid.tid
                || ask_px != bid_px
                || ask_sz != bid_sz
                || !ask.hash.eq_ignore_ascii_case(&bid.hash)
                || ask.crossed == bid.crossed
            {
                return None;
            }
            let side = if ask.crossed { "A" } else { "B" }.to_owned();
            canonical_trade(
                ask.coin,
                side,
                ask_px,
                ask_sz,
                ask.hash,
                ask.time,
                ask.tid,
                [buyer, seller],
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn canonical_trade(
    coin: String,
    side: String,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [String; 2],
) -> Option<EventKey> {
    if coin.is_empty()
        || !matches!(side.as_str(), "A" | "B")
        || hash.is_empty()
        || time == 0
        || users.iter().any(String::is_empty)
    {
        return None;
    }
    let px = canonical_positive_decimal(px)?;
    let sz = canonical_positive_decimal(sz)?;
    Some(EventKey {
        coin,
        event_ms: time,
        content: ContentKey::Trade {
            tid,
            side,
            px,
            sz,
            hash: hash.to_ascii_lowercase(),
            users: users.map(|user| user.to_ascii_lowercase()),
        },
    })
}

fn canonical_positive_decimal(value: String) -> Option<String> {
    let value = Decimal::from_str(&value).ok()?;
    (value > Decimal::ZERO).then(|| value.normalize().to_string())
}

fn canonical_level(px: String, sz: String, n: u32) -> Option<(LevelKey, Decimal)> {
    let px_value = Decimal::from_str(&px).ok()?;
    let sz_value = Decimal::from_str(&sz).ok()?;
    if px_value <= Decimal::ZERO || sz_value <= Decimal::ZERO || n == 0 {
        return None;
    }
    Some((
        LevelKey {
            px: px_value.normalize().to_string(),
            sz: sz_value.normalize().to_string(),
            n,
        },
        px_value,
    ))
}

fn canonical_side<I>(levels: I, descending: bool) -> Option<Vec<LevelKey>>
where
    I: IntoIterator<Item = (String, String, u32)>,
{
    let mut result = Vec::new();
    let mut previous = None;
    for (px, sz, n) in levels {
        let (key, price) = canonical_level(px, sz, n)?;
        if let Some(previous) = previous
            && (descending && previous <= price || !descending && previous >= price)
        {
            return None;
        }
        previous = Some(price);
        result.push(key);
    }
    Some(result)
}

fn book_is_crossed(bid: Option<&LevelKey>, ask: Option<&LevelKey>) -> bool {
    match (bid, ask) {
        (Some(bid), Some(ask)) => {
            let Ok(bid) = Decimal::from_str(&bid.px) else {
                return true;
            };
            let Ok(ask) = Decimal::from_str(&ask.px) else {
                return true;
            };
            bid >= ask
        }
        _ => false,
    }
}

fn hydromancer_ack_includes(values: &[serde_json::Value], channel: &str) -> bool {
    values.iter().any(|value| {
        value.as_str() == Some(channel)
            || value.get("type").and_then(serde_json::Value::as_str) == Some(channel)
    })
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

    #[test]
    fn backoff_is_bounded_and_resets_after_a_healthy_connection() {
        let mut backoff = ReconnectBackoff::default();
        let observed = (0..10)
            .map(|_| backoff.after_connection(Duration::from_secs(1)))
            .collect::<Vec<_>>();
        assert_eq!(observed[0], Duration::from_millis(500));
        assert_eq!(observed.last(), Some(&MAX_RECONNECT_BACKOFF));
        assert_eq!(
            backoff.after_connection(HEALTHY_CONNECTION),
            INITIAL_RECONNECT_BACKOFF
        );
    }

    #[test]
    fn parses_and_canonicalizes_bbo_without_crossed_books() {
        let frame = parse_ws_frame(
            r#"{"channel":"bbo","data":{"coin":"BTC","time":42,"bbo":[{"px":"100.00","sz":"2.0","n":1},{"px":"101","sz":"3","n":2}]}}"#,
        )
        .unwrap();
        let parsed = parse_ws_book(frame, Dataset::Bbo).unwrap();
        assert_eq!(parsed.key.event_ms, 42);
        assert_eq!(
            parsed.key.content,
            ContentKey::Bbo {
                bid: Some(LevelKey {
                    px: "100".to_owned(),
                    sz: "2".to_owned(),
                    n: 1,
                }),
                ask: Some(LevelKey {
                    px: "101".to_owned(),
                    sz: "3".to_owned(),
                    n: 2,
                }),
            }
        );
        let crossed = parse_ws_frame(
            r#"{"channel":"bbo","data":{"coin":"BTC","time":42,"bbo":[{"px":"101","sz":"2","n":1},{"px":"101","sz":"3","n":2}]}}"#,
        )
        .unwrap();
        assert!(parse_ws_book(crossed, Dataset::Bbo).is_none());
    }

    #[test]
    fn quicknode_fill_pairs_and_foundation_trades_share_one_canonical_key() {
        let foundation = parse_ws_trades(
            parse_ws_frame(
                r#"{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"100.00","sz":"2.0","hash":"0xABC","time":42,"tid":7,"users":["0xBUYER","0xSELLER"]}]}"#,
            )
            .unwrap(),
        );
        let quicknode = parse_quicknode_fill_batch(
            r#"{"local_time":"1970-01-01T00:00:00.100","block_time":"1970-01-01T00:00:00.042","block_number":1,"events":[["0xSELLER",{"coin":"BTC","px":"100.0","sz":"2.00","side":"A","time":42,"hash":"0xabc","crossed":true,"tid":7}],["0xBUYER",{"coin":"BTC","px":"100.0","sz":"2.00","side":"B","time":42,"hash":"0xABC","crossed":false,"tid":7}]]}"#,
        );

        assert_eq!(foundation, quicknode);
        assert_eq!(foundation.len(), 1);
        assert_eq!(
            foundation[0].content,
            ContentKey::Trade {
                tid: 7,
                side: "A".to_owned(),
                px: "100".to_owned(),
                sz: "2".to_owned(),
                hash: "0xabc".to_owned(),
                users: ["0xbuyer".to_owned(), "0xseller".to_owned()],
            }
        );
        assert_eq!(foundation[0].base().trade_id, Some(7));
    }

    #[test]
    fn fills_subscription_is_public_trades_and_has_two_expected_sources() {
        let subscription =
            websocket_subscription(Dataset::Fills, "BTC", WsSubscriptionMode::Standard);
        assert_eq!(subscription["subscription"]["type"], "trades");
        assert_eq!(subscription["subscription"]["coin"], "BTC");
        assert_eq!(
            Dataset::Fills.providers(),
            &[Provider::FoundationWs, Provider::QuickNodeGrpc]
        );
    }

    #[test]
    fn parses_exactly_twenty_l2_levels_and_rejects_short_books() {
        let bids = (81..=100)
            .rev()
            .map(|px| serde_json::json!({"px": px.to_string(), "sz": "1", "n": 1}))
            .collect::<Vec<_>>();
        let asks = (101..=120)
            .map(|px| serde_json::json!({"px": px.to_string(), "sz": "1", "n": 1}))
            .collect::<Vec<_>>();
        let envelope = serde_json::json!({
            "channel": "l2Book",
            "data": {"coin": "BTC", "time": 42, "levels": [bids, asks]},
        });
        let parsed = parse_ws_book(
            parse_ws_frame(&envelope.to_string()).unwrap(),
            Dataset::L2book,
        )
        .unwrap();
        let ContentKey::L2 { bids, asks } = parsed.key.content else {
            panic!("expected L2 book");
        };
        assert_eq!(bids.len(), L2_BOOK_DEPTH as usize);
        assert_eq!(asks.len(), L2_BOOK_DEPTH as usize);

        let short = serde_json::json!({
            "channel": "l2Book",
            "data": {"coin": "BTC", "time": 42, "levels": [[], []]},
        });
        assert!(
            parse_ws_book(parse_ws_frame(&short.to_string()).unwrap(), Dataset::L2book).is_none()
        );
    }

    #[test]
    fn one_outer_frame_representation_covers_books_errors_and_controls() {
        let book = parse_ws_frame(
            r#"{"channel":"bbo","seq":7,"cursor":"next","data":{"coin":"BTC","time":42,"bbo":[{"px":"100","sz":"2","n":1},{"px":"101","sz":"3","n":2}]}}"#,
        )
        .unwrap();
        assert_eq!(book.channel.as_deref(), Some("bbo"));
        assert_eq!(book.seq, Some(7));
        assert!(parse_ws_book(book, Dataset::Bbo).is_some());

        let control = parse_ws_frame(
            r#"{"type":"subscriptionUpdate","subscribed":[{"type":"bbo"}],"failed":[]}"#,
        )
        .unwrap();
        assert_eq!(control.message_type.as_deref(), Some("subscriptionUpdate"));
        assert!(hydromancer_ack_includes(&control.subscribed, "bbo"));

        let error = parse_ws_frame(r#"{"channel":"error","data":{"message":"no"}}"#).unwrap();
        assert_eq!(error.channel.as_deref(), Some("error"));
    }

    #[test]
    fn l2_subscription_shape_is_source_specific_and_fixed_depth() {
        let standard = websocket_subscription(Dataset::L2book, "BTC", WsSubscriptionMode::Standard);
        let hydromancer =
            websocket_subscription(Dataset::L2book, "BTC", WsSubscriptionMode::Hydromancer);
        assert_eq!(standard["subscription"]["coin"], "BTC");
        assert_eq!(hydromancer["subscription"]["coins"][0], "BTC");
        assert_eq!(hydromancer["subscription"]["nLevels"], L2_BOOK_DEPTH);
    }

    #[test]
    fn hydromancer_endpoint_never_accepts_embedded_secrets() {
        assert!(
            validate_hydromancer_endpoint("wss://api.hydromancer.xyz/ws?token=secret").is_err()
        );
        let url = hydromancer_authenticated_url(
            "wss://api.hydromancer.xyz/ws",
            "secret",
            Some("session"),
            Some("cursor"),
        )
        .unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.query_pairs().count(), 3);
        assert!(validate_hydromancer_endpoint("ws://api.hydromancer.xyz/ws").is_err());
        assert!(validate_hydromancer_endpoint("ws://127.0.0.1:8080/ws").is_ok());
        assert!(validate_hydromancer_endpoint("wss://user:secret@api.hydromancer.xyz/ws").is_err());
        assert!(
            validate_hydromancer_endpoint("wss://api.hydromancer.xyz/ws?api_key=secret").is_err()
        );
    }

    #[test]
    fn grpc_endpoint_requires_the_public_quicknode_mainnet_origin() {
        assert_eq!(
            normalize_grpc_endpoint("example-guide-demo.hype-mainnet.quiknode.pro:10000"),
            "https://example-guide-demo.hype-mainnet.quiknode.pro:10000"
        );
        assert!(
            validate_grpc_endpoint("https://example-guide-demo.hype-mainnet.quiknode.pro:10000")
                .is_ok()
        );
        assert!(
            validate_grpc_endpoint(
                "https://user:secret@example-guide-demo.hype-mainnet.quiknode.pro:10000"
            )
            .is_err()
        );
        assert!(
            validate_grpc_endpoint(
                "https://example-guide-demo.hype-mainnet.quiknode.pro:10000/token"
            )
            .is_err()
        );
        assert!(
            validate_grpc_endpoint(
                "https://example-guide-demo.hype-mainnet.quiknode.pro:10000?token=secret"
            )
            .is_err()
        );
        assert!(validate_grpc_endpoint("https://internal.example:10000").is_err());
        assert!(
            validate_grpc_endpoint("https://example-guide-demo.hype-mainnet.quiknode.pro:443")
                .is_err()
        );
        assert!(
            validate_grpc_endpoint("https://example-guide-demo.hype-testnet.quiknode.pro:10000")
                .is_err()
        );
    }

    #[test]
    fn public_endpoint_requires_websocket_scheme() {
        assert!(validate_public_ws_endpoint("https://example.com/ws", "Foundation").is_err());
        assert!(validate_public_ws_endpoint("ws://example.com/ws", "Foundation").is_err());
        assert!(validate_public_ws_endpoint("ws://127.0.0.1:8080/ws", "Foundation").is_ok());
        assert!(validate_public_ws_endpoint("wss://example.com/ws", "Foundation").is_ok());
        assert!(
            validate_public_ws_endpoint("wss://example.com/ws?token=secret", "Foundation").is_err()
        );
    }

    #[tokio::test]
    async fn authenticated_grpc_rejects_plaintext_remote_endpoints() {
        let error = grpc_channel("http://example.com:10000")
            .await
            .expect_err("remote plaintext must be rejected");
        assert!(error.to_string().contains("must use HTTPS"));
    }

    #[test]
    fn websocket_messages_have_a_protocol_sized_hard_cap() {
        let config = websocket_config();
        assert_eq!(config.max_message_size, Some(MAX_WS_MESSAGE_BYTES));
        assert_eq!(config.max_frame_size, Some(MAX_WS_MESSAGE_BYTES));
    }
}

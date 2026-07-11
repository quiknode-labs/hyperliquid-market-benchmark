use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use axiom::{AxiomClient, spawn_axiom_worker};
use benchmark::{Benchmark, BenchmarkConfig};
use clap::Parser;
use model::{Dataset, ProbeEvent, ProbeSender, RuntimeSignals};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

mod axiom;
mod benchmark;
mod clock;
mod grpc;
mod model;
mod streams;

const EVENT_QUEUE_CAPACITY: usize = 16_384;
const ROLLING_WINDOW: Duration = Duration::from_secs(300);
const PUBLISH_INTERVAL: Duration = Duration::from_secs(30);
const COHORT_TIMEOUT: Duration = Duration::from_secs(5);
const STALE_AFTER: Duration = Duration::from_secs(60);
const MAX_COINS_PER_PROCESS: usize = 10;
const PUBLIC_CLOUDS: &[&str] = &["aws", "gcp", "oracle"];

#[derive(Debug, Parser)]
#[command(
    name = "hyperliquid-market-benchmark",
    about = "Continuously measure Hyperliquid event-to-canonical-book-ready latency from three matched sources"
)]
struct Args {
    /// Book dataset measured by this process. Run one process per dataset.
    #[arg(long, value_enum, default_value_t = Dataset::Bbo)]
    dataset: Dataset,

    /// Comma-separated coins measured by this process.
    #[arg(long, default_value = "BTC")]
    coins: String,

    /// Hyperliquid Foundation websocket URL.
    #[arg(long, default_value = "wss://api.hyperliquid.xyz/ws")]
    foundation_ws: String,

    /// Hydromancer websocket URL. Authentication is read only from HYDROMANCER_API_KEY.
    #[arg(long, default_value = "wss://api.hydromancer.xyz/ws")]
    hydromancer_ws: String,

    /// Quicknode Hyperliquid gRPC endpoint. Authentication is read only from QUICKNODE_HYPERLIQUID_TOKEN.
    #[arg(long, env = "QUICKNODE_HYPERLIQUID_GRPC_URL")]
    quicknode_grpc: String,

    /// Cloud measurement location. Inferred from the public runner ID when absent.
    #[arg(long, env = "BENCHMARK_CLOUD")]
    cloud: Option<String>,

    /// Logical comparison region. Inferred from the public runner ID when absent.
    #[arg(long, env = "BENCHMARK_REGION")]
    region: Option<String>,

    /// Physical observer metro. Inferred from the public runner ID when absent.
    #[arg(long, env = "BENCHMARK_METRO")]
    metro: Option<String>,

    /// Public runner ID. Never use an SSH, inventory, or private infrastructure hostname.
    #[arg(long, env = "BENCHMARK_RUNNER_ID")]
    runner: String,

    /// Axiom dataset receiving latency_window events.
    #[arg(
        long,
        env = "AXIOM_DATASET",
        default_value = "hyperliquid-market-benchmark"
    )]
    axiom_dataset: String,

    /// Axiom API origin. Override only for an edge deployment or local test endpoint.
    #[arg(long, env = "AXIOM_URL", default_value = "https://api.axiom.co")]
    axiom_url: String,

    /// Root for the persistent Axiom outbox. A dataset subdirectory is added automatically.
    #[arg(
        long,
        env = "BENCHMARK_OUTBOX_DIR",
        default_value = "/var/lib/hyperliquid-market-benchmark"
    )]
    outbox_dir: PathBuf,

    /// Maximum persisted latency-window files per dataset process.
    #[arg(long, env = "BENCHMARK_OUTBOX_MAX_FILES", default_value_t = 2_880)]
    outbox_max_files: usize,

    /// Maximum persisted outbox bytes per dataset process.
    #[arg(
        long,
        env = "BENCHMARK_OUTBOX_MAX_BYTES",
        default_value_t = 268_435_456
    )]
    outbox_max_bytes: u64,

    /// Maximum Chrony offset and conservative clock-error bound permitted for ready latency.
    #[arg(
        long,
        env = "BENCHMARK_MAX_CLOCK_OFFSET_MS",
        default_value_t = clock::DEFAULT_MAX_CLOCK_OFFSET_MS
    )]
    max_clock_offset_ms: f64,

    /// SHA-256 of the exact deployed binary, or "unavailable" for an unproven local build.
    #[arg(long, env = "BENCHMARK_ARTIFACT_SHA256", default_value = "unavailable")]
    artifact_sha256: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let max_clock_offset_ms = clock::validate_max_clock_offset_ms(args.max_clock_offset_ms)?;
    let artifact_sha256 = normalize_artifact_sha256(&args.artifact_sha256)?;
    let coins = parse_coins(&args.coins)?;
    let (inferred_cloud, inferred_region, inferred_metro) = infer_location(&args.runner);
    let cloud = normalize_location("cloud", args.cloud.or(inferred_cloud), PUBLIC_CLOUDS)?;
    let region = normalize_location(
        "region",
        args.region.or(inferred_region),
        &["iad", "us-west", "fra", "nrt", "sin"],
    )?;
    let metro = normalize_location(
        "metro",
        args.metro.or(inferred_metro),
        &["iad", "sjc", "lax", "fra", "nrt", "sin"],
    )?;
    validate_public_identity(&args.runner, &cloud, &region, &metro)?;

    let hydromancer_token = required_secret("HYDROMANCER_API_KEY")?;
    let quicknode_token = required_secret("QUICKNODE_HYPERLIQUID_TOKEN")?;
    tonic::metadata::MetadataValue::try_from(quicknode_token.as_str())
        .context("QUICKNODE_HYPERLIQUID_TOKEN contains invalid header characters")?;
    let axiom_token = required_secret("AXIOM_API_TOKEN")?;
    let axiom_org_id = std::env::var("AXIOM_ORG_ID")
        .ok()
        .filter(|value| !value.is_empty());

    let axiom_client = AxiomClient::new(
        &args.axiom_url,
        &args.axiom_dataset,
        &axiom_token,
        axiom_org_id.as_deref(),
    )?;
    let (axiom, axiom_worker) = spawn_axiom_worker(
        axiom_client,
        axiom::OutboxConfig {
            directory: args.outbox_dir.join(args.dataset.label()),
            max_files: args.outbox_max_files,
            max_bytes: args.outbox_max_bytes,
        },
    )
    .await?;
    let ingest_health = axiom.health();

    let initial_clock = clock::sample(max_clock_offset_ms).await;
    let (clock_tx, clock_health) = tokio::sync::watch::channel(initial_clock);
    let clock_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(clock::CLOCK_SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            clock_tx.send_replace(clock::sample(max_clock_offset_ms).await);
        }
    });

    let wall_now = SystemTime::now();
    let now = Instant::now();
    let run_id = Uuid::new_v4().to_string();
    let mut config = BenchmarkConfig::production(
        args.dataset,
        coins.clone(),
        cloud.clone(),
        region.clone(),
        metro.clone(),
        args.runner.clone(),
        run_id.clone(),
    );
    config.rolling_window = ROLLING_WINDOW;
    config.publish_interval = PUBLISH_INTERVAL;
    config.cohort_timeout = COHORT_TIMEOUT;
    config.stale_after = STALE_AFTER;
    config.artifact_sha256 = artifact_sha256;
    let mut benchmark = Benchmark::new(config, now, wall_now);

    let signals = Arc::new(RuntimeSignals::new(&coins));
    let (tx, mut rx) = mpsc::channel::<ProbeEvent>(EVENT_QUEUE_CAPACITY);
    let sender = ProbeSender::new(tx, signals.clone());
    let stream_tasks = streams::spawn_streams(
        streams::StreamConfig {
            dataset: args.dataset,
            coins: coins.clone(),
            foundation_ws: args.foundation_ws,
            hydromancer_ws: args.hydromancer_ws,
            hydromancer_token,
            quicknode_grpc: args.quicknode_grpc,
            quicknode_token,
        },
        sender,
    );

    info!(
        schema = benchmark::SCHEMA,
        dataset = args.dataset.label(),
        coins = %coins.join(","),
        %cloud,
        %region,
        %metro,
        runner = %args.runner,
        %run_id,
        rolling_window_seconds = ROLLING_WINDOW.as_secs(),
        publish_interval_seconds = PUBLISH_INTERVAL.as_secs(),
        "Hyperliquid market benchmark started"
    );

    let mut publish = tokio::time::interval_at(next_publish_deadline(), PUBLISH_INTERVAL);
    publish.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    anyhow::bail!("all benchmark stream tasks stopped");
                };
                benchmark.record(event);
            }
            _ = publish.tick() => {
                publish_window(
                    &mut benchmark,
                    &signals,
                    &axiom,
                    &ingest_health,
                    clock_health.borrow().clone(),
                ).await?;
            }
            signal = &mut shutdown => {
                signal?;
                info!("shutdown requested; flushing completed Axiom windows");
                break;
            }
        }
    }

    for task in stream_tasks {
        task.abort();
    }
    clock_task.abort();
    axiom.close();
    match tokio::time::timeout(Duration::from_secs(15), axiom_worker).await {
        Ok(Ok(())) => info!("Axiom queue flushed"),
        Ok(Err(error)) => warn!(?error, "Axiom worker stopped during shutdown"),
        Err(_) => warn!("timed out flushing the bounded Axiom queue"),
    }
    Ok(())
}

async fn publish_window(
    benchmark: &mut Benchmark,
    signals: &RuntimeSignals,
    axiom: &axiom::AxiomSubmitter,
    health: &axiom::IngestHealth,
    clock: clock::ClockHealthSnapshot,
) -> Result<()> {
    let events = benchmark.window_events(
        Instant::now(),
        SystemTime::now(),
        signals,
        health.snapshot(),
        clock,
    );
    let event_count = events.len();
    let accepted = axiom
        .submit(events)
        .await
        .context("durably admit the Axiom latency window")?;
    let outcome_committed = if accepted {
        benchmark.commit_prepared_publication()
    } else {
        benchmark.reject_prepared_publication();
        false
    };
    if accepted && event_count > 0 && !outcome_committed {
        warn!("durable latency window had no matching prepared outcome interval");
    }
    info!(
        event_count,
        accepted, outcome_committed, "published exact rolling latency window"
    );
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal.context("install Ctrl-C handler"),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("install Ctrl-C handler")
}

fn next_publish_deadline() -> tokio::time::Instant {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let interval_ms = PUBLISH_INTERVAL.as_millis() as u64;
    let now_ms = now.as_millis() as u64;
    let wait_ms = interval_ms - now_ms % interval_ms;
    tokio::time::Instant::now() + Duration::from_millis(wait_ms)
}

fn parse_coins(raw: &str) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut coins = Vec::new();
    for coin in raw
        .split(',')
        .map(str::trim)
        .filter(|coin| !coin.is_empty())
    {
        if coin.len() > 32
            || !coin
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            anyhow::bail!("coin names must be 1-32 ASCII letters, numbers, '-' or '_'");
        }
        let coin = coin.to_ascii_uppercase();
        if seen.insert(coin.clone()) {
            coins.push(coin);
        }
    }
    if coins.is_empty() {
        anyhow::bail!("--coins must contain at least one coin");
    }
    if coins.len() > MAX_COINS_PER_PROCESS {
        anyhow::bail!("one benchmark process supports at most {MAX_COINS_PER_PROCESS} coins");
    }
    Ok(coins)
}

fn required_secret(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{name} is required"))
}

fn infer_location(runner: &str) -> (Option<String>, Option<String>, Option<String>) {
    let parts = runner
        .split(['-', '.'])
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let cloud = parts
        .iter()
        .find(|part| matches!(part.as_str(), "aws" | "gcp" | "oracle"))
        .cloned();
    let region = parts.iter().find_map(|part| match part.as_str() {
        "iad" | "fra" | "nrt" | "sin" => Some(part.clone()),
        "usw" => Some("us-west".to_owned()),
        _ => None,
    });
    let metro = parts
        .iter()
        .find(|part| matches!(part.as_str(), "iad" | "sjc" | "lax" | "fra" | "nrt" | "sin"))
        .cloned();
    (cloud, region, metro)
}

fn normalize_location(name: &str, value: Option<String>, allowed: &[&str]) -> Result<String> {
    let value = value
        .with_context(|| {
            format!(
                "BENCHMARK_{} is required when it cannot be inferred from the public runner ID",
                name.to_ascii_uppercase()
            )
        })?
        .to_ascii_lowercase();
    if !allowed.contains(&value.as_str()) {
        anyhow::bail!(
            "unsupported {name} '{value}'; expected one of {}",
            allowed.join(", ")
        );
    }
    Ok(value)
}

fn validate_public_identity(runner: &str, cloud: &str, region: &str, metro: &str) -> Result<()> {
    if !PUBLIC_CLOUDS.contains(&cloud) {
        anyhow::bail!("unsupported public cloud '{cloud}'");
    }
    let expected = if region == "us-west" {
        let fleet_metro = if cloud == "gcp" { "lax" } else { "sjc" };
        if metro != fleet_metro {
            anyhow::bail!(
                "public {cloud} us-west runners must use the current fleet metro '{fleet_metro}'"
            );
        }
        format!("{cloud}-usw-{metro}-01")
    } else {
        if metro != region {
            anyhow::bail!("public runner metro '{metro}' must match non-us-west region '{region}'");
        }
        format!("{cloud}-{region}-01")
    };
    if runner != expected {
        anyhow::bail!(
            "public runner ID must be '{expected}' for cloud={cloud}, region={region}, metro={metro}; private or inventory hostnames are forbidden"
        );
    }
    Ok(())
}

fn normalize_artifact_sha256(value: &str) -> Result<String> {
    if value == "unavailable" {
        return Ok(value.to_owned());
    }
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!(
            "BENCHMARK_ARTIFACT_SHA256 must be exactly 64 hexadecimal characters or 'unavailable'"
        );
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coins_are_normalized_deduplicated_and_bounded() {
        assert_eq!(parse_coins("btc, ETH,btc").unwrap(), vec!["BTC", "ETH"]);
        assert!(parse_coins(" , ").is_err());
        assert!(parse_coins("BTC/USD").is_err());
    }

    #[test]
    fn public_runner_id_supplies_location_without_private_hostname() {
        assert_eq!(
            infer_location("aws-nrt-01"),
            (
                Some("aws".to_owned()),
                Some("nrt".to_owned()),
                Some("nrt".to_owned())
            )
        );
        assert_eq!(
            infer_location("gcp-usw-lax-01"),
            (
                Some("gcp".to_owned()),
                Some("us-west".to_owned()),
                Some("lax".to_owned())
            )
        );
        assert_eq!(infer_location("unknown-runner"), (None, None, None));
    }

    #[test]
    fn runtime_rejects_private_or_inconsistent_public_identity() {
        assert!(validate_public_identity("aws-nrt-01", "aws", "nrt", "nrt").is_ok());
        assert!(validate_public_identity("gcp-usw-lax-01", "gcp", "us-west", "lax").is_ok());
        assert!(validate_public_identity("private-host", "aws", "nrt", "nrt").is_err());
        assert!(validate_public_identity("aws-nrt-01", "aws", "nrt", "fra").is_err());
        assert!(validate_public_identity("aws-usw-fra-01", "aws", "us-west", "fra").is_err());
        assert!(validate_public_identity("aws-usw-lax-01", "aws", "us-west", "lax").is_err());
        assert!(validate_public_identity("gcp-usw-sjc-01", "gcp", "us-west", "sjc").is_err());
        assert!(validate_public_identity("oracle-usw-lax-01", "oracle", "us-west", "lax").is_err());
    }

    #[test]
    fn current_location_vocabulary_has_no_oracle_abbreviation() {
        assert!(normalize_location("cloud", Some("oracle".to_owned()), PUBLIC_CLOUDS).is_ok());
        assert!(normalize_location("cloud", Some("ora".to_owned()), PUBLIC_CLOUDS).is_err());
    }

    #[test]
    fn artifact_provenance_is_explicit_and_validated() {
        assert_eq!(
            normalize_artifact_sha256("unavailable").unwrap(),
            "unavailable"
        );
        assert_eq!(
            normalize_artifact_sha256(&"A".repeat(64)).unwrap(),
            "a".repeat(64)
        );
        assert!(normalize_artifact_sha256("secret-or-not-a-digest").is_err());
        assert!(normalize_artifact_sha256(&"a".repeat(63)).is_err());
    }
}

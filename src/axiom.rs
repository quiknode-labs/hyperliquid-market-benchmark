use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::benchmark::LatencyWindowEvent;

const AXIOM_MAX_EVENTS_PER_BATCH: usize = 10_000;
const AXIOM_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const AXIOM_MAX_RESPONSE_BYTES: usize = 64 * 1024;
const OUTBOX_POLL_INTERVAL: Duration = Duration::from_secs(5);
const FAILED_CYCLE_BACKOFF: Duration = Duration::from_secs(30);
const RETRY_DELAYS: [Duration; 5] = [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];

#[derive(Debug, Clone)]
pub struct OutboxConfig {
    pub directory: PathBuf,
    pub max_files: usize,
    pub max_bytes: u64,
}

#[derive(Default)]
pub struct IngestHealth {
    pending_batches: AtomicU64,
    pending_bytes: AtomicU64,
    attempts: AtomicU64,
    batches_succeeded: AtomicU64,
    batches_failed: AtomicU64,
    batches_dropped: AtomicU64,
    events_succeeded: AtomicU64,
    events_dropped: AtomicU64,
    outbox_write_failures: AtomicU64,
    outbox_delete_failures: AtomicU64,
    outbox_cap_rejections: AtomicU64,
    last_success_wall_ms: AtomicU64,
}

impl IngestHealth {
    pub fn snapshot(&self) -> IngestHealthSnapshot {
        IngestHealthSnapshot {
            pending_batches: self.pending_batches.load(Ordering::Relaxed),
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            batches_succeeded: self.batches_succeeded.load(Ordering::Relaxed),
            batches_failed: self.batches_failed.load(Ordering::Relaxed),
            batches_dropped: self.batches_dropped.load(Ordering::Relaxed),
            events_succeeded: self.events_succeeded.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            outbox_write_failures: self.outbox_write_failures.load(Ordering::Relaxed),
            outbox_delete_failures: self.outbox_delete_failures.load(Ordering::Relaxed),
            outbox_cap_rejections: self.outbox_cap_rejections.load(Ordering::Relaxed),
            last_success_wall_ms: self.last_success_wall_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IngestHealthSnapshot {
    pub pending_batches: u64,
    pub pending_bytes: u64,
    pub attempts: u64,
    pub batches_succeeded: u64,
    pub batches_failed: u64,
    pub batches_dropped: u64,
    pub events_succeeded: u64,
    pub events_dropped: u64,
    pub outbox_write_failures: u64,
    pub outbox_delete_failures: u64,
    pub outbox_cap_rejections: u64,
    pub last_success_wall_ms: u64,
}

#[derive(Debug, Clone)]
struct OutboxFile {
    name: String,
    path: PathBuf,
    order: u64,
    bytes: u64,
    events: u64,
}

#[derive(Default)]
struct OutboxState {
    files: VecDeque<OutboxFile>,
    bytes: u64,
}

struct PersistentOutbox {
    config: OutboxConfig,
    _process_lock: std::fs::File,
    state: Mutex<OutboxState>,
    health: Arc<IngestHealth>,
    notify: Notify,
    stopping: AtomicBool,
    poisoned: AtomicBool,
    #[cfg(test)]
    fail_next_directory_sync: AtomicBool,
}

impl PersistentOutbox {
    async fn open(config: OutboxConfig, health: Arc<IngestHealth>) -> Result<Arc<Self>> {
        if config.max_files == 0 || config.max_bytes == 0 {
            anyhow::bail!("Axiom outbox caps must be greater than zero");
        }
        tokio::fs::create_dir_all(&config.directory)
            .await
            .with_context(|| format!("create Axiom outbox {}", config.directory.display()))?;
        let process_lock = acquire_process_lock(config.directory.clone()).await?;
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(&config.directory)
            .await
            .context("scan Axiom outbox")?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") {
                tokio::fs::remove_file(entry.path()).await.ok();
                continue;
            }
            if !name.ends_with(".ndjson") {
                continue;
            }
            let events = event_count_from_name(&name)
                .with_context(|| format!("invalid Axiom outbox filename {name}"))?;
            let order = order_from_name(&name)
                .with_context(|| format!("invalid Axiom outbox filename {name}"))?;
            let bytes = entry.metadata().await?.len();
            if bytes > AXIOM_MAX_BATCH_BYTES as u64 {
                anyhow::bail!("Axiom outbox file {name} exceeds the batch byte limit");
            }
            files.push(OutboxFile {
                name,
                path: entry.path(),
                order,
                bytes,
                events,
            });
        }
        files.sort_by(|left, right| {
            left.order
                .cmp(&right.order)
                .then_with(|| left.name.cmp(&right.name))
        });
        let bytes = files.iter().try_fold(0u64, |total, file| {
            total
                .checked_add(file.bytes)
                .context("existing Axiom outbox byte total overflowed")
        })?;
        if files.len() > config.max_files || bytes > config.max_bytes {
            anyhow::bail!(
                "existing Axiom outbox exceeds configured caps ({} files, {} bytes; caps are {} files, {} bytes)",
                files.len(),
                bytes,
                config.max_files,
                config.max_bytes
            );
        }
        health
            .pending_batches
            .store(files.len() as u64, Ordering::Relaxed);
        health.pending_bytes.store(bytes, Ordering::Relaxed);
        sync_directory(config.directory.clone()).await?;
        Ok(Arc::new(Self {
            config,
            _process_lock: process_lock,
            state: Mutex::new(OutboxState {
                files: files.into(),
                bytes,
            }),
            health,
            notify: Notify::new(),
            stopping: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_directory_sync: AtomicBool::new(false),
        }))
    }

    async fn persist_events(&self, events: &[LatencyWindowEvent]) -> Result<bool> {
        self.ensure_writable()?;
        if events.is_empty() {
            return Ok(true);
        }
        if events.len() > AXIOM_MAX_EVENTS_PER_BATCH {
            self.record_drop(events.len() as u64, true);
            warn!(
                event_count = events.len(),
                "Axiom batch exceeded the event cap"
            );
            return Ok(false);
        }
        let mut body = String::new();
        for event in events {
            let Ok(encoded) = serde_json::to_string(event) else {
                self.record_drop(events.len() as u64, false);
                warn!("failed to serialize Axiom latency window");
                return Ok(false);
            };
            if body.len().saturating_add(encoded.len()).saturating_add(1) > AXIOM_MAX_BATCH_BYTES {
                self.record_drop(events.len() as u64, true);
                warn!(
                    event_count = events.len(),
                    "Axiom batch exceeded the byte cap"
                );
                return Ok(false);
            }
            body.push_str(&encoded);
            body.push('\n');
        }
        self.persist_body(body, events.len() as u64).await
    }

    async fn persist_body(&self, body: String, events: u64) -> Result<bool> {
        self.ensure_writable()?;
        let bytes = body.len() as u64;
        let mut state = self.state.lock().await;
        self.ensure_writable()?;
        let next_bytes = state.bytes.checked_add(bytes);
        if state.files.len() >= self.config.max_files
            || next_bytes.is_none_or(|total| total > self.config.max_bytes)
        {
            self.record_drop(events, true);
            warn!(
                pending_files = state.files.len(),
                pending_bytes = state.bytes,
                "Axiom outbox cap reached; dropping a latency window"
            );
            return Ok(false);
        }

        let order = match state.files.back() {
            Some(last) => now_ms().max(
                last.order
                    .checked_add(1)
                    .context("Axiom outbox ordering key exhausted")?,
            ),
            None => now_ms(),
        };
        let id = Uuid::new_v4();
        let name = format!("{order:020}-{id}-{events}.ndjson");
        let final_path = self.config.directory.join(&name);
        let temp_path = self.config.directory.join(format!(".{id}.tmp"));
        let temp_write_result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)
                .await?;
            file.write_all(body.as_bytes()).await?;
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            Result::<(), std::io::Error>::Ok(())
        }
        .await;
        if let Err(error) = temp_write_result {
            tokio::fs::remove_file(&temp_path).await.ok();
            self.health
                .outbox_write_failures
                .fetch_add(1, Ordering::Relaxed);
            self.record_drop(events, false);
            warn!(?error, "failed to persist Axiom latency window");
            return Ok(false);
        }
        if let Err(error) = tokio::fs::rename(&temp_path, &final_path).await {
            tokio::fs::remove_file(&temp_path).await.ok();
            self.health
                .outbox_write_failures
                .fetch_add(1, Ordering::Relaxed);
            self.record_drop(events, false);
            warn!(?error, "failed to finalize Axiom latency window");
            return Ok(false);
        }
        if let Err(error) = self.sync_outbox_directory().await {
            self.poisoned.store(true, Ordering::Release);
            self.health
                .outbox_write_failures
                .fetch_add(1, Ordering::Relaxed);
            // The rename may or may not survive a crash. Continuing would let the
            // benchmark retry these outcomes under a different window identity,
            // while a later restart could also replay this finalized file.
            return Err(error).context(format!(
                "Axiom outbox file {} has ambiguous durability after rename; refusing an in-run retry",
                final_path.display()
            ));
        }

        let file = OutboxFile {
            name,
            path: final_path,
            order,
            bytes,
            events,
        };
        state.files.push_back(file);
        state.bytes = next_bytes.expect("validated outbox byte total");
        self.health.pending_batches.fetch_add(1, Ordering::Relaxed);
        self.health
            .pending_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        drop(state);
        self.notify.notify_one();
        Ok(true)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            anyhow::bail!(
                "Axiom outbox is poisoned by an earlier durability ambiguity; restart is required"
            );
        }
        Ok(())
    }

    async fn next(&self) -> Option<OutboxFile> {
        self.state.lock().await.files.front().cloned()
    }

    async fn read(&self, file: &OutboxFile) -> Result<String> {
        if file.bytes > AXIOM_MAX_BATCH_BYTES as u64 {
            anyhow::bail!("Axiom outbox file exceeds the batch byte limit");
        }
        let bytes = tokio::fs::read(&file.path).await?;
        if bytes.len() > AXIOM_MAX_BATCH_BYTES {
            anyhow::bail!("Axiom outbox file grew past the batch byte limit");
        }
        String::from_utf8(bytes).context("Axiom outbox file is not UTF-8 NDJSON")
    }

    async fn acknowledge(&self, file: &OutboxFile) -> Result<()> {
        match tokio::fs::remove_file(&file.path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A prior acknowledgement attempt can unlink successfully and
                // then fail its directory fsync. Retrying only the local commit
                // must not re-POST an already fully acknowledged batch.
            }
            Err(error) => return Err(error.into()),
        }
        self.sync_outbox_directory().await?;
        let mut state = self.state.lock().await;
        let Some(position) = state
            .files
            .iter()
            .position(|pending| pending.name == file.name)
        else {
            anyhow::bail!("acknowledged Axiom outbox file was not pending");
        };
        let removed = state.files.remove(position).expect("position exists");
        state.bytes = state.bytes.saturating_sub(removed.bytes);
        self.health.pending_batches.fetch_sub(1, Ordering::Relaxed);
        self.health
            .pending_bytes
            .fetch_sub(removed.bytes, Ordering::Relaxed);
        Ok(())
    }

    async fn sync_outbox_directory(&self) -> Result<()> {
        #[cfg(test)]
        if self.fail_next_directory_sync.swap(false, Ordering::Relaxed) {
            anyhow::bail!("injected Axiom outbox directory fsync failure");
        }
        sync_directory(self.config.directory.clone()).await
    }

    fn record_drop(&self, events: u64, cap_rejection: bool) {
        self.health.batches_dropped.fetch_add(1, Ordering::Relaxed);
        self.health
            .events_dropped
            .fetch_add(events, Ordering::Relaxed);
        if cap_rejection {
            self.health
                .outbox_cap_rejections
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct AxiomSubmitter {
    outbox: Arc<PersistentOutbox>,
}

impl AxiomSubmitter {
    pub async fn submit(&self, events: Vec<LatencyWindowEvent>) -> Result<bool> {
        self.outbox.persist_events(&events).await
    }

    pub fn health(&self) -> Arc<IngestHealth> {
        self.outbox.health.clone()
    }

    pub fn close(self) {
        self.outbox.stopping.store(true, Ordering::Relaxed);
        self.outbox.notify.notify_waiters();
    }
}

pub struct AxiomClient {
    client: reqwest::Client,
    ingest_url: Url,
    headers: HeaderMap,
}

impl AxiomClient {
    pub fn new(base_url: &str, dataset: &str, token: &str, org_id: Option<&str>) -> Result<Self> {
        if token.is_empty() {
            anyhow::bail!("AXIOM_API_TOKEN must not be empty");
        }
        if token.starts_with("xapt-") {
            anyhow::bail!(
                "AXIOM_API_TOKEN must be a dataset ingest API token; personal query tokens cannot ingest"
            );
        }
        validate_dataset(dataset)?;
        let ingest_url = ingest_url(base_url, dataset)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("AXIOM_API_TOKEN contains invalid header characters")?,
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        if let Some(org_id) = org_id.filter(|value| !value.is_empty()) {
            headers.insert(
                "x-axiom-org-id",
                HeaderValue::from_str(org_id)
                    .context("AXIOM_ORG_ID contains invalid header characters")?,
            );
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            ingest_url,
            headers,
        })
    }

    async fn post_ndjson(&self, body: String, expected_events: usize) -> Result<()> {
        let mut response = self
            .client
            .post(self.ingest_url.clone())
            .headers(self.headers.clone())
            .body(body)
            .send()
            .await
            .context("send Axiom ingest request")?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > AXIOM_MAX_RESPONSE_BYTES as u64)
        {
            anyhow::bail!("Axiom ingest response exceeded the byte limit");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("read Axiom ingest response")?
        {
            if bytes.len().saturating_add(chunk.len()) > AXIOM_MAX_RESPONSE_BYTES {
                anyhow::bail!("Axiom ingest response exceeded the byte limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            anyhow::bail!("Axiom ingest returned HTTP {status}");
        }
        let result: IngestResponse =
            serde_json::from_slice(&bytes).context("decode Axiom ingest response")?;
        if result.failed != 0 || result.ingested != expected_events as u64 {
            anyhow::bail!(
                "Axiom accepted {} of {} events and rejected {}",
                result.ingested,
                expected_events,
                result.failed
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct IngestResponse {
    ingested: u64,
    failed: u64,
}

pub async fn spawn_axiom_worker(
    client: AxiomClient,
    config: OutboxConfig,
) -> Result<(AxiomSubmitter, JoinHandle<()>)> {
    let health = Arc::new(IngestHealth::default());
    let outbox = PersistentOutbox::open(config, health.clone()).await?;
    let submitter = AxiomSubmitter {
        outbox: outbox.clone(),
    };
    let worker = tokio::spawn(async move {
        loop {
            let Some(file) = outbox.next().await else {
                if outbox.stopping.load(Ordering::Relaxed) {
                    return;
                }
                tokio::select! {
                    _ = outbox.notify.notified() => {}
                    _ = tokio::time::sleep(OUTBOX_POLL_INTERVAL) => {}
                }
                continue;
            };
            let body = match outbox.read(&file).await {
                Ok(body) => body,
                Err(error) => {
                    outbox.health.batches_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(file = file.name, ?error, "cannot read Axiom outbox head");
                    if outbox.stopping.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(FAILED_CYCLE_BACKOFF) => {}
                        _ = outbox.notify.notified() => {}
                    }
                    continue;
                }
            };
            let mut delivered = false;
            for (attempt, delay) in RETRY_DELAYS.into_iter().enumerate() {
                if outbox.stopping.load(Ordering::Relaxed) && attempt > 0 {
                    break;
                }
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                if outbox.stopping.load(Ordering::Relaxed) && attempt > 0 {
                    break;
                }
                outbox.health.attempts.fetch_add(1, Ordering::Relaxed);
                match client.post_ndjson(body.clone(), file.events as usize).await {
                    Ok(()) => {
                        loop {
                            match outbox.acknowledge(&file).await {
                                Ok(()) => break,
                                Err(error) => {
                                    outbox
                                        .health
                                        .outbox_delete_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!(
                                        file = file.name,
                                        ?error,
                                        "Axiom acked; retrying only the local outbox commit"
                                    );
                                    if outbox.stopping.load(Ordering::Relaxed) {
                                        return;
                                    }
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                        outbox
                            .health
                            .batches_succeeded
                            .fetch_add(1, Ordering::Relaxed);
                        outbox
                            .health
                            .events_succeeded
                            .fetch_add(file.events, Ordering::Relaxed);
                        outbox
                            .health
                            .last_success_wall_ms
                            .store(now_ms(), Ordering::Relaxed);
                        delivered = true;
                        info!(
                            event_count = file.events,
                            attempts = attempt + 1,
                            "acknowledged persisted Axiom latency window"
                        );
                    }
                    Err(error) => {
                        warn!(attempt = attempt + 1, ?error, "Axiom ingest attempt failed");
                    }
                }
                if delivered {
                    break;
                }
            }
            if delivered {
                continue;
            }
            outbox.health.batches_failed.fetch_add(1, Ordering::Relaxed);
            if outbox.stopping.load(Ordering::Relaxed) {
                return;
            }
            warn!(
                file = file.name,
                "Axiom batch retained after bounded retry cycle"
            );
            tokio::select! {
                _ = tokio::time::sleep(FAILED_CYCLE_BACKOFF) => {}
                _ = outbox.notify.notified() => {}
            }
        }
    });
    Ok((submitter, worker))
}

fn ingest_url(base_url: &str, dataset: &str) -> Result<Url> {
    let mut url = Url::parse(base_url).context("invalid Axiom base URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        anyhow::bail!("Axiom base URL must be HTTP or HTTPS");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Axiom base URL must not contain credentials");
    }
    if url.scheme() == "http" && !url.host_str().is_some_and(is_loopback_host) {
        anyhow::bail!("Axiom base URL must use HTTPS except on loopback");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("Axiom base URL must not contain a query or fragment");
    }
    if !matches!(url.path(), "" | "/") {
        anyhow::bail!("Axiom base URL must be an origin without a path");
    }
    url.set_path("");
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("Axiom base URL cannot contain path segments"))?
        .extend(["v1", "datasets", dataset, "ingest"]);
    Ok(url)
}

fn validate_dataset(dataset: &str) -> Result<()> {
    if dataset.is_empty()
        || !dataset
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        anyhow::bail!("Axiom dataset must contain only letters, numbers, '-' or '_'");
    }
    Ok(())
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn event_count_from_name(name: &str) -> Result<u64> {
    name.strip_suffix(".ndjson")
        .and_then(|stem| stem.rsplit_once('-'))
        .and_then(|(_, count)| count.parse::<u64>().ok())
        .filter(|count| *count > 0 && *count <= AXIOM_MAX_EVENTS_PER_BATCH as u64)
        .context("outbox filename has no valid event count")
}

fn order_from_name(name: &str) -> Result<u64> {
    let order = name
        .split_once('-')
        .map(|(order, _)| order)
        .filter(|order| order.len() == 20)
        .and_then(|order| order.parse::<u64>().ok())
        .context("outbox filename has no valid ordering key")?;
    Ok(order)
}

async fn sync_directory(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .context("join directory fsync task")?
        .context("fsync Axiom outbox directory")
}

async fn acquire_process_lock(directory: PathBuf) -> Result<std::fs::File> {
    tokio::task::spawn_blocking(move || {
        let path = directory.join(".producer.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open Axiom outbox lock {}", path.display()))?;
        fs2::FileExt::try_lock_exclusive(&file).with_context(|| {
            format!(
                "Axiom outbox {} is already owned by another producer process",
                directory.display()
            )
        })?;
        Ok::<_, anyhow::Error>(file)
    })
    .await
    .context("join Axiom outbox lock task")?
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as TokioMutex;

    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "hyperliquid-market-benchmark-{name}-{}",
            Uuid::new_v4()
        ))
    }

    async fn mock_axiom() -> (String, Arc<TokioMutex<String>>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured = Arc::new(TokioMutex::new(String::new()));
        let server_capture = captured.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request);
                let Some(header_end) = text.find("\r\n\r\n") else {
                    continue;
                };
                let content_length = text[..header_end]
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            *server_capture.lock().await = String::from_utf8_lossy(&request).into_owned();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 25\r\nConnection: close\r\n\r\n{\"ingested\":1,\"failed\":0}",
                )
                .await
                .unwrap();
        });
        (format!("http://{address}"), captured, server)
    }

    #[test]
    fn builds_only_the_current_tls_ingest_endpoint() {
        assert_eq!(
            ingest_url("https://api.axiom.co", "hyperliquid-market-benchmark")
                .unwrap()
                .as_str(),
            "https://api.axiom.co/v1/datasets/hyperliquid-market-benchmark/ingest"
        );
        assert!(ingest_url("http://api.axiom.co", "hyperliquid-market-benchmark").is_err());
        assert!(
            ingest_url(
                "https://user:secret@api.axiom.co",
                "hyperliquid-market-benchmark"
            )
            .is_err()
        );
        assert!(ingest_url("ftp://api.axiom.co", "hyperliquid-market-benchmark").is_err());
        assert!(
            ingest_url(
                "https://api.axiom.co/unexpected",
                "hyperliquid-market-benchmark"
            )
            .is_err()
        );
        assert!(validate_dataset("bad/dataset").is_err());
    }

    #[test]
    fn rejects_a_query_pat_before_any_network_request() {
        let token = ["xapt", "fixture"].join("-");
        let error = AxiomClient::new(
            "https://api.axiom.co",
            "hyperliquid-market-benchmark",
            &token,
            None,
        )
        .err()
        .expect("PAT must be rejected");
        assert!(error.to_string().contains("cannot ingest"));
        assert!(!error.to_string().contains(&token));
    }

    #[tokio::test]
    async fn verifies_axiom_ack_and_keeps_credentials_out_of_the_body() {
        let (url, captured, server) = mock_axiom().await;
        let client =
            AxiomClient::new(&url, "hyperliquid-market-benchmark", "t", Some("test-org")).unwrap();
        client
            .post_ndjson("{\"safe\":true}\n".to_owned(), 1)
            .await
            .unwrap();
        server.await.unwrap();
        let request = captured.lock().await.clone();
        let (headers, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(headers.contains("authorization: Bearer t"));
        assert!(headers.contains("x-axiom-org-id: test-org"));
        assert_eq!(body, "{\"safe\":true}\n");
        assert!(!body.contains("secret"));
    }

    #[tokio::test]
    async fn rejects_an_oversized_axiom_ack_before_buffering_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 4096];
            let _ = socket.read(&mut buffer).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 70000\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let client = AxiomClient::new(
            &format!("http://{address}"),
            "hyperliquid-market-benchmark",
            "t",
            None,
        )
        .unwrap();
        let error = client
            .post_ndjson("{\"safe\":true}\n".to_owned(), 1)
            .await
            .expect_err("oversized response must fail");
        assert!(error.to_string().contains("byte limit"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn outbox_is_atomic_bounded_and_replays_after_restart() {
        let directory = test_directory("outbox");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 1,
            max_bytes: 1024,
        };
        let health = Arc::new(IngestHealth::default());
        let outbox = PersistentOutbox::open(config.clone(), health.clone())
            .await
            .unwrap();
        assert!(
            outbox
                .persist_body("{\"event_id\":\"one\"}\n".to_owned(), 1)
                .await
                .unwrap()
        );
        assert!(
            !outbox
                .persist_body("{\"event_id\":\"two\"}\n".to_owned(), 1)
                .await
                .unwrap()
        );
        let entries = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".ndjson"))
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].ends_with(".ndjson"));
        assert!(!entries[0].ends_with(".tmp"));
        assert_eq!(health.snapshot().outbox_cap_rejections, 1);
        drop(outbox);

        let restarted = PersistentOutbox::open(config, Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        let pending = restarted.next().await.unwrap();
        assert_eq!(
            restarted.read(&pending).await.unwrap(),
            "{\"event_id\":\"one\"}\n"
        );
        restarted.acknowledge(&pending).await.unwrap();
        assert!(restarted.next().await.is_none());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn recovery_refuses_existing_data_beyond_the_configured_hard_cap() {
        let directory = test_directory("recovery-cap");
        let roomy = OutboxConfig {
            directory: directory.clone(),
            max_files: 2,
            max_bytes: 4096,
        };
        let outbox = PersistentOutbox::open(roomy, Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        for id in ["one", "two"] {
            assert!(
                outbox
                    .persist_body(format!("{{\"event_id\":\"{id}\"}}\n"), 1)
                    .await
                    .unwrap()
            );
        }
        drop(outbox);

        let constrained = OutboxConfig {
            directory: directory.clone(),
            max_files: 1,
            max_bytes: 4096,
        };
        let error = PersistentOutbox::open(constrained, Arc::new(IngestHealth::default()))
            .await
            .err()
            .expect("recovery must not silently exceed its hard cap");
        assert!(error.to_string().contains("exceeds configured caps"));
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn one_dataset_outbox_has_exactly_one_process_owner() {
        let directory = test_directory("exclusive-owner");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 1,
            max_bytes: 4096,
        };
        let first = PersistentOutbox::open(config.clone(), Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        let error = PersistentOutbox::open(config.clone(), Arc::new(IngestHealth::default()))
            .await
            .err()
            .expect("a second producer must not share an outbox");
        assert!(error.to_string().contains("already owned"));
        drop(first);

        let reopened = PersistentOutbox::open(config, Arc::new(IngestHealth::default()))
            .await
            .expect("dropping the owner must release the OS lock");
        drop(reopened);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn admission_order_is_strict_even_with_sub_millisecond_persistence() {
        let directory = test_directory("fifo-order");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 8,
            max_bytes: 4096,
        };
        let outbox = PersistentOutbox::open(config, Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        for id in ["one", "two"] {
            assert!(
                outbox
                    .persist_body(format!("{{\"event_id\":\"{id}\"}}\n"), 1)
                    .await
                    .unwrap()
            );
        }
        let state = outbox.state.lock().await;
        assert!(state.files[0].order < state.files[1].order);
        let first_path = state.files[0].path.clone();
        let second_path = state.files[1].path.clone();
        drop(state);
        assert_eq!(
            tokio::fs::read_to_string(first_path).await.unwrap(),
            "{\"event_id\":\"one\"}\n"
        );
        assert_eq!(
            tokio::fs::read_to_string(second_path).await.unwrap(),
            "{\"event_id\":\"two\"}\n"
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn post_rename_fsync_failure_is_fatal_and_replayable_not_retryable_in_run() {
        let directory = test_directory("ambiguous-fsync");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 8,
            max_bytes: 4096,
        };
        let outbox = PersistentOutbox::open(config.clone(), Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        outbox
            .fail_next_directory_sync
            .store(true, Ordering::Relaxed);
        let error = outbox
            .persist_body("{\"event_id\":\"ambiguous\"}\n".to_owned(), 1)
            .await
            .expect_err("post-rename fsync failure must stop the live publication path");
        assert!(error.to_string().contains("ambiguous durability"));
        assert!(outbox.next().await.is_none());
        let files_after_failure = std::fs::read_dir(&directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".ndjson"))
            .count();
        assert_eq!(files_after_failure, 1);
        let retry_error = outbox
            .persist_body("{\"event_id\":\"must-not-exist\"}\n".to_owned(), 1)
            .await
            .expect_err("a poisoned live outbox must reject every later admission");
        assert!(retry_error.to_string().contains("poisoned"));
        let files_after_retry = std::fs::read_dir(&directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".ndjson"))
            .count();
        assert_eq!(files_after_retry, 1);
        drop(outbox);

        let restarted = PersistentOutbox::open(config, Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        assert_eq!(
            restarted
                .read(&restarted.next().await.unwrap())
                .await
                .unwrap(),
            "{\"event_id\":\"ambiguous\"}\n"
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn local_ack_retry_tolerates_an_already_unlinked_file() {
        let directory = test_directory("ack-fsync-retry");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 8,
            max_bytes: 4096,
        };
        let outbox = PersistentOutbox::open(config, Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        assert!(
            outbox
                .persist_body("{\"event_id\":\"acked\"}\n".to_owned(), 1)
                .await
                .unwrap()
        );
        let file = outbox.next().await.unwrap();
        tokio::fs::remove_file(&file.path).await.unwrap();
        outbox.acknowledge(&file).await.unwrap();
        assert!(outbox.next().await.is_none());
        assert_eq!(outbox.health.snapshot().pending_batches, 0);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn worker_deletes_only_after_a_full_axiom_ack() {
        let directory = test_directory("worker");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 8,
            max_bytes: 4096,
        };
        let seed = PersistentOutbox::open(config.clone(), Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        assert!(
            seed.persist_body("{\"event_id\":\"stable\"}\n".to_owned(), 1)
                .await
                .unwrap()
        );
        drop(seed);

        let (url, _, server) = mock_axiom().await;
        let client = AxiomClient::new(&url, "hyperliquid-market-benchmark", "t", None).unwrap();
        let (submitter, worker) = spawn_axiom_worker(client, config).await.unwrap();
        server.await.unwrap();
        for _ in 0..100 {
            if submitter.health().snapshot().pending_batches == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(submitter.health().snapshot().pending_batches, 0);
        submitter.close();
        worker.await.unwrap();
        assert!(
            std::fs::read_dir(&directory)
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".ndjson"))
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn failed_ingest_remains_on_disk_for_the_next_process() {
        let directory = test_directory("retained");
        let config = OutboxConfig {
            directory: directory.clone(),
            max_files: 8,
            max_bytes: 4096,
        };
        let seed = PersistentOutbox::open(config.clone(), Arc::new(IngestHealth::default()))
            .await
            .unwrap();
        assert!(
            seed.persist_body("{\"event_id\":\"stable\"}\n".to_owned(), 1)
                .await
                .unwrap()
        );
        drop(seed);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 4096];
            let _ = socket.read(&mut buffer).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let client = AxiomClient::new(
            &format!("http://{address}"),
            "hyperliquid-market-benchmark",
            "t",
            None,
        )
        .unwrap();
        let (submitter, worker) = spawn_axiom_worker(client, config).await.unwrap();
        submitter.close();
        server.await.unwrap();
        worker.await.unwrap();
        assert_eq!(
            std::fs::read_dir(&directory)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(".ndjson"))
                .count(),
            1
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

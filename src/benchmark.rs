use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;

use crate::axiom::IngestHealthSnapshot;
use crate::clock::ClockHealthSnapshot;
use crate::model::{
    BaseKey, ContentKey, Dataset, EventKey, MarketEvent, ProbeEvent, Provider, RuntimeSignals,
};

#[cfg(test)]
use crate::model::PROVIDERS;
#[cfg(test)]
pub const SCHEMA: &str = "hyperliquid-market-benchmark-v1";
#[cfg(test)]
pub const FILLS_SCHEMA: &str = "hyperliquid-market-benchmark-v2";
pub const EVENT_TYPE: &str = "latency_window";
#[cfg(test)]
pub const METRIC_KIND: &str = "event_to_canonical_book_ready";
#[cfg(test)]
pub const FILLS_METRIC_KIND: &str = "event_to_canonical_trade_ready";
#[cfg(test)]
pub const MEASUREMENT_VERSION: &str = "canonical-book-ready-v1";
#[cfg(test)]
pub const FILLS_MEASUREMENT_VERSION: &str = "canonical-trade-ready-v1";
pub const SOURCE_COMMIT: &str = env!("BENCHMARK_SOURCE_COMMIT");
pub const MIN_READY_SAMPLES: usize = 1_000;
const MAX_FUTURE_SKEW_MS: u64 = 5_000;
const MAX_SIGNATURES_PER_BASE: usize = 4;
const DEFAULT_MAX_PENDING: usize = 5_000;
const DEFAULT_MAX_SETTLED: usize = 20_000;
const DEFAULT_MAX_ROLLING_COHORTS: usize = 25_000;

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub dataset: Dataset,
    pub coins: Vec<String>,
    pub cloud: String,
    pub region: String,
    pub metro: String,
    pub runner: String,
    pub run_id: String,
    pub artifact_sha256: String,
    pub rolling_window: Duration,
    pub publish_interval: Duration,
    pub cohort_timeout: Duration,
    pub stale_after: Duration,
    pub max_pending: usize,
    pub max_settled: usize,
    pub max_rolling_cohorts: usize,
}

impl BenchmarkConfig {
    pub fn production(
        dataset: Dataset,
        coins: Vec<String>,
        cloud: String,
        region: String,
        metro: String,
        runner: String,
        run_id: String,
    ) -> Self {
        Self {
            dataset,
            coins,
            cloud,
            region,
            metro,
            runner,
            run_id,
            artifact_sha256: "unavailable".to_owned(),
            rolling_window: Duration::from_secs(300),
            publish_interval: Duration::from_secs(30),
            cohort_timeout: Duration::from_secs(5),
            stale_after: Duration::from_secs(60),
            max_pending: DEFAULT_MAX_PENDING,
            max_settled: DEFAULT_MAX_SETTLED,
            max_rolling_cohorts: DEFAULT_MAX_ROLLING_COHORTS,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ProviderCounters {
    observed: u64,
    matched: u64,
    missing: u64,
    mismatch: u64,
    duplicate: u64,
    late: u64,
    orphaned: u64,
    negative_latency: u64,
    future_timestamp: u64,
    signature_overflow: u64,
    reconnects: u64,
    sequence_gaps: u64,
    replay_count: u64,
    replay_gaps: u64,
}

#[derive(Debug, Clone)]
struct Observation {
    received: Instant,
    received_wall_ms: u64,
}

struct PendingCandidate {
    first_observed: Instant,
    arrivals: [Option<Observation>; 3],
}

impl PendingCandidate {
    fn new(first_observed: Instant) -> Self {
        Self {
            first_observed,
            arrivals: std::array::from_fn(|_| None),
        }
    }
}

struct PendingBase {
    first_observed: Instant,
    candidates: HashMap<ContentKey, PendingCandidate>,
}

impl PendingBase {
    fn new(first_observed: Instant) -> Self {
        Self {
            first_observed,
            candidates: HashMap::new(),
        }
    }
}

struct SettledCohort {
    settled_at: Instant,
    matched_mask: u8,
}

#[derive(Debug, Clone)]
struct CommittedCohort {
    committed_at: Instant,
    event_ms: u64,
    latency_ms: [u64; 3],
    commit_delay_ms: u64,
    outcome_reported: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoverageOutcome {
    Matched,
    Missing,
    Mismatch,
}

#[derive(Debug, Clone)]
struct CoverageCohort {
    settled_at: Instant,
    outcomes: [CoverageOutcome; 3],
}

#[derive(Default)]
struct CoinWindow {
    cohorts: VecDeque<CommittedCohort>,
    coverage: VecDeque<CoverageCohort>,
    complete_cohorts: u64,
    state_evictions: u64,
    rolling_evictions: u64,
    coverage_evictions: u64,
    last_integrity_loss: Option<Instant>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyWindowEvent {
    #[serde(rename = "_time")]
    pub time: String,
    pub schema: &'static str,
    pub event_type: &'static str,
    pub metric_kind: &'static str,
    pub benchmark_version: &'static str,
    pub measurement_version: &'static str,
    pub source_commit: &'static str,
    pub artifact_sha256: String,
    pub event_id: String,
    pub window_id: String,
    pub window_end: String,
    pub window_seconds: u64,
    pub publish_interval_seconds: u64,
    pub provider: &'static str,
    pub protocol: &'static str,
    pub source: &'static str,
    pub dataset: &'static str,
    pub coin: String,
    pub cloud: String,
    pub region: String,
    pub metro: String,
    pub runner: String,
    pub location: String,
    pub run_id: String,
    pub runner_uptime_seconds: u64,
    pub cohort: &'static str,
    pub cohort_complete: bool,
    pub sample_count: u64,
    pub min_ready_samples: u64,
    pub ready: bool,
    pub readiness: &'static str,
    pub coverage_count_scope: &'static str,
    pub health_count_scope: &'static str,
    pub outcome_count_scope: &'static str,
    pub outcome_interval_id: String,
    pub outcome_interval_start: String,
    pub outcome_interval_end: String,
    pub outcome_interval_duration_ms: u64,
    pub outcome_interval_complete: bool,
    pub outcome_complete_cohort_count: u64,
    pub outcome_foundation_strict_fastest_count: u64,
    pub outcome_hydromancer_strict_fastest_count: u64,
    pub outcome_quicknode_strict_fastest_count: u64,
    pub outcome_foundation_tied_fastest_count: u64,
    pub outcome_hydromancer_tied_fastest_count: u64,
    pub outcome_quicknode_tied_fastest_count: u64,
    pub outcome_tie_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standard_deviation_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_p50_spread_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cohort_commit_delay_p99_ms: Option<f64>,
    pub observed_count: u64,
    pub matched_count: u64,
    pub missing_count: u64,
    pub mismatch_count: u64,
    pub matched_total: u64,
    pub missing_total: u64,
    pub mismatch_total: u64,
    pub duplicate_count: u64,
    pub late_count: u64,
    pub orphaned_count: u64,
    pub negative_latency_count: u64,
    pub future_timestamp_count: u64,
    pub signature_overflow_count: u64,
    pub complete_cohort_count: u64,
    pub pending_cohort_count: u64,
    pub state_eviction_count: u64,
    pub rolling_eviction_count: u64,
    pub coverage_eviction_count: u64,
    pub reconnects: u64,
    pub sequence_gaps: u64,
    pub replay_count: u64,
    pub replay_gap_count: u64,
    pub queue_drops: u64,
    pub clock_healthy: bool,
    pub clock_status: &'static str,
    pub clock_source: &'static str,
    pub clock_synchronized: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_offset_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_error_bound_ms: Option<f64>,
    pub clock_max_offset_ms: f64,
    pub clock_checked_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_check_age_ms: Option<u64>,
    pub connection_state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_age_ms: Option<u64>,
    pub ingest_pending_batches: u64,
    pub ingest_pending_bytes: u64,
    pub ingest_attempts: u64,
    pub ingest_batches_succeeded: u64,
    pub ingest_batches_failed: u64,
    pub ingest_batches_dropped: u64,
    pub ingest_events_succeeded: u64,
    pub ingest_events_dropped: u64,
    pub ingest_outbox_write_failures: u64,
    pub ingest_outbox_delete_failures: u64,
    pub ingest_outbox_cap_rejections: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingest_last_success_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingest_last_success_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Distribution {
    p50: f64,
    p95: f64,
    p99: f64,
    min: f64,
    max: f64,
    mean: f64,
    standard_deviation: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct OutcomeCounts {
    complete: u64,
    strict_fastest: [u64; 3],
    tied_fastest: [u64; 3],
    ties: u64,
}

#[derive(Debug, Clone, Copy)]
struct PreparedOutcomeInterval {
    end: Instant,
    end_wall_ms: u64,
}

pub struct Benchmark {
    config: BenchmarkConfig,
    started_at: Instant,
    outcome_cursor: Instant,
    outcome_cursor_wall_ms: u64,
    outcome_has_published: bool,
    prepared_outcome_interval: Option<PreparedOutcomeInterval>,
    pending: HashMap<BaseKey, PendingBase>,
    settled_bases: HashMap<BaseKey, Instant>,
    settled: HashMap<EventKey, SettledCohort>,
    counters: HashMap<String, [ProviderCounters; 3]>,
    windows: HashMap<String, CoinWindow>,
}

impl Benchmark {
    pub fn new(config: BenchmarkConfig, now: Instant, wall_now: SystemTime) -> Self {
        let mut counters = HashMap::new();
        let mut windows = HashMap::new();
        for coin in &config.coins {
            windows.insert(coin.clone(), CoinWindow::default());
            counters.insert(
                coin.clone(),
                std::array::from_fn(|_| ProviderCounters::default()),
            );
        }
        Self {
            config,
            started_at: now,
            outcome_cursor: now,
            outcome_cursor_wall_ms: system_time_ms(wall_now),
            outcome_has_published: false,
            prepared_outcome_interval: None,
            pending: HashMap::new(),
            settled_bases: HashMap::new(),
            settled: HashMap::new(),
            counters,
            windows,
        }
    }

    pub fn record(&mut self, event: ProbeEvent) {
        match event {
            ProbeEvent::Market(event) => self.record_market(event),
            ProbeEvent::Reconnect { provider, coin } => {
                self.counters_mut(provider, &coin).reconnects += 1;
            }
            ProbeEvent::SequenceGap {
                provider,
                coin,
                missing,
            } => {
                self.counters_mut(provider, &coin).sequence_gaps += missing;
            }
            ProbeEvent::Replay {
                provider,
                coin,
                messages,
                has_gap,
            } => {
                let counters = self.counters_mut(provider, &coin);
                counters.replay_count += messages;
                counters.replay_gaps += u64::from(has_gap);
            }
        }
    }

    pub fn tick(&mut self, now: Instant) {
        let expired = self
            .pending
            .iter()
            .filter(|(_, cohort)| {
                now.saturating_duration_since(cohort.first_observed) >= self.config.cohort_timeout
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            self.settle(key, now, false);
        }
        self.prune(now);
    }

    pub fn window_events(
        &mut self,
        now: Instant,
        wall_now: SystemTime,
        signals: &RuntimeSignals,
        ingest: IngestHealthSnapshot,
        clock: ClockHealthSnapshot,
    ) -> Vec<LatencyWindowEvent> {
        self.prepared_outcome_interval = None;
        self.tick(now);
        let actual_wall_ms = system_time_ms(wall_now);
        let wall_ms = aligned_window_end_ms(wall_now, self.config.publish_interval);
        if wall_ms <= self.outcome_cursor_wall_ms || now <= self.outcome_cursor {
            return Vec::new();
        }
        let aligned_wall_now = UNIX_EPOCH + Duration::from_millis(wall_ms);
        let window_end = format_time(aligned_wall_now);
        let outcome_interval_start_ms = self.outcome_cursor_wall_ms;
        let outcome_interval_duration_ms = wall_ms - outcome_interval_start_ms;
        let outcome_schedule_complete = self.outcome_has_published
            && outcome_interval_duration_ms == self.config.publish_interval.as_millis() as u64;
        let outcome_interval_start =
            format_time(UNIX_EPOCH + Duration::from_millis(outcome_interval_start_ms));
        let clock_assessment = clock.assess(wall_now);
        let clock_checked_at =
            format_time(UNIX_EPOCH + Duration::from_millis(clock.checked_at_wall_ms));
        let mut outcome_counts = HashMap::with_capacity(self.config.coins.len());
        let providers = self.config.dataset.providers();
        for coin in &self.config.coins {
            let window = self.windows.get(coin).expect("registered coin");
            outcome_counts.insert(coin.clone(), unreported_outcomes(window, now, providers));
        }
        let mut events = Vec::with_capacity(self.config.coins.len() * providers.len());

        for coin in &self.config.coins {
            let window_id = format!(
                "{}:{}:{}:{}",
                self.config.runner,
                self.config.dataset.label(),
                coin,
                wall_ms
            );
            let window = self.windows.get(coin).expect("registered coin");
            let outcomes = outcome_counts.get(coin).expect("registered coin outcomes");
            let outcome_interval_id = format!(
                "{}:{}:{}:{}:{}:{}",
                self.config.run_id,
                self.config.runner,
                self.config.dataset.label(),
                coin,
                outcome_interval_start_ms,
                wall_ms
            );
            let sample_count = window.cohorts.len() as u64;
            let commit_delays = window
                .cohorts
                .iter()
                .map(|cohort| cohort.commit_delay_ms)
                .collect::<Vec<_>>();
            let commit_delay = distribution(commit_delays);
            let state_integrity_complete = window.last_integrity_loss.is_none_or(|lost| {
                now.saturating_duration_since(lost) > self.config.rolling_window
            });
            let queue_integrity_complete = providers.iter().all(|provider| {
                let dropped_at = signals.snapshot(*provider, coin).last_queue_drop_wall_ms;
                dropped_at == 0
                    || (dropped_at <= actual_wall_ms
                        && actual_wall_ms - dropped_at
                            > self.config.rolling_window.as_millis() as u64)
            });
            let integrity_complete = state_integrity_complete && queue_integrity_complete;
            let cohort_connections_live = providers
                .iter()
                .all(|provider| signals.snapshot(*provider, coin).connection_up);
            let cohort_messages_fresh = providers.iter().all(|provider| {
                let last_message = signals.snapshot(*provider, coin).last_message_wall_ms;
                stream_message_age_ms(actual_wall_ms, last_message)
                    .is_some_and(|age| age <= self.config.stale_after.as_millis() as u64)
            });
            let cohort_health_complete = integrity_complete
                && cohort_connections_live
                && cohort_messages_fresh
                && clock_assessment.healthy;
            let outcome_interval_complete = outcome_schedule_complete && cohort_health_complete;

            for &provider in providers {
                let values = window
                    .cohorts
                    .iter()
                    .map(|cohort| cohort.latency_ms[provider.index()])
                    .collect::<Vec<_>>();
                let summary = distribution(values);
                let latest = window.cohorts.back();
                let stream = signals.snapshot(provider, coin);
                let last_message_age_ms =
                    stream_message_age_ms(actual_wall_ms, stream.last_message_wall_ms);
                let fresh = last_message_age_ms
                    .is_some_and(|age| age <= self.config.stale_after.as_millis() as u64);
                let enough_samples = sample_count >= MIN_READY_SAMPLES as u64;
                let cohort_complete = cohort_health_complete && sample_count > 0;
                let ready = cohort_complete && enough_samples;
                let readiness = if !cohort_connections_live {
                    "disconnected"
                } else if !cohort_messages_fresh || !fresh {
                    "stale"
                } else if !clock_assessment.healthy {
                    clock_assessment.status
                } else if !integrity_complete {
                    "integrity-gap"
                } else if !enough_samples {
                    "warming-up"
                } else {
                    "ready"
                };
                let counters = &self.counters.get(coin).expect("registered coin")[provider.index()];
                let (matched_count, missing_count, mismatch_count) =
                    rolling_coverage(&window.coverage, provider);
                let last_success_age_ms = (ingest.last_success_wall_ms > 0
                    && ingest.last_success_wall_ms <= actual_wall_ms)
                    .then(|| actual_wall_ms - ingest.last_success_wall_ms);

                events.push(LatencyWindowEvent {
                    time: window_end.clone(),
                    schema: self.config.dataset.schema(),
                    event_type: EVENT_TYPE,
                    metric_kind: self.config.dataset.metric_kind(),
                    benchmark_version: env!("CARGO_PKG_VERSION"),
                    measurement_version: self.config.dataset.measurement_version(),
                    source_commit: SOURCE_COMMIT,
                    artifact_sha256: self.config.artifact_sha256.clone(),
                    event_id: format!(
                        "{}:{}:{}:{}",
                        self.config.dataset.schema(),
                        self.config.run_id,
                        window_id,
                        public_provider(provider)
                    ),
                    window_id: window_id.clone(),
                    window_end: window_end.clone(),
                    window_seconds: self.config.rolling_window.as_secs(),
                    publish_interval_seconds: self.config.publish_interval.as_secs(),
                    provider: public_provider(provider),
                    protocol: provider.transport(),
                    source: public_source(provider),
                    dataset: self.config.dataset.label(),
                    coin: coin.clone(),
                    cloud: self.config.cloud.clone(),
                    region: self.config.region.clone(),
                    metro: self.config.metro.clone(),
                    runner: self.config.runner.clone(),
                    location: if self.config.region == self.config.metro {
                        format!("{}-{}", self.config.cloud, self.config.region)
                    } else {
                        format!(
                            "{}-{}-{}",
                            self.config.cloud, self.config.region, self.config.metro
                        )
                    },
                    run_id: self.config.run_id.clone(),
                    runner_uptime_seconds: now.saturating_duration_since(self.started_at).as_secs(),
                    cohort: self.config.dataset.cohort(),
                    cohort_complete,
                    sample_count,
                    min_ready_samples: MIN_READY_SAMPLES as u64,
                    ready,
                    readiness,
                    coverage_count_scope: "rolling-window",
                    health_count_scope: "run-lifetime",
                    outcome_count_scope: "non-overlapping-publication-interval",
                    outcome_interval_id: outcome_interval_id.clone(),
                    outcome_interval_start: outcome_interval_start.clone(),
                    outcome_interval_end: window_end.clone(),
                    outcome_interval_duration_ms,
                    outcome_interval_complete,
                    outcome_complete_cohort_count: outcomes.complete,
                    outcome_foundation_strict_fastest_count: outcomes.strict_fastest
                        [Provider::FoundationWs.index()],
                    outcome_hydromancer_strict_fastest_count: outcomes.strict_fastest
                        [Provider::HydromancerWs.index()],
                    outcome_quicknode_strict_fastest_count: outcomes.strict_fastest
                        [Provider::QuickNodeGrpc.index()],
                    outcome_foundation_tied_fastest_count: outcomes.tied_fastest
                        [Provider::FoundationWs.index()],
                    outcome_hydromancer_tied_fastest_count: outcomes.tied_fastest
                        [Provider::HydromancerWs.index()],
                    outcome_quicknode_tied_fastest_count: outcomes.tied_fastest
                        [Provider::QuickNodeGrpc.index()],
                    outcome_tie_count: outcomes.ties,
                    p50_ms: summary.map(|value| value.p50),
                    p95_ms: summary.map(|value| value.p95),
                    p99_ms: summary.map(|value| value.p99),
                    min_ms: summary.map(|value| value.min),
                    max_ms: summary.map(|value| value.max),
                    mean_ms: summary.map(|value| value.mean),
                    standard_deviation_ms: summary.map(|value| value.standard_deviation),
                    p99_p50_spread_ms: summary.map(|value| value.p99 - value.p50),
                    latest_ms: latest.map(|value| value.latency_ms[provider.index()] as f64),
                    latest_event_at: latest.map(|value| {
                        format_time(UNIX_EPOCH + Duration::from_millis(value.event_ms))
                    }),
                    cohort_commit_delay_p99_ms: commit_delay.map(|value| value.p99),
                    observed_count: counters.observed,
                    matched_count,
                    missing_count,
                    mismatch_count,
                    matched_total: counters.matched,
                    missing_total: counters.missing,
                    mismatch_total: counters.mismatch,
                    duplicate_count: counters.duplicate,
                    late_count: counters.late,
                    orphaned_count: counters.orphaned,
                    negative_latency_count: counters.negative_latency,
                    future_timestamp_count: counters.future_timestamp,
                    signature_overflow_count: counters.signature_overflow,
                    complete_cohort_count: window.complete_cohorts,
                    pending_cohort_count: self
                        .pending
                        .iter()
                        .filter(|(key, _)| key.coin == *coin)
                        .map(|(_, pending)| pending.candidates.len() as u64)
                        .sum(),
                    state_eviction_count: window.state_evictions,
                    rolling_eviction_count: window.rolling_evictions,
                    coverage_eviction_count: window.coverage_evictions,
                    reconnects: counters.reconnects,
                    sequence_gaps: counters.sequence_gaps,
                    replay_count: counters.replay_count,
                    replay_gap_count: counters.replay_gaps,
                    queue_drops: stream.queue_dropped,
                    clock_healthy: clock_assessment.healthy,
                    clock_status: clock_assessment.status,
                    clock_source: clock.source,
                    clock_synchronized: clock.synchronized,
                    clock_offset_ms: clock.offset_ms,
                    clock_error_bound_ms: clock.error_bound_ms,
                    clock_max_offset_ms: clock.max_offset_ms,
                    clock_checked_at: clock_checked_at.clone(),
                    clock_check_age_ms: clock_assessment.age_ms,
                    connection_state: if stream.connection_up {
                        "connected"
                    } else {
                        "disconnected"
                    },
                    last_message_at: (stream.last_message_wall_ms > 0).then(|| {
                        format_time(UNIX_EPOCH + Duration::from_millis(stream.last_message_wall_ms))
                    }),
                    last_message_age_ms,
                    ingest_pending_batches: ingest.pending_batches,
                    ingest_pending_bytes: ingest.pending_bytes,
                    ingest_attempts: ingest.attempts,
                    ingest_batches_succeeded: ingest.batches_succeeded,
                    ingest_batches_failed: ingest.batches_failed,
                    ingest_batches_dropped: ingest.batches_dropped,
                    ingest_events_succeeded: ingest.events_succeeded,
                    ingest_events_dropped: ingest.events_dropped,
                    ingest_outbox_write_failures: ingest.outbox_write_failures,
                    ingest_outbox_delete_failures: ingest.outbox_delete_failures,
                    ingest_outbox_cap_rejections: ingest.outbox_cap_rejections,
                    ingest_last_success_at: (ingest.last_success_wall_ms > 0).then(|| {
                        format_time(UNIX_EPOCH + Duration::from_millis(ingest.last_success_wall_ms))
                    }),
                    ingest_last_success_age_ms: last_success_age_ms,
                });
            }
        }
        self.prepared_outcome_interval = Some(PreparedOutcomeInterval {
            end: now,
            end_wall_ms: wall_ms,
        });
        events
    }

    /// Commits outcome state only after the serialized event batch is durably admitted
    /// to the persistent Axiom outbox.
    pub fn commit_prepared_publication(&mut self) -> bool {
        let Some(prepared) = self.prepared_outcome_interval.take() else {
            return false;
        };
        if prepared.end_wall_ms <= self.outcome_cursor_wall_ms
            || prepared.end <= self.outcome_cursor
        {
            return false;
        }
        for window in self.windows.values_mut() {
            mark_outcomes_reported(window, prepared.end);
        }
        self.outcome_cursor = prepared.end;
        self.outcome_cursor_wall_ms = prepared.end_wall_ms;
        self.outcome_has_published = true;
        true
    }

    pub fn reject_prepared_publication(&mut self) {
        self.prepared_outcome_interval = None;
    }

    fn record_market(&mut self, event: MarketEvent) {
        let provider = event.provider;
        if !self.config.dataset.providers().contains(&provider) {
            return;
        }
        let coin = event.key.coin.clone();
        let counters = self.counters_mut(provider, &coin);
        if event.key.event_ms == 0 {
            counters.future_timestamp += 1;
            self.mark_integrity_loss(&coin, event.received);
            return;
        }
        if event.key.event_ms > event.received_wall_ms {
            let ahead = event.key.event_ms - event.received_wall_ms;
            if ahead > MAX_FUTURE_SKEW_MS {
                counters.future_timestamp += 1;
            } else {
                counters.negative_latency += 1;
            }
            self.mark_integrity_loss(&coin, event.received);
            return;
        }
        counters.observed += 1;
        let base = event.key.base();

        if self.pending.get(&base).is_some_and(|pending| {
            event
                .received
                .saturating_duration_since(pending.first_observed)
                >= self.config.cohort_timeout
        }) {
            self.settle(base.clone(), event.received, false);
        }

        if self.settled_bases.contains_key(&base) {
            let matched_before = self
                .settled
                .get(&event.key)
                .is_some_and(|settled| settled.matched_mask & (1 << provider.index()) != 0);
            let counters = self.counters_mut(provider, &coin);
            if matched_before {
                counters.duplicate += 1;
            } else {
                counters.late += 1;
            }
            return;
        }

        if !self.pending.contains_key(&base)
            && self.pending.len() >= self.config.max_pending
            && let Some(oldest) = self
                .pending
                .iter()
                .min_by_key(|(_, cohort)| cohort.first_observed)
                .map(|(key, _)| key.clone())
        {
            self.settle(oldest, event.received, true);
        }

        let cohort = self
            .pending
            .entry(base.clone())
            .or_insert_with(|| PendingBase::new(event.received));
        cohort.first_observed = cohort.first_observed.min(event.received);
        if !cohort.candidates.contains_key(&event.key.content)
            && cohort.candidates.len() >= MAX_SIGNATURES_PER_BASE
        {
            self.counters_mut(provider, &coin).signature_overflow += 1;
            self.mark_integrity_loss(&coin, event.received);
            return;
        }
        let candidate = cohort
            .candidates
            .entry(event.key.content.clone())
            .or_insert_with(|| PendingCandidate::new(event.received));
        candidate.first_observed = candidate.first_observed.min(event.received);
        if candidate.arrivals[provider.index()].is_some() {
            self.counters_mut(provider, &coin).duplicate += 1;
            return;
        }
        candidate.arrivals[provider.index()] = Some(Observation {
            received: event.received,
            received_wall_ms: event.received_wall_ms,
        });
    }

    fn settle(&mut self, key: BaseKey, now: Instant, evicted: bool) {
        let Some(mut pending) = self.pending.remove(&key) else {
            return;
        };
        self.insert_settled_base(key.clone(), now);
        if evicted {
            let window = self.windows.get_mut(&key.coin).expect("registered coin");
            window.state_evictions += 1;
            window.last_integrity_loss = Some(now);
        }
        for candidate in pending.candidates.values_mut() {
            for &provider in self.config.dataset.providers() {
                let arrived_after_deadline = candidate.arrivals[provider.index()]
                    .as_ref()
                    .is_some_and(|arrival| {
                        arrival
                            .received
                            .saturating_duration_since(pending.first_observed)
                            >= self.config.cohort_timeout
                    });
                if arrived_after_deadline {
                    candidate.arrivals[provider.index()] = None;
                    self.counters_mut(provider, &key.coin).late += 1;
                }
            }
        }
        let foundation_content = pending
            .candidates
            .iter()
            .filter(|(_, candidate)| candidate.arrivals[Provider::FoundationWs.index()].is_some())
            .map(|(content, _)| content.clone())
            .collect::<HashSet<_>>();
        let mut noncanonical_seen = [false; 3];
        for (content, candidate) in &pending.candidates {
            if foundation_content.contains(content) {
                continue;
            }
            for &provider in self.config.dataset.providers() {
                if candidate.arrivals[provider.index()].is_some() {
                    noncanonical_seen[provider.index()] = true;
                    self.counters_mut(provider, &key.coin).orphaned += 1;
                }
            }
        }
        for (content, candidate) in pending.candidates {
            if !foundation_content.contains(&content) {
                continue;
            }
            self.finalize_candidate(
                EventKey {
                    coin: key.coin.clone(),
                    event_ms: key.event_ms,
                    content,
                },
                candidate,
                now,
                noncanonical_seen,
            );
        }
    }

    fn finalize_candidate(
        &mut self,
        key: EventKey,
        candidate: PendingCandidate,
        now: Instant,
        noncanonical_seen: [bool; 3],
    ) {
        debug_assert!(candidate.arrivals[Provider::FoundationWs.index()].is_some());
        let providers = self.config.dataset.providers();
        let complete = providers
            .iter()
            .all(|provider| candidate.arrivals[provider.index()].is_some());
        let settled_at = if complete {
            providers
                .iter()
                .filter_map(|provider| candidate.arrivals[provider.index()].as_ref())
                .map(|arrival| arrival.received)
                .max()
                .expect("complete candidate has arrivals")
        } else {
            now
        };
        let mut outcomes = [CoverageOutcome::Missing; 3];
        let mut matched_mask = 0;
        for &provider in providers {
            let counters = self.counters_mut(provider, &key.coin);
            if candidate.arrivals[provider.index()].is_some() {
                counters.matched += 1;
                outcomes[provider.index()] = CoverageOutcome::Matched;
                matched_mask |= 1 << provider.index();
            } else if noncanonical_seen[provider.index()] {
                counters.missing += 1;
                counters.mismatch += 1;
                outcomes[provider.index()] = CoverageOutcome::Mismatch;
            } else {
                counters.missing += 1;
            }
        }
        {
            let window = self.windows.get_mut(&key.coin).expect("registered coin");
            let position = window
                .coverage
                .iter()
                .position(|cohort| cohort.settled_at > settled_at)
                .unwrap_or(window.coverage.len());
            window.coverage.insert(
                position,
                CoverageCohort {
                    settled_at,
                    outcomes,
                },
            );
            if window.coverage.len() > self.config.max_rolling_cohorts {
                window.coverage.pop_front();
                window.coverage_evictions += 1;
                window.last_integrity_loss = Some(now);
            }
        }
        if complete {
            let mut latency_ms = [0; 3];
            for &provider in providers {
                latency_ms[provider.index()] = candidate.arrivals[provider.index()]
                    .as_ref()
                    .expect("complete cohort")
                    .received_wall_ms
                    - key.event_ms;
            }
            let window = self.windows.get_mut(&key.coin).expect("registered coin");
            window.complete_cohorts += 1;
            let committed = CommittedCohort {
                committed_at: settled_at,
                event_ms: key.event_ms,
                latency_ms,
                commit_delay_ms: settled_at
                    .saturating_duration_since(candidate.first_observed)
                    .as_millis() as u64,
                outcome_reported: false,
            };
            let position = window
                .cohorts
                .iter()
                .position(|cohort| cohort.committed_at > settled_at)
                .unwrap_or(window.cohorts.len());
            window.cohorts.insert(position, committed);
            if window.cohorts.len() > self.config.max_rolling_cohorts {
                window.cohorts.pop_front();
                window.rolling_evictions += 1;
                window.last_integrity_loss = Some(now);
            }
        }
        self.insert_settled(
            key,
            SettledCohort {
                settled_at,
                matched_mask,
            },
        );
    }

    fn insert_settled(&mut self, key: EventKey, settled: SettledCohort) {
        if self.settled.len() >= self.config.max_settled
            && let Some(oldest) = self
                .settled
                .iter()
                .min_by_key(|(_, cohort)| cohort.settled_at)
                .map(|(key, _)| key.clone())
        {
            self.settled.remove(&oldest);
            let window = self.windows.get_mut(&oldest.coin).expect("registered coin");
            window.state_evictions += 1;
            window.last_integrity_loss = Some(settled.settled_at);
        }
        self.settled.insert(key, settled);
    }

    fn insert_settled_base(&mut self, key: BaseKey, settled_at: Instant) {
        if self.settled_bases.len() >= self.config.max_settled
            && let Some(oldest) = self
                .settled_bases
                .iter()
                .min_by_key(|(_, timestamp)| *timestamp)
                .map(|(key, _)| key.clone())
        {
            self.settled_bases.remove(&oldest);
            let window = self.windows.get_mut(&oldest.coin).expect("registered coin");
            window.state_evictions += 1;
            window.last_integrity_loss = Some(settled_at);
        }
        self.settled_bases.insert(key, settled_at);
    }

    fn prune(&mut self, now: Instant) {
        let settled_ttl = self.config.rolling_window;
        self.settled_bases
            .retain(|_, settled_at| now.saturating_duration_since(*settled_at) <= settled_ttl);
        self.settled
            .retain(|_, cohort| now.saturating_duration_since(cohort.settled_at) <= settled_ttl);
        for window in self.windows.values_mut() {
            window.cohorts.retain(|cohort| {
                now.saturating_duration_since(cohort.committed_at) <= self.config.rolling_window
            });
            window.coverage.retain(|cohort| {
                now.saturating_duration_since(cohort.settled_at) <= self.config.rolling_window
            });
        }
    }

    fn counters_mut(&mut self, provider: Provider, coin: &str) -> &mut ProviderCounters {
        self.counters
            .get_mut(coin)
            .unwrap_or_else(|| panic!("unregistered coin {coin}"))
            .get_mut(provider.index())
            .expect("provider index")
    }

    fn mark_integrity_loss(&mut self, coin: &str, now: Instant) {
        self.windows
            .get_mut(coin)
            .expect("registered coin")
            .last_integrity_loss = Some(now);
    }
}

fn distribution(mut values: Vec<u64>) -> Option<Distribution> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let mean = values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let distance = *value as f64 - mean;
            distance * distance
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(Distribution {
        p50: nearest_rank(&values, 50) as f64,
        p95: nearest_rank(&values, 95) as f64,
        p99: nearest_rank(&values, 99) as f64,
        min: values[0] as f64,
        max: values[values.len() - 1] as f64,
        mean,
        standard_deviation: variance.sqrt(),
    })
}

fn unreported_outcomes(window: &CoinWindow, now: Instant, providers: &[Provider]) -> OutcomeCounts {
    let mut counts = OutcomeCounts::default();
    for cohort in window
        .cohorts
        .iter()
        .filter(|cohort| !cohort.outcome_reported && cohort.committed_at <= now)
    {
        counts.complete += 1;
        let fastest = providers
            .iter()
            .map(|provider| cohort.latency_ms[provider.index()])
            .min()
            .expect("complete cohort");
        let fastest_providers = providers
            .iter()
            .copied()
            .filter(|provider| cohort.latency_ms[provider.index()] == fastest)
            .collect::<Vec<_>>();
        if fastest_providers.len() == 1 {
            counts.strict_fastest[fastest_providers[0].index()] += 1;
        } else {
            counts.ties += 1;
            for provider in fastest_providers {
                counts.tied_fastest[provider.index()] += 1;
            }
        }
    }
    counts
}

fn mark_outcomes_reported(window: &mut CoinWindow, through: Instant) {
    for cohort in window
        .cohorts
        .iter_mut()
        .filter(|cohort| !cohort.outcome_reported && cohort.committed_at <= through)
    {
        cohort.outcome_reported = true;
    }
}

fn rolling_coverage(coverage: &VecDeque<CoverageCohort>, provider: Provider) -> (u64, u64, u64) {
    let mut matched = 0;
    let mut missing = 0;
    let mut mismatch = 0;
    for cohort in coverage {
        match cohort.outcomes[provider.index()] {
            CoverageOutcome::Matched => matched += 1,
            CoverageOutcome::Missing => missing += 1,
            CoverageOutcome::Mismatch => {
                missing += 1;
                mismatch += 1;
            }
        }
    }
    (matched, missing, mismatch)
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

fn public_provider(provider: Provider) -> &'static str {
    match provider {
        Provider::FoundationWs => "hyperliquid",
        Provider::HydromancerWs => "hydromancer",
        Provider::QuickNodeGrpc => "quicknode",
    }
}

fn public_source(provider: Provider) -> &'static str {
    match provider {
        Provider::FoundationWs => "hyperliquid-ws",
        Provider::HydromancerWs => "hydromancer-ws",
        Provider::QuickNodeGrpc => "quicknode-grpc",
    }
}

fn system_time_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn aligned_window_end_ms(time: SystemTime, interval: Duration) -> u64 {
    let now_ms = system_time_ms(time);
    let interval_ms = interval.as_millis() as u64;
    debug_assert!(interval_ms > 0);
    now_ms - now_ms % interval_ms
}

fn stream_message_age_ms(reference_wall_ms: u64, message_wall_ms: u64) -> Option<u64> {
    (message_wall_ms > 0).then(|| reference_wall_ms.saturating_sub(message_wall_ms))
}

fn format_time(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventKey, LevelKey};

    fn config(now: Instant) -> (Benchmark, std::sync::Arc<RuntimeSignals>) {
        let coins = vec!["BTC".to_owned()];
        let mut config = BenchmarkConfig::production(
            Dataset::Bbo,
            coins.clone(),
            "aws".to_owned(),
            "nrt".to_owned(),
            "nrt".to_owned(),
            "aws-nrt-01".to_owned(),
            "run".to_owned(),
        );
        config.max_pending = 8;
        config.max_settled = 8;
        config.max_rolling_cohorts = 8;
        config.cohort_timeout = Duration::from_secs(1);
        (
            Benchmark::new(config, now, UNIX_EPOCH),
            std::sync::Arc::new(RuntimeSignals::new(&coins)),
        )
    }

    fn healthy_clock(wall_ms: u64) -> ClockHealthSnapshot {
        ClockHealthSnapshot {
            checked_at_wall_ms: wall_ms,
            source: "chrony",
            verified: true,
            synchronized: true,
            offset_ms: Some(0.1),
            error_bound_ms: Some(0.5),
            max_offset_ms: 5.0,
        }
    }

    fn book(provider: Provider, event_ms: u64, wall_ms: u64, now: Instant, px: &str) -> ProbeEvent {
        ProbeEvent::Market(MarketEvent {
            provider,
            key: EventKey {
                coin: "BTC".to_owned(),
                event_ms,
                content: ContentKey::Bbo {
                    bid: Some(LevelKey {
                        px: px.to_owned(),
                        sz: "1".to_owned(),
                        n: 1,
                    }),
                    ask: Some(LevelKey {
                        px: (px.parse::<u64>().unwrap() + 1).to_string(),
                        sz: "1".to_owned(),
                        n: 1,
                    }),
                },
            },
            received: now,
            received_wall_ms: wall_ms,
        })
    }

    fn trade(provider: Provider, event_ms: u64, wall_ms: u64, now: Instant) -> ProbeEvent {
        ProbeEvent::Market(MarketEvent {
            provider,
            key: EventKey {
                coin: "BTC".to_owned(),
                event_ms,
                content: ContentKey::Trade {
                    tid: 7,
                    side: "A".to_owned(),
                    px: "100".to_owned(),
                    sz: "2".to_owned(),
                    hash: "0xabc".to_owned(),
                    users: ["0xbuyer".to_owned(), "0xseller".to_owned()],
                },
            },
            received: now,
            received_wall_ms: wall_ms,
        })
    }

    fn settle_pending(benchmark: &mut Benchmark, now: Instant) {
        benchmark.tick(now + Duration::from_secs(2));
    }

    #[test]
    fn exact_distribution_uses_empirical_nearest_rank() {
        let values = (1..=1_000).collect::<Vec<_>>();
        let result = distribution(values).unwrap();
        assert_eq!(result.p50, 500.0);
        assert_eq!(result.p95, 950.0);
        assert_eq!(result.p99, 990.0);
        assert_eq!(result.mean, 500.5);
        let expected_standard_deviation = ((1_000_f64.powi(2) - 1.0) / 12.0).sqrt();
        assert!((result.standard_deviation - expected_standard_deviation).abs() < 0.000_001);
    }

    #[test]
    fn publication_timestamps_are_globally_aligned() {
        assert_eq!(
            aligned_window_end_ms(
                UNIX_EPOCH + Duration::from_millis(61_234),
                Duration::from_secs(30)
            ),
            60_000
        );
    }

    #[test]
    fn health_ages_use_actual_wall_time_not_the_aligned_label() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
            signals.set_test_state(provider, "BTC", true, 89_998, 0);
        }
        settle_pending(&mut benchmark, now);

        let events = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_millis(149_999),
            &signals,
            IngestHealthSnapshot {
                last_success_wall_ms: 120_000,
                ..IngestHealthSnapshot::default()
            },
            healthy_clock(149_999),
        );

        assert!(
            events
                .iter()
                .all(|event| event.window_end == "1970-01-01T00:02:00.000Z")
        );
        assert!(
            events
                .iter()
                .all(|event| event.last_message_age_ms == Some(60_001))
        );
        assert!(events.iter().all(|event| !event.cohort_complete));
        assert!(
            events
                .iter()
                .all(|event| event.ingest_last_success_age_ms == Some(29_999))
        );
    }

    #[test]
    fn concurrently_newer_stream_snapshots_are_fresh_with_zero_age() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
        }
        settle_pending(&mut benchmark, now);

        signals.set_test_state(Provider::FoundationWs, "BTC", true, 29_999, 0);
        signals.set_test_state(Provider::HydromancerWs, "BTC", true, 30_002, 0);
        signals.set_test_state(Provider::QuickNodeGrpc, "BTC", true, 30_011, 0);

        let events = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_millis(30_000),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );

        assert_eq!(events.len(), PROVIDERS.len());
        assert!(events.iter().all(|event| {
            event.cohort_complete
                && !event.ready
                && event.readiness == "warming-up"
                && event.sample_count == 1
                && event.min_ready_samples == MIN_READY_SAMPLES as u64
        }));
        assert_eq!(
            events
                .iter()
                .find(|event| event.provider == "hyperliquid")
                .unwrap()
                .last_message_age_ms,
            Some(1)
        );
        assert_eq!(
            events
                .iter()
                .find(|event| event.provider == "hydromancer")
                .unwrap()
                .last_message_age_ms,
            Some(0)
        );
        assert_eq!(
            events
                .iter()
                .find(|event| event.provider == "quicknode")
                .unwrap()
                .last_message_age_ms,
            Some(0)
        );
    }

    #[test]
    fn only_identical_three_source_cohorts_enter_the_latency_ring() {
        let now = Instant::now();
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for permutation in permutations {
            let (mut benchmark, _) = config(now);
            let arrivals = [
                (Provider::FoundationWs, 200),
                (Provider::HydromancerWs, 300),
                (Provider::QuickNodeGrpc, 100),
            ];
            for index in permutation {
                let (provider, latency) = arrivals[index];
                benchmark.record(book(provider, 1_000, 1_000 + latency, now, "100"));
            }
            settle_pending(&mut benchmark, now);

            let ring = &benchmark.windows["BTC"].cohorts;
            assert_eq!(ring.len(), 1);
            assert_eq!(ring[0].latency_ms, [200, 300, 100]);
            for provider in PROVIDERS {
                assert_eq!(benchmark.counters["BTC"][provider.index()].matched, 1);
            }
        }
    }

    #[test]
    fn fills_publish_an_exact_two_source_trade_ready_contract() {
        let now = Instant::now();
        let coins = vec!["BTC".to_owned()];
        let mut config = BenchmarkConfig::production(
            Dataset::Fills,
            coins.clone(),
            "aws".to_owned(),
            "nrt".to_owned(),
            "nrt".to_owned(),
            "aws-nrt-01".to_owned(),
            "fills-run".to_owned(),
        );
        config.cohort_timeout = Duration::from_secs(1);
        let mut benchmark = Benchmark::new(config, now, UNIX_EPOCH);
        let signals = RuntimeSignals::new(&coins);

        benchmark.record(trade(Provider::FoundationWs, 1_000, 1_200, now));
        benchmark.record(trade(Provider::QuickNodeGrpc, 1_000, 1_100, now));
        settle_pending(&mut benchmark, now);
        for provider in Dataset::Fills.providers() {
            signals.set_test_state(*provider, "BTC", true, 29_999, 0);
        }

        let events = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_millis(30_000),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );

        assert_eq!(benchmark.windows["BTC"].cohorts.len(), 1);
        assert_eq!(
            benchmark.windows["BTC"].cohorts[0].latency_ms,
            [200, 0, 100]
        );
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| {
            event.schema == FILLS_SCHEMA
                && event.metric_kind == FILLS_METRIC_KIND
                && event.measurement_version == FILLS_MEASUREMENT_VERSION
                && event.cohort == "hyperliquid-ws+quicknode-grpc"
                && event.sample_count == 1
        }));
        assert!(events.iter().all(|event| event.provider != "hydromancer"));
        assert_eq!(events[0].outcome_hydromancer_strict_fastest_count, 0);
        assert_eq!(events[0].outcome_hydromancer_tied_fastest_count, 0);
    }

    #[test]
    fn same_millisecond_book_revisions_are_two_distinct_cohorts() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
            benchmark.record(book(provider, 1_000, 1_101, now, "200"));
        }
        settle_pending(&mut benchmark, now);
        assert_eq!(benchmark.windows["BTC"].cohorts.len(), 2);
    }

    #[test]
    fn late_third_source_cannot_bypass_the_cohort_deadline() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        benchmark.record(book(Provider::FoundationWs, 1_000, 1_100, now, "100"));
        benchmark.record(book(Provider::HydromancerWs, 1_000, 1_100, now, "100"));
        benchmark.record(book(
            Provider::QuickNodeGrpc,
            1_000,
            1_100,
            now + Duration::from_secs(2),
            "100",
        ));
        assert!(benchmark.windows["BTC"].cohorts.is_empty());
        assert_eq!(
            benchmark.counters["BTC"][Provider::QuickNodeGrpc.index()].late,
            1
        );
    }

    #[test]
    fn a_sealed_timestamp_cannot_reopen_with_an_unseen_revision() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        benchmark.record(book(Provider::FoundationWs, 1_000, 1_100, now, "100"));
        settle_pending(&mut benchmark, now);

        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                1_000,
                1_200,
                now + Duration::from_secs(3),
                "200",
            ));
        }
        benchmark.tick(now + Duration::from_secs(5));

        assert!(benchmark.windows["BTC"].cohorts.is_empty());
        assert!(!benchmark.pending.contains_key(&BaseKey {
            coin: "BTC".to_owned(),
            event_ms: 1_000,
            trade_id: None,
        }));
        assert!(
            PROVIDERS
                .iter()
                .all(|provider| { benchmark.counters["BTC"][provider.index()].late == 1 })
        );
    }

    #[test]
    fn corrected_earliest_receipt_makes_deadline_order_independent() {
        let now = Instant::now();
        for processing_order in [[0, 1, 2], [1, 2, 0]] {
            let (mut benchmark, _) = config(now);
            let arrivals = [
                (Provider::FoundationWs, now),
                (Provider::HydromancerWs, now + Duration::from_millis(800)),
                (Provider::QuickNodeGrpc, now + Duration::from_millis(1_200)),
            ];
            for index in processing_order {
                let (provider, received) = arrivals[index];
                benchmark.record(book(provider, 1_000, 1_100, received, "100"));
            }
            benchmark.tick(now + Duration::from_secs(2));

            assert!(benchmark.windows["BTC"].cohorts.is_empty());
            assert_eq!(
                benchmark.counters["BTC"][Provider::QuickNodeGrpc.index()].late,
                1
            );
        }
    }

    #[test]
    fn revision_processing_order_cannot_retroactively_change_admission_or_outcomes() {
        let now = Instant::now();
        let run = |late_revision_first: bool| {
            let (mut benchmark, _) = config(now);
            let record_revision =
                |benchmark: &mut Benchmark, received: Instant, px: &'static str| {
                    for provider in PROVIDERS {
                        benchmark.record(book(provider, 1_000, 1_100, received, px));
                    }
                };
            if late_revision_first {
                record_revision(&mut benchmark, now + Duration::from_secs(2), "200");
                record_revision(&mut benchmark, now, "100");
            } else {
                record_revision(&mut benchmark, now, "100");
                record_revision(&mut benchmark, now + Duration::from_secs(2), "200");
            }
            benchmark.tick(now + Duration::from_secs(3));
            benchmark
        };

        let late_then_early = run(true);
        let early_then_late = run(false);
        for benchmark in [&late_then_early, &early_then_late] {
            let window = &benchmark.windows["BTC"];
            assert_eq!(window.cohorts.len(), 1);
            assert_eq!(window.cohorts[0].latency_ms, [100, 100, 100]);
            assert_eq!(window.complete_cohorts, 1);
            assert_eq!(window.state_evictions, 0);
            assert_eq!(window.rolling_evictions, 0);
            assert_eq!(window.coverage_evictions, 0);
            assert_eq!(window.last_integrity_loss, None);
            assert_eq!(
                unreported_outcomes(window, now + Duration::from_secs(3), &PROVIDERS).complete,
                1
            );
            assert!(benchmark.pending.is_empty());
            assert_eq!(benchmark.settled_bases.len(), 1);
            assert_eq!(benchmark.settled.len(), 1);
            for provider in PROVIDERS {
                let counters = &benchmark.counters["BTC"][provider.index()];
                assert_eq!(counters.observed, 2);
                assert_eq!(counters.matched, 1);
                assert_eq!(counters.missing, 0);
                assert_eq!(counters.mismatch, 0);
                assert_eq!(counters.duplicate, 0);
                assert_eq!(counters.late, 1);
                assert_eq!(counters.orphaned, 0);
                assert_eq!(counters.signature_overflow, 0);
                assert_eq!(rolling_coverage(&window.coverage, provider), (1, 0, 0));
            }
        }

        assert_eq!(
            late_then_early.windows["BTC"].cohorts[0].latency_ms,
            early_then_late.windows["BTC"].cohorts[0].latency_ms
        );
        assert_eq!(
            unreported_outcomes(
                &late_then_early.windows["BTC"],
                now + Duration::from_secs(3),
                &PROVIDERS,
            ),
            unreported_outcomes(
                &early_then_late.windows["BTC"],
                now + Duration::from_secs(3),
                &PROVIDERS,
            )
        );
    }

    #[test]
    fn replay_inside_the_rolling_window_cannot_duplicate_a_sample() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
        }
        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                1_000,
                1_100,
                now + Duration::from_secs(11),
                "100",
            ));
        }
        assert_eq!(benchmark.windows["BTC"].cohorts.len(), 1);
    }

    #[test]
    fn cross_task_processing_order_cannot_change_latest_or_pruning_order() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                2_000,
                2_100,
                now + Duration::from_secs(2),
                "200",
            ));
        }
        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                1_000,
                1_100,
                now + Duration::from_secs(1),
                "100",
            ));
        }
        benchmark.tick(now + Duration::from_secs(4));
        let ring = &benchmark.windows["BTC"].cohorts;
        assert_eq!(ring.len(), 2);
        assert_eq!(ring[0].event_ms, 1_000);
        assert_eq!(ring[1].event_ms, 2_000);
    }

    #[test]
    fn timeout_distinguishes_mismatch_from_missing() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        benchmark.record(book(Provider::FoundationWs, 1_000, 1_200, now, "100"));
        benchmark.record(book(Provider::HydromancerWs, 1_000, 1_300, now, "90"));
        benchmark.tick(now + Duration::from_secs(2));

        assert!(benchmark.windows["BTC"].cohorts.is_empty());
        assert_eq!(
            benchmark.counters["BTC"][Provider::HydromancerWs.index()].mismatch,
            1
        );
        assert_eq!(
            benchmark.counters["BTC"][Provider::QuickNodeGrpc.index()].missing,
            1
        );
    }

    #[test]
    fn future_timestamps_never_turn_into_zero_latency() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        benchmark.record(book(Provider::QuickNodeGrpc, 2_000, 1_900, now, "100"));
        benchmark.record(book(Provider::QuickNodeGrpc, 10_000, 1_000, now, "101"));
        let counters = &benchmark.counters["BTC"][Provider::QuickNodeGrpc.index()];
        assert_eq!(counters.negative_latency, 1);
        assert_eq!(counters.future_timestamp, 1);
        assert_eq!(counters.observed, 0);
    }

    #[test]
    fn rolling_state_has_a_hard_cap_and_marks_integrity_loss() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        benchmark.config.max_rolling_cohorts = 1;
        for event_ms in [1_000, 2_000] {
            for provider in PROVIDERS {
                benchmark.record(book(provider, event_ms, event_ms + 100, now, "100"));
            }
        }
        settle_pending(&mut benchmark, now);
        let window = &benchmark.windows["BTC"];
        assert_eq!(window.cohorts.len(), 1);
        assert_eq!(window.rolling_evictions, 1);
        assert_eq!(
            window.last_integrity_loss,
            Some(now + Duration::from_secs(2))
        );
    }

    #[test]
    fn tombstone_cap_pressure_is_never_silent() {
        let now = Instant::now();
        let (mut benchmark, _) = config(now);
        benchmark.config.max_settled = 1;
        for event_ms in [1_000, 2_000] {
            for provider in PROVIDERS {
                benchmark.record(book(provider, event_ms, event_ms + 100, now, "100"));
            }
        }
        settle_pending(&mut benchmark, now);
        let window = &benchmark.windows["BTC"];
        assert!(window.state_evictions >= 1);
        assert_eq!(window.last_integrity_loss, Some(now));
    }

    #[test]
    fn published_contract_has_three_equal_cohort_sample_counts() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
        }
        settle_pending(&mut benchmark, now);
        let events = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|event| event.sample_count == 1));
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            3
        );
        assert!(
            events
                .iter()
                .all(|event| event.window_id == "aws-nrt-01:bbo:BTC:30000")
        );
        assert!(
            events
                .iter()
                .all(|event| event.window_end == "1970-01-01T00:00:30.000Z")
        );
        assert_eq!(events[0].schema, SCHEMA);
        assert_eq!(events[0].metric_kind, METRIC_KIND);
        assert_eq!(
            events
                .iter()
                .map(|event| event.provider)
                .collect::<Vec<_>>(),
            vec!["hyperliquid", "hydromancer", "quicknode"]
        );
        assert_eq!(
            events.iter().map(|event| event.source).collect::<Vec<_>>(),
            vec!["hyperliquid-ws", "hydromancer-ws", "quicknode-grpc"]
        );
        let encoded = serde_json::to_value(&events[2]).unwrap();
        assert_eq!(encoded["event_type"], "latency_window");
        assert_eq!(
            encoded["event_id"],
            "hyperliquid-market-benchmark-v1:run:aws-nrt-01:bbo:BTC:30000:quicknode"
        );
        assert_eq!(encoded["window_seconds"], 300);
        assert_eq!(encoded["publish_interval_seconds"], 30);
        assert_eq!(encoded["p99_ms"], 100.0);
        assert_eq!(encoded["standard_deviation_ms"], 0.0);
        assert_eq!(encoded["p99_p50_spread_ms"], 0.0);
        assert_eq!(encoded["benchmark_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(encoded["measurement_version"], MEASUREMENT_VERSION);
        assert_eq!(encoded["source_commit"], SOURCE_COMMIT);
        assert_eq!(encoded["artifact_sha256"], "unavailable");
        assert_eq!(encoded["outcome_complete_cohort_count"], 1);
        assert_eq!(encoded["outcome_tie_count"], 1);
        assert_eq!(encoded["outcome_quicknode_tied_fastest_count"], 1);
        assert_eq!(encoded["outcome_interval_complete"], false);
        assert_eq!(encoded["clock_status"], "healthy");
        assert!(encoded.get("histogram").is_none());
    }

    #[test]
    fn outcome_intervals_are_non_overlapping_and_never_count_a_cohort_twice() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        let first_latencies = [200, 300, 100];
        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                1_000,
                1_000 + first_latencies[provider.index()],
                now + Duration::from_secs(5),
                "100",
            ));
        }
        let first = benchmark.window_events(
            now + Duration::from_secs(10),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert_eq!(first.len(), 3);
        assert!(first.iter().all(|event| {
            !event.outcome_interval_complete
                && event.outcome_interval_start == "1970-01-01T00:00:00.000Z"
                && event.outcome_interval_end == "1970-01-01T00:00:30.000Z"
                && event.outcome_interval_duration_ms == 30_000
                && event.outcome_complete_cohort_count == 1
                && event.outcome_quicknode_strict_fastest_count == 1
                && event.outcome_tie_count == 0
        }));
        assert!(benchmark.commit_prepared_publication());

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 59_000, 0);
        }
        let second_latencies = [300, 100, 200];
        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                2_000,
                2_000 + second_latencies[provider.index()],
                now + Duration::from_secs(35),
                "200",
            ));
        }
        let second = benchmark.window_events(
            now + Duration::from_secs(40),
            UNIX_EPOCH + Duration::from_secs(60),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(60_000),
        );
        assert!(second.iter().all(|event| {
            event.outcome_interval_complete
                && event.outcome_interval_start == first[0].outcome_interval_end
                && event.outcome_complete_cohort_count == 1
                && event.outcome_hydromancer_strict_fastest_count == 1
                && event.outcome_quicknode_strict_fastest_count == 0
        }));
        assert_ne!(first[0].outcome_interval_id, second[0].outcome_interval_id);
        assert!(benchmark.commit_prepared_publication());

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 89_000, 0);
        }
        let third = benchmark.window_events(
            now + Duration::from_secs(70),
            UNIX_EPOCH + Duration::from_secs(90),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(90_000),
        );
        assert!(third.iter().all(|event| {
            event.outcome_interval_complete
                && event.outcome_interval_start == second[0].outcome_interval_end
                && event.outcome_complete_cohort_count == 0
                && event.outcome_foundation_strict_fastest_count == 0
                && event.outcome_hydromancer_strict_fastest_count == 0
                && event.outcome_quicknode_strict_fastest_count == 0
                && event.outcome_tie_count == 0
        }));
    }

    #[test]
    fn rejected_durable_submission_retains_outcomes_for_a_transparent_retry() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(
                provider,
                1_000,
                1_100 + provider.index() as u64,
                now + Duration::from_secs(5),
                "100",
            ));
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        let rejected = benchmark.window_events(
            now + Duration::from_secs(10),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert!(
            rejected
                .iter()
                .all(|event| event.outcome_complete_cohort_count == 1)
        );
        benchmark.reject_prepared_publication();
        assert!(
            benchmark.windows["BTC"]
                .cohorts
                .iter()
                .all(|cohort| !cohort.outcome_reported)
        );

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 59_000, 0);
        }
        let retry = benchmark.window_events(
            now + Duration::from_secs(40),
            UNIX_EPOCH + Duration::from_secs(60),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(60_000),
        );
        assert!(retry.iter().all(|event| {
            !event.outcome_interval_complete
                && event.outcome_interval_start == "1970-01-01T00:00:00.000Z"
                && event.outcome_interval_end == "1970-01-01T00:01:00.000Z"
                && event.outcome_interval_duration_ms == 60_000
                && event.outcome_complete_cohort_count == 1
        }));
        assert_ne!(rejected[0].event_id, retry[0].event_id);
        assert_ne!(
            rejected[0].outcome_interval_id,
            retry[0].outcome_interval_id
        );
        assert!(benchmark.commit_prepared_publication());
        assert!(
            benchmark.windows["BTC"]
                .cohorts
                .iter()
                .all(|cohort| cohort.outcome_reported)
        );

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 89_000, 0);
        }
        let next = benchmark.window_events(
            now + Duration::from_secs(70),
            UNIX_EPOCH + Duration::from_secs(90),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(90_000),
        );
        assert!(next.iter().all(|event| {
            event.outcome_interval_complete
                && event.outcome_interval_start == retry[0].outcome_interval_end
                && event.outcome_complete_cohort_count == 0
        }));
    }

    #[test]
    fn outcome_counts_distinguish_strict_fastest_from_two_and_three_way_ties() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        for (index, latencies) in [[200, 300, 100], [200, 100, 100], [100, 100, 100]]
            .into_iter()
            .enumerate()
        {
            let event_ms = (index as u64 + 1) * 1_000;
            for provider in PROVIDERS {
                benchmark.record(book(
                    provider,
                    event_ms,
                    event_ms + latencies[provider.index()],
                    now + Duration::from_secs(5 + index as u64),
                    &(100 + index).to_string(),
                ));
            }
        }
        let events = benchmark.window_events(
            now + Duration::from_secs(10),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert!(events.iter().all(|event| {
            event.outcome_complete_cohort_count == 3
                && event.outcome_foundation_strict_fastest_count == 0
                && event.outcome_hydromancer_strict_fastest_count == 0
                && event.outcome_quicknode_strict_fastest_count == 1
                && event.outcome_tie_count == 2
                && event.outcome_foundation_tied_fastest_count == 1
                && event.outcome_hydromancer_tied_fastest_count == 2
                && event.outcome_quicknode_tied_fastest_count == 2
        }));
        assert_eq!(
            events[0].outcome_foundation_strict_fastest_count
                + events[0].outcome_hydromancer_strict_fastest_count
                + events[0].outcome_quicknode_strict_fastest_count
                + events[0].outcome_tie_count,
            events[0].outcome_complete_cohort_count
        );
    }

    #[test]
    fn rolling_source_dispersion_is_population_standard_deviation_and_p99_p50_spread() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        for (index, latencies) in [[100, 200, 300], [300, 400, 500]].into_iter().enumerate() {
            let event_ms = (index as u64 + 1) * 1_000;
            for provider in PROVIDERS {
                benchmark.record(book(
                    provider,
                    event_ms,
                    event_ms + latencies[provider.index()],
                    now + Duration::from_secs(index as u64 + 1),
                    &(100 + index).to_string(),
                ));
            }
        }
        let events = benchmark.window_events(
            now + Duration::from_secs(10),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        for (provider, p50, p99) in [
            ("hyperliquid", 100.0, 300.0),
            ("hydromancer", 200.0, 400.0),
            ("quicknode", 300.0, 500.0),
        ] {
            let event = events
                .iter()
                .find(|event| event.provider == provider)
                .unwrap();
            assert_eq!(event.p50_ms, Some(p50));
            assert_eq!(event.p99_ms, Some(p99));
            assert_eq!(event.standard_deviation_ms, Some(100.0));
            assert_eq!(event.p99_p50_spread_ms, Some(200.0));
        }
    }

    #[test]
    fn rolling_coverage_matches_the_same_five_minute_scope_as_latency() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        benchmark.record(book(Provider::FoundationWs, 1_000, 1_100, now, "100"));
        benchmark.record(book(Provider::HydromancerWs, 1_000, 1_100, now, "90"));
        benchmark.tick(now + Duration::from_secs(2));
        let events = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        let hydromancer = events
            .iter()
            .find(|event| event.provider == "hydromancer")
            .unwrap();
        assert_eq!(hydromancer.matched_count, 0);
        assert_eq!(hydromancer.missing_count, 1);
        assert_eq!(hydromancer.mismatch_count, 1);
        assert_eq!(hydromancer.missing_total, 1);
        assert_eq!(hydromancer.mismatch_total, 1);
        let quicknode = events
            .iter()
            .find(|event| event.provider == "quicknode")
            .unwrap();
        assert_eq!(quicknode.missing_count, 1);
        assert_eq!(quicknode.missing_total, 1);
    }

    #[test]
    fn queue_drop_and_clock_rejection_invalidate_the_whole_cohort_window() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        settle_pending(&mut benchmark, now);
        let healthy = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert!(healthy.iter().all(|event| event.cohort_complete));

        signals.set_test_state(Provider::HydromancerWs, "BTC", false, 0, 0);
        let disconnected = benchmark.window_events(
            now + Duration::from_secs(32),
            UNIX_EPOCH + Duration::from_secs(60),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(60_000),
        );
        assert!(
            disconnected
                .iter()
                .all(|event| !event.cohort_complete && event.readiness == "disconnected")
        );
        signals.set_test_state(Provider::HydromancerWs, "BTC", true, 59_000, 0);

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 89_000, 0);
        }
        signals.set_test_state(Provider::FoundationWs, "BTC", true, 89_000, 89_500);
        let dropped = benchmark.window_events(
            now + Duration::from_secs(62),
            UNIX_EPOCH + Duration::from_secs(90),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(90_000),
        );
        assert!(dropped.iter().all(|event| !event.cohort_complete));
        assert!(
            dropped
                .iter()
                .all(|event| event.readiness == "integrity-gap")
        );

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 119_000, 0);
        }
        benchmark.record(book(
            Provider::QuickNodeGrpc,
            120_100,
            120_000,
            now + Duration::from_secs(91),
            "200",
        ));
        let clock_rejected = benchmark.window_events(
            now + Duration::from_secs(92),
            UNIX_EPOCH + Duration::from_secs(120),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(120_000),
        );
        assert!(clock_rejected.iter().all(|event| !event.cohort_complete));
    }

    #[test]
    fn runtime_clock_health_gates_absolute_latency_and_interval_evidence() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        for provider in PROVIDERS {
            benchmark.record(book(provider, 1_000, 1_100, now, "100"));
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        settle_pending(&mut benchmark, now);
        let first = benchmark.window_events(
            now + Duration::from_secs(2),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert!(first.iter().all(|event| event.cohort_complete));
        assert!(benchmark.commit_prepared_publication());

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 59_000, 0);
        }
        let mut unsynchronized = healthy_clock(60_000);
        unsynchronized.synchronized = false;
        let unsynchronized_events = benchmark.window_events(
            now + Duration::from_secs(32),
            UNIX_EPOCH + Duration::from_secs(60),
            &signals,
            IngestHealthSnapshot::default(),
            unsynchronized,
        );
        assert!(unsynchronized_events.iter().all(|event| {
            !event.clock_healthy
                && !event.cohort_complete
                && !event.outcome_interval_complete
                && event.readiness == "clock-unsynchronized"
        }));
        assert!(benchmark.commit_prepared_publication());

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 89_000, 0);
        }
        let mut excessive_offset = healthy_clock(90_000);
        excessive_offset.offset_ms = Some(-5.001);
        let excessive_events = benchmark.window_events(
            now + Duration::from_secs(62),
            UNIX_EPOCH + Duration::from_secs(90),
            &signals,
            IngestHealthSnapshot::default(),
            excessive_offset,
        );
        assert!(excessive_events.iter().all(|event| {
            !event.clock_healthy
                && !event.cohort_complete
                && event.readiness == "clock-offset-exceeded"
        }));
        assert!(benchmark.commit_prepared_publication());

        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 119_000, 0);
        }
        let mut boundary = healthy_clock(120_000);
        boundary.offset_ms = Some(-5.0);
        let boundary_events = benchmark.window_events(
            now + Duration::from_secs(92),
            UNIX_EPOCH + Duration::from_secs(120),
            &signals,
            IngestHealthSnapshot::default(),
            boundary,
        );
        assert!(boundary_events.iter().all(|event| {
            event.clock_healthy
                && event.cohort_complete
                && event.outcome_interval_complete
                && event.readiness == "warming-up"
        }));
    }

    #[test]
    fn publication_identity_and_provenance_cannot_change_within_an_aligned_window() {
        let now = Instant::now();
        let (mut benchmark, signals) = config(now);
        benchmark.config.artifact_sha256 = "a".repeat(64);
        for provider in PROVIDERS {
            signals.set_test_state(provider, "BTC", true, 29_000, 0);
        }
        let events = benchmark.window_events(
            now + Duration::from_secs(30),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        let replay_bytes = serde_json::to_vec(&events).unwrap();
        assert!(events.iter().all(|event| {
            event.benchmark_version == env!("CARGO_PKG_VERSION")
                && event.measurement_version == MEASUREMENT_VERSION
                && event.source_commit == SOURCE_COMMIT
                && event.artifact_sha256 == "a".repeat(64)
        }));
        assert!(
            SOURCE_COMMIT == "unavailable"
                || ((7..=64).contains(&SOURCE_COMMIT.len())
                    && SOURCE_COMMIT.bytes().all(|byte| byte.is_ascii_hexdigit()))
        );

        let retry = benchmark.window_events(
            now + Duration::from_secs(30),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert_eq!(replay_bytes, serde_json::to_vec(&retry).unwrap());
        assert!(benchmark.commit_prepared_publication());

        let conflicting_regeneration = benchmark.window_events(
            now + Duration::from_secs(31),
            UNIX_EPOCH + Duration::from_secs(31),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(31_000),
        );
        assert!(conflicting_regeneration.is_empty());
    }

    #[test]
    fn window_id_is_coin_scoped_for_multi_coin_processes() {
        let now = Instant::now();
        let coins = vec!["BTC".to_owned(), "ETH".to_owned()];
        let config = BenchmarkConfig::production(
            Dataset::Bbo,
            coins.clone(),
            "aws".to_owned(),
            "nrt".to_owned(),
            "nrt".to_owned(),
            "runner".to_owned(),
            "run".to_owned(),
        );
        let mut benchmark = Benchmark::new(config, now, UNIX_EPOCH);
        let signals = RuntimeSignals::new(&coins);
        let events = benchmark.window_events(
            now + Duration::from_secs(30),
            UNIX_EPOCH + Duration::from_secs(30),
            &signals,
            IngestHealthSnapshot::default(),
            healthy_clock(30_000),
        );
        assert_eq!(events.len(), 6);
        assert!(
            events
                .iter()
                .filter(|event| event.coin == "BTC")
                .all(|event| event.window_id == "runner:bbo:BTC:30000")
        );
        assert!(
            events
                .iter()
                .filter(|event| event.coin == "ETH")
                .all(|event| event.window_id == "runner:bbo:ETH:30000")
        );
    }
}

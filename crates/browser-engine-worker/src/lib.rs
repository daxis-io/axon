//! Internal browser worker artifact for bundle, startup, and contract tracking.

use std::mem::size_of;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

use browser_sdk::{preferred_target, BrowserWorkerRequestEnvelope, BrowserWorkerResponseEnvelope};
use query_contract::{
    BrowserAccessMode, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
    QueryMetricsSummary, QueryRequest,
};
use serde::{Deserialize, Serialize};
use wasm_query_runtime::{BrowserObjectAccessMode, BrowserRuntimeConfig, BrowserRuntimeSession};

pub const OWNER: &str = "Web platform team";
pub const RESPONSIBILITY: &str =
    "Internal browser worker artifact used for size, startup, and footprint reporting.";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerStartupReport {
    pub target: ExecutionTarget,
    pub access_mode: BrowserAccessMode,
    pub startup_duration_ms: u64,
    pub request_envelope_bytes: u64,
    pub error_envelope_bytes: u64,
    pub request_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub snapshot_preflight_timeout_ms: u64,
    pub snapshot_preflight_max_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerMemoryBaseline {
    pub runtime_session_bytes: u64,
    pub runtime_config_bytes: u64,
    pub request_envelope_struct_bytes: u64,
    pub response_envelope_struct_bytes: u64,
    pub query_metrics_struct_bytes: u64,
    pub request_envelope_json_bytes: u64,
    pub error_envelope_json_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerArtifactReport {
    pub startup: BrowserWorkerStartupReport,
    pub memory: BrowserWorkerMemoryBaseline,
}

pub fn worker_target() -> ExecutionTarget {
    preferred_target()
}

pub fn cold_start_report() -> Result<BrowserWorkerStartupReport, QueryError> {
    let started = wall_clock_start();
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())?;
    let request = sample_request();
    let error_response = sample_error_response();
    let config = session.config();

    Ok(BrowserWorkerStartupReport {
        target: worker_target(),
        access_mode: browser_access_mode(config.object_access_mode),
        startup_duration_ms: wall_clock_duration_ms(started),
        request_envelope_bytes: serialized_len(&request)?,
        error_envelope_bytes: serialized_len(&error_response)?,
        request_timeout_ms: config.request_timeout_ms,
        execution_timeout_ms: config.execution_timeout_ms,
        snapshot_preflight_timeout_ms: config.snapshot_preflight_timeout_ms,
        snapshot_preflight_max_concurrency: config.snapshot_preflight_max_concurrency,
    })
}

pub fn memory_baseline_report() -> Result<BrowserWorkerMemoryBaseline, QueryError> {
    let request = sample_request();
    let error_response = sample_error_response();

    Ok(BrowserWorkerMemoryBaseline {
        runtime_session_bytes: size_of::<BrowserRuntimeSession>() as u64,
        runtime_config_bytes: size_of::<BrowserRuntimeConfig>() as u64,
        request_envelope_struct_bytes: size_of::<BrowserWorkerRequestEnvelope>() as u64,
        response_envelope_struct_bytes: size_of::<BrowserWorkerResponseEnvelope>() as u64,
        query_metrics_struct_bytes: size_of::<QueryMetricsSummary>() as u64,
        request_envelope_json_bytes: serialized_len(&request)?,
        error_envelope_json_bytes: serialized_len(&error_response)?,
    })
}

pub fn artifact_report() -> Result<BrowserWorkerArtifactReport, QueryError> {
    Ok(BrowserWorkerArtifactReport {
        startup: cold_start_report()?,
        memory: memory_baseline_report()?,
    })
}

fn sample_request() -> BrowserWorkerRequestEnvelope {
    BrowserWorkerRequestEnvelope::new(
        "browser-worker-cold-start",
        QueryRequest::new(
            "https://example.invalid/tables/axon_fixture",
            "SELECT id FROM axon_table LIMIT 1",
            worker_target(),
        ),
    )
}

fn sample_error_response() -> BrowserWorkerResponseEnvelope {
    BrowserWorkerResponseEnvelope::error(
        "browser-worker-cold-start",
        QueryError::new(
            QueryErrorCode::FallbackRequired,
            "browser worker startup probe is not executing a live query",
            worker_target(),
        )
        .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint),
    )
}

fn serialized_len<T>(value: &T) -> Result<u64, QueryError>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).map_err(|error| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            format!("worker artifact report serialization failed: {error}"),
            worker_target(),
        )
    })?;

    u64::try_from(bytes.len()).map_err(|_| {
        QueryError::new(
            QueryErrorCode::ExecutionFailed,
            "worker artifact report size overflowed u64",
            worker_target(),
        )
    })
}

fn browser_access_mode(access_mode: BrowserObjectAccessMode) -> BrowserAccessMode {
    match access_mode {
        BrowserObjectAccessMode::BrowserSafeHttp => BrowserAccessMode::BrowserSafeHttp,
        BrowserObjectAccessMode::CloudObjectStore => BrowserAccessMode::CloudObjectStore,
    }
}

#[cfg(not(target_arch = "wasm32"))]
type StartupClock = Instant;

#[cfg(target_arch = "wasm32")]
type StartupClock = f64;

#[cfg(not(target_arch = "wasm32"))]
fn wall_clock_start() -> StartupClock {
    Instant::now()
}

#[cfg(target_arch = "wasm32")]
fn wall_clock_start() -> StartupClock {
    js_sys::Date::now()
}

#[cfg(not(target_arch = "wasm32"))]
fn wall_clock_duration_ms(started: StartupClock) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(target_arch = "wasm32")]
fn wall_clock_duration_ms(started: StartupClock) -> u64 {
    let elapsed = (js_sys::Date::now() - started).max(0.0);
    elapsed.min(u64::MAX as f64) as u64
}

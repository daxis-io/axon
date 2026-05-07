//! Internal browser worker artifact for bundle, startup, and contract tracking.

use std::mem::size_of;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

use browser_sdk::{
    preferred_target, ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerCommand,
    BrowserWorkerResponseEnvelope,
};
use query_contract::{
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget,
    FallbackReason, QueryError, QueryErrorCode, QueryMetricsSummary, QueryRequest,
};
use serde::{Deserialize, Serialize};
use wasm_query_runtime::{BrowserObjectAccessMode, BrowserRuntimeConfig, BrowserRuntimeSession};
use wasm_query_session::{BrowserQuerySession, BrowserSessionQueryResult};

pub const OWNER: &str = "Web platform team";
pub const RESPONSIBILITY: &str =
    "Internal browser worker artifact used for session-backed command handling, size, startup, and footprint reporting.";
pub const BROWSER_ENGINE_WORKER_WASM_ARTIFACT: &str = "browser_engine_worker.wasm";
pub const DEFAULT_SESSION_CACHE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserRuntimeSku {
    Narrow,
    Sql,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserResultTransport {
    ArrowIpc,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerCapabilities {
    pub session_shell: bool,
    pub browser_datafusion: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerArtifactIdentity {
    pub package_name: String,
    pub package_version: String,
    pub wasm_artifact: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerStartupReport {
    pub target: ExecutionTarget,
    pub access_mode: BrowserAccessMode,
    pub startup_duration_ms: u64,
    pub command_envelope_bytes: u64,
    pub error_envelope_bytes: u64,
    pub request_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub snapshot_preflight_timeout_ms: u64,
    pub snapshot_preflight_max_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerMemoryBaseline {
    pub runtime_session_bytes: u64,
    pub query_session_bytes: u64,
    pub runtime_config_bytes: u64,
    pub command_struct_bytes: u64,
    pub response_envelope_struct_bytes: u64,
    pub query_metrics_struct_bytes: u64,
    pub command_envelope_json_bytes: u64,
    pub error_envelope_json_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerArtifactReport {
    pub runtime_sku: BrowserRuntimeSku,
    pub result_transport: BrowserResultTransport,
    pub capabilities: BrowserWorkerCapabilities,
    pub identity: BrowserWorkerArtifactIdentity,
    pub startup: BrowserWorkerStartupReport,
    pub memory: BrowserWorkerMemoryBaseline,
}

#[derive(Clone, Debug)]
pub struct BrowserWorker {
    session: BrowserQuerySession,
}

impl BrowserWorker {
    pub fn new(
        runtime_config: BrowserRuntimeConfig,
        max_cached_bytes: u64,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            session: BrowserQuerySession::new(runtime_config, max_cached_bytes)?,
        })
    }

    pub fn runtime(&self) -> &BrowserRuntimeSession {
        self.session.runtime()
    }

    pub fn session(&self) -> &BrowserQuerySession {
        &self.session
    }

    pub async fn handle_command(
        &mut self,
        command: BrowserWorkerCommand,
    ) -> BrowserWorkerResponseEnvelope {
        match command {
            BrowserWorkerCommand::OpenTable(command) => {
                match self
                    .session
                    .open_table(command.name.clone(), command.snapshot)
                {
                    Ok(()) => {
                        BrowserWorkerResponseEnvelope::opened(command.request_id, command.name)
                    }
                    Err(error) => BrowserWorkerResponseEnvelope::error(command.request_id, error),
                }
            }
            BrowserWorkerCommand::OpenDeltaTable(command) => {
                match self
                    .session
                    .open_delta_table(command.name.clone(), command.snapshot)
                    .await
                {
                    Ok(()) => {
                        BrowserWorkerResponseEnvelope::opened(command.request_id, command.name)
                    }
                    Err(error) => BrowserWorkerResponseEnvelope::error(command.request_id, error),
                }
            }
            BrowserWorkerCommand::Sql(command) => {
                match self.session.sql(&command.name, &command.request).await {
                    Ok(result) => success_response(command.request_id, result),
                    Err(error) => BrowserWorkerResponseEnvelope::error(command.request_id, error),
                }
            }
            BrowserWorkerCommand::Dispose(command) => {
                if self.session.dispose_table(&command.name) {
                    BrowserWorkerResponseEnvelope::disposed(command.request_id, command.name)
                } else {
                    BrowserWorkerResponseEnvelope::error(
                        command.request_id,
                        QueryError::new(
                            QueryErrorCode::InvalidRequest,
                            format!(
                                "browser worker does not have an open table named '{}'",
                                command.name
                            ),
                            worker_target(),
                        ),
                    )
                }
            }
        }
    }
}

pub fn worker_target() -> ExecutionTarget {
    preferred_target()
}

pub fn runtime_sku() -> BrowserRuntimeSku {
    BrowserRuntimeSku::Sql
}

pub fn result_transport() -> BrowserResultTransport {
    BrowserResultTransport::ArrowIpc
}

pub fn capabilities() -> BrowserWorkerCapabilities {
    BrowserWorkerCapabilities {
        session_shell: true,
        browser_datafusion: true,
    }
}

pub fn artifact_identity() -> BrowserWorkerArtifactIdentity {
    BrowserWorkerArtifactIdentity {
        package_name: env!("CARGO_PKG_NAME").to_string(),
        package_version: env!("CARGO_PKG_VERSION").to_string(),
        wasm_artifact: BROWSER_ENGINE_WORKER_WASM_ARTIFACT.to_string(),
    }
}

pub fn cold_start_report() -> Result<BrowserWorkerStartupReport, QueryError> {
    let started = wall_clock_start();
    let runtime = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())?;
    let command = sample_command();
    let error_response = sample_error_response();
    let config = runtime.config();

    Ok(BrowserWorkerStartupReport {
        target: worker_target(),
        access_mode: browser_access_mode(config.object_access_mode),
        startup_duration_ms: wall_clock_duration_ms(started),
        command_envelope_bytes: serialized_len(&command)?,
        error_envelope_bytes: serialized_len(&error_response)?,
        request_timeout_ms: config.request_timeout_ms,
        execution_timeout_ms: config.execution_timeout_ms,
        snapshot_preflight_timeout_ms: config.snapshot_preflight_timeout_ms,
        snapshot_preflight_max_concurrency: config.snapshot_preflight_max_concurrency,
    })
}

pub fn memory_baseline_report() -> Result<BrowserWorkerMemoryBaseline, QueryError> {
    let command = sample_command();
    let error_response = sample_error_response();

    Ok(BrowserWorkerMemoryBaseline {
        runtime_session_bytes: size_of::<BrowserRuntimeSession>() as u64,
        query_session_bytes: size_of::<BrowserQuerySession>() as u64,
        runtime_config_bytes: size_of::<BrowserRuntimeConfig>() as u64,
        command_struct_bytes: size_of::<BrowserWorkerCommand>() as u64,
        response_envelope_struct_bytes: size_of::<BrowserWorkerResponseEnvelope>() as u64,
        query_metrics_struct_bytes: size_of::<QueryMetricsSummary>() as u64,
        command_envelope_json_bytes: serialized_len(&command)?,
        error_envelope_json_bytes: serialized_len(&error_response)?,
    })
}

pub fn artifact_report() -> Result<BrowserWorkerArtifactReport, QueryError> {
    Ok(BrowserWorkerArtifactReport {
        runtime_sku: runtime_sku(),
        result_transport: result_transport(),
        capabilities: capabilities(),
        identity: artifact_identity(),
        startup: cold_start_report()?,
        memory: memory_baseline_report()?,
    })
}

fn success_response(
    request_id: impl Into<String>,
    result: BrowserSessionQueryResult,
) -> BrowserWorkerResponseEnvelope {
    BrowserWorkerResponseEnvelope::success(
        request_id,
        result.response,
        ArrowIpcResultEnvelope::new(
            ArrowIpcFormat::Stream,
            result.runtime_result.ipc_bytes.to_vec(),
        ),
    )
}

fn sample_command() -> BrowserWorkerCommand {
    BrowserWorkerCommand::sql(
        "browser-worker-cold-start",
        "axon_fixture",
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

#[allow(dead_code)]
fn sample_snapshot() -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: "https://example.invalid/tables/axon_fixture".to_string(),
        snapshot_version: 1,
        partition_column_types: Default::default(),
        browser_compatibility: Default::default(),
        required_capabilities: Default::default(),
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://example.invalid/part-000.parquet".to_string(),
            size_bytes: 128,
            partition_values: Default::default(),
            stats: None,
        }],
    }
}

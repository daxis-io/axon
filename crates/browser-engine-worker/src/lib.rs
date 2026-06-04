//! Internal browser worker artifact for bundle, startup, and contract tracking.

use std::mem::size_of;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

use browser_sdk::{
    preferred_target, ArrowIpcFormat, ArrowIpcResultEnvelope, BrowserWorkerCommand,
    BrowserWorkerEventContext, BrowserWorkerEventEnvelope, BrowserWorkerLogLevel,
    BrowserWorkerProgressStage, BrowserWorkerResponseEnvelope,
};
use query_contract::{
    BrowserAccessMode, BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget,
    FallbackReason, QueryError, QueryErrorCode, QueryMetricsSummary, QueryRequest,
};
use serde::{Deserialize, Serialize};
use wasm_query_runtime::{BrowserObjectAccessMode, BrowserRuntimeConfig, BrowserRuntimeSession};
use wasm_query_session::{BrowserQueryBudget, BrowserQuerySession, BrowserSessionQueryResult};

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
    #[serde(default)]
    pub datafusion_query_budget: BrowserWorkerQueryBudgetReport,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerQueryBudgetReport {
    pub max_scan_bytes: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
    pub max_rows_returned: Option<u64>,
}

impl BrowserWorkerQueryBudgetReport {
    pub fn from_query_budget(query_budget: BrowserQueryBudget) -> Self {
        Self {
            max_scan_bytes: query_budget.max_scan_bytes,
            max_output_ipc_bytes: query_budget.max_output_ipc_bytes,
            max_batches_in_flight: query_budget.max_batches_in_flight,
            max_rows_returned: query_budget.max_rows_returned,
        }
    }
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
        Self::new_with_event_sink(runtime_config, max_cached_bytes, |_| {})
    }

    pub fn new_with_event_sink<F>(
        runtime_config: BrowserRuntimeConfig,
        max_cached_bytes: u64,
        mut emit_event: F,
    ) -> Result<Self, QueryError>
    where
        F: FnMut(BrowserWorkerEventEnvelope),
    {
        let context = BrowserWorkerEventContext::instantiate();
        emit_event(BrowserWorkerEventEnvelope::progress(
            context.clone(),
            BrowserWorkerProgressStage::Started,
        ));
        emit_event(BrowserWorkerEventEnvelope::log(
            context.clone(),
            BrowserWorkerLogLevel::Info,
            "browser worker instantiating runtime session",
        ));

        match BrowserQuerySession::new(runtime_config, max_cached_bytes) {
            Ok(session) => {
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context,
                    BrowserWorkerProgressStage::Finished,
                ));
                Ok(Self { session })
            }
            Err(error) => {
                emit_error_events(&mut emit_event, context, &error);
                Err(error)
            }
        }
    }

    pub fn new_with_query_budget(
        runtime_config: BrowserRuntimeConfig,
        max_cached_bytes: u64,
        query_budget: BrowserQueryBudget,
    ) -> Result<Self, QueryError> {
        Self::new_with_query_budget_and_event_sink(
            runtime_config,
            max_cached_bytes,
            query_budget,
            |_| {},
        )
    }

    pub fn new_with_query_budget_and_event_sink<F>(
        runtime_config: BrowserRuntimeConfig,
        max_cached_bytes: u64,
        query_budget: BrowserQueryBudget,
        mut emit_event: F,
    ) -> Result<Self, QueryError>
    where
        F: FnMut(BrowserWorkerEventEnvelope),
    {
        let context = BrowserWorkerEventContext::instantiate();
        emit_event(BrowserWorkerEventEnvelope::progress(
            context.clone(),
            BrowserWorkerProgressStage::Started,
        ));
        emit_event(BrowserWorkerEventEnvelope::log(
            context.clone(),
            BrowserWorkerLogLevel::Info,
            "browser worker instantiating runtime session with query budget",
        ));

        match BrowserQuerySession::new_with_query_budget(
            runtime_config,
            max_cached_bytes,
            query_budget,
        ) {
            Ok(session) => {
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context,
                    BrowserWorkerProgressStage::Finished,
                ));
                Ok(Self { session })
            }
            Err(error) => {
                emit_error_events(&mut emit_event, context, &error);
                Err(error)
            }
        }
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
        self.handle_command_streaming_events(command, |_| {}).await
    }

    pub async fn handle_command_streaming_events<F>(
        &mut self,
        command: BrowserWorkerCommand,
        mut emit_event: F,
    ) -> BrowserWorkerResponseEnvelope
    where
        F: FnMut(BrowserWorkerEventEnvelope),
    {
        match command {
            BrowserWorkerCommand::OpenTable(command) => {
                let context = BrowserWorkerEventContext::open(
                    command.request_id.clone(),
                    command.name.clone(),
                );
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Started,
                ));
                emit_event(BrowserWorkerEventEnvelope::log(
                    context.clone(),
                    BrowserWorkerLogLevel::Info,
                    "browser worker opening table descriptor",
                ));
                match self
                    .session
                    .open_table(command.name.clone(), command.snapshot)
                {
                    Ok(()) => {
                        emit_cache_metrics(&mut emit_event, &context, &self.session);
                        emit_event(BrowserWorkerEventEnvelope::progress(
                            context,
                            BrowserWorkerProgressStage::Finished,
                        ));
                        BrowserWorkerResponseEnvelope::opened(command.request_id, command.name)
                    }
                    Err(error) => {
                        emit_error_events(&mut emit_event, context, &error);
                        BrowserWorkerResponseEnvelope::error(command.request_id, error)
                    }
                }
            }
            BrowserWorkerCommand::OpenDeltaTable(command) => {
                let context = BrowserWorkerEventContext::open(
                    command.request_id.clone(),
                    command.name.clone(),
                );
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Started,
                ));
                emit_event(BrowserWorkerEventEnvelope::log(
                    context.clone(),
                    BrowserWorkerLogLevel::Info,
                    "browser worker opening Delta table descriptor",
                ));
                match self
                    .session
                    .open_delta_table(command.name.clone(), command.snapshot)
                    .await
                {
                    Ok(()) => {
                        emit_open_bootstrap_metrics(
                            &mut emit_event,
                            &context,
                            &self.session,
                            &command.name,
                        );
                        emit_cache_metrics(&mut emit_event, &context, &self.session);
                        emit_event(BrowserWorkerEventEnvelope::progress(
                            context,
                            BrowserWorkerProgressStage::Finished,
                        ));
                        BrowserWorkerResponseEnvelope::opened(command.request_id, command.name)
                    }
                    Err(error) => {
                        emit_error_events(&mut emit_event, context, &error);
                        BrowserWorkerResponseEnvelope::error(command.request_id, error)
                    }
                }
            }
            BrowserWorkerCommand::OpenParquetDataset(command) => {
                let context = BrowserWorkerEventContext::open(
                    command.request_id.clone(),
                    command.name.clone(),
                );
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Started,
                ));
                emit_event(BrowserWorkerEventEnvelope::log(
                    context.clone(),
                    BrowserWorkerLogLevel::Info,
                    "browser worker opening Parquet dataset descriptor",
                ));
                match self
                    .session
                    .open_parquet_dataset(command.name.clone(), command.dataset)
                    .await
                {
                    Ok(()) => {
                        emit_open_bootstrap_metrics(
                            &mut emit_event,
                            &context,
                            &self.session,
                            &command.name,
                        );
                        emit_cache_metrics(&mut emit_event, &context, &self.session);
                        emit_event(BrowserWorkerEventEnvelope::progress(
                            context,
                            BrowserWorkerProgressStage::Finished,
                        ));
                        BrowserWorkerResponseEnvelope::opened(command.request_id, command.name)
                    }
                    Err(error) => {
                        emit_error_events(&mut emit_event, context, &error);
                        BrowserWorkerResponseEnvelope::error(command.request_id, error)
                    }
                }
            }
            BrowserWorkerCommand::InspectParquet(command) => {
                let context = BrowserWorkerEventContext::inspect(
                    command.request_id.clone(),
                    command.name.clone(),
                );
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Started,
                ));
                emit_event(BrowserWorkerEventEnvelope::log(
                    context.clone(),
                    BrowserWorkerLogLevel::Info,
                    "browser worker inspecting Parquet file metadata",
                ));
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Executing,
                ));
                match self
                    .session
                    .inspect_parquet(&command.name, &command.path)
                    .await
                {
                    Ok(summary) => {
                        emit_cache_metrics(&mut emit_event, &context, &self.session);
                        emit_event(BrowserWorkerEventEnvelope::progress(
                            context,
                            BrowserWorkerProgressStage::Finished,
                        ));
                        BrowserWorkerResponseEnvelope::parquet_inspection(
                            command.request_id,
                            summary,
                        )
                    }
                    Err(error) => {
                        emit_error_events(&mut emit_event, context, &error);
                        BrowserWorkerResponseEnvelope::error(command.request_id, error)
                    }
                }
            }
            BrowserWorkerCommand::Sql(command) => {
                let context = BrowserWorkerEventContext::query(
                    command.request_id.clone(),
                    command.name.clone(),
                );
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Started,
                ));
                emit_event(BrowserWorkerEventEnvelope::log(
                    context.clone(),
                    BrowserWorkerLogLevel::Info,
                    "browser worker executing SQL query",
                ));
                emit_event(BrowserWorkerEventEnvelope::progress(
                    context.clone(),
                    BrowserWorkerProgressStage::Executing,
                ));
                match self.session.sql(&command.name, &command.request).await {
                    Ok(result) => {
                        emit_event(BrowserWorkerEventEnvelope::range_read_metrics(
                            context.clone(),
                            result.response.metrics,
                        ));
                        if let Some(reason) = result.response.fallback_reason.clone() {
                            emit_event(BrowserWorkerEventEnvelope::fallback(
                                context.clone(),
                                reason,
                            ));
                        }
                        emit_cache_metrics(&mut emit_event, &context, &self.session);
                        emit_event(BrowserWorkerEventEnvelope::progress(
                            context.clone(),
                            BrowserWorkerProgressStage::ArrowIpcReady,
                        ));
                        emit_event(BrowserWorkerEventEnvelope::progress(
                            context,
                            BrowserWorkerProgressStage::Finished,
                        ));
                        success_response(command.request_id, result)
                    }
                    Err(error) => {
                        emit_error_events(&mut emit_event, context, &error);
                        BrowserWorkerResponseEnvelope::error(command.request_id, error)
                    }
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
    BrowserRuntimeSku::Narrow
}

pub fn result_transport() -> BrowserResultTransport {
    BrowserResultTransport::ArrowIpc
}

pub fn capabilities() -> BrowserWorkerCapabilities {
    BrowserWorkerCapabilities {
        session_shell: true,
        browser_datafusion: false,
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
        datafusion_query_budget: BrowserWorkerQueryBudgetReport::from_query_budget(
            BrowserQueryBudget::default(),
        ),
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

fn emit_cache_metrics<F>(
    emit_event: &mut F,
    context: &BrowserWorkerEventContext,
    session: &BrowserQuerySession,
) where
    F: FnMut(BrowserWorkerEventEnvelope),
{
    let session_table_count = u64::try_from(session.table_count()).unwrap_or(u64::MAX);
    emit_event(BrowserWorkerEventEnvelope::cache_metrics(
        context.clone(),
        session.cached_bytes(),
        session_table_count,
        session.max_cached_bytes(),
        None,
    ));
}

fn emit_open_bootstrap_metrics<F>(
    emit_event: &mut F,
    context: &BrowserWorkerEventContext,
    session: &BrowserQuerySession,
    table_name: &str,
) where
    F: FnMut(BrowserWorkerEventEnvelope),
{
    let Some(snapshot) = session
        .table(table_name)
        .and_then(|table| table.bootstrapped_snapshot())
    else {
        return;
    };

    let active_files = snapshot.active_files();
    let files_touched = u64::try_from(active_files.len()).unwrap_or(u64::MAX);
    let row_groups_touched = active_files.iter().fold(0_u64, |total, file| {
        total.saturating_add(file.metadata().row_group_count)
    });
    let rows_emitted = active_files.iter().fold(0_u64, |total, file| {
        total.saturating_add(file.metadata().row_count)
    });

    emit_event(BrowserWorkerEventEnvelope::range_read_metrics(
        context.clone(),
        QueryMetricsSummary {
            bytes_fetched: 0,
            duration_ms: snapshot.snapshot_bootstrap_duration_ms().unwrap_or(0),
            files_touched,
            files_skipped: 0,
            row_groups_touched,
            row_groups_skipped: 0,
            footer_reads: snapshot.footer_reads(),
            rows_emitted,
            snapshot_bootstrap_duration_ms: snapshot.snapshot_bootstrap_duration_ms(),
            access_mode: snapshot.access_mode(),
        },
    ));
}

fn emit_error_events<F>(emit_event: &mut F, context: BrowserWorkerEventContext, error: &QueryError)
where
    F: FnMut(BrowserWorkerEventEnvelope),
{
    if let Some(reason) = error.fallback_reason.clone() {
        emit_event(BrowserWorkerEventEnvelope::fallback(
            context.clone(),
            reason,
        ));
    }
    emit_event(BrowserWorkerEventEnvelope::log(
        context.clone(),
        BrowserWorkerLogLevel::Error,
        error.message.clone(),
    ));
    emit_event(BrowserWorkerEventEnvelope::terminal_error(
        context,
        error.clone(),
    ));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use query_contract::CapabilityReport;
    use wasm_query_runtime::{
        BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserObjectSource,
        BrowserParquetFileMetadata, MaterializedBrowserFile, MaterializedBrowserSnapshot,
    };

    #[test]
    fn open_bootstrap_metrics_event_uses_bootstrapped_snapshot_telemetry() {
        let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("session should construct");
        session
            .cache_table(
                "events",
                materialized_snapshot(),
                Some(bootstrapped_snapshot()),
            )
            .expect("table should cache");
        let context = BrowserWorkerEventContext::open("req-open", "events");
        let mut events = Vec::new();

        emit_open_bootstrap_metrics(
            &mut |event| events.push(event),
            &context,
            &session,
            "events",
        );

        let range_metrics = events
            .iter()
            .find_map(|event| match event {
                BrowserWorkerEventEnvelope::RangeReadMetrics(metrics) => Some(metrics),
                _ => None,
            })
            .expect("open bootstrap should emit range-read metrics");
        assert_eq!(range_metrics.context, context);
        assert_eq!(range_metrics.files_touched, 1);
        assert_eq!(range_metrics.row_groups_touched, 2);
        assert_eq!(range_metrics.footer_reads, Some(1));
        assert_eq!(range_metrics.rows_emitted, 10);
        assert_eq!(range_metrics.snapshot_bootstrap_duration_ms, Some(7));
        assert_eq!(
            range_metrics.access_mode,
            Some(BrowserAccessMode::BrowserSafeHttp)
        );
    }

    #[tokio::test]
    async fn inspect_parquet_command_preserves_request_id_on_missing_table_error() {
        let mut worker = BrowserWorker::new(BrowserRuntimeConfig::default(), u64::MAX)
            .expect("worker should construct");

        let response = worker
            .handle_command(BrowserWorkerCommand::inspect_parquet(
                "req-inspect",
                "missing",
                "part-000.parquet",
            ))
            .await;

        match response {
            BrowserWorkerResponseEnvelope::Error(error) => {
                assert_eq!(error.request_id, "req-inspect");
                assert_eq!(error.error.code, QueryErrorCode::InvalidRequest);
                assert!(error.error.message.contains("missing"));
            }
            _ => panic!("missing inspect target should return an error envelope"),
        }
    }

    fn materialized_snapshot() -> MaterializedBrowserSnapshot {
        let file = MaterializedBrowserFile::new(
            "part-000.parquet",
            128,
            BTreeMap::new(),
            BrowserObjectSource::from_url("https://example.invalid/part-000.parquet")
                .expect("fixture URL should be browser-safe"),
        );
        MaterializedBrowserSnapshot::new("gs://axon-fixtures/events", 7, vec![file])
            .expect("materialized snapshot should construct")
    }

    fn bootstrapped_snapshot() -> BootstrappedBrowserSnapshot {
        let file = BootstrappedBrowserFile::new(
            "part-000.parquet",
            128,
            BTreeMap::new(),
            BrowserParquetFileMetadata {
                object_size_bytes: 128,
                footer_length_bytes: 16,
                row_group_count: 2,
                row_count: 10,
                fields: Vec::new(),
                field_stats: BTreeMap::new(),
            },
        )
        .expect("bootstrapped file should construct");
        BootstrappedBrowserSnapshot::new_with_partition_metadata_and_telemetry(
            "gs://axon-fixtures/events",
            7,
            vec![file],
            BTreeMap::new(),
            CapabilityReport::default(),
            Some(1),
            Some(7),
            Some(BrowserAccessMode::BrowserSafeHttp),
        )
        .expect("bootstrapped snapshot should construct")
    }
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

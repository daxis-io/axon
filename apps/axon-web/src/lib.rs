use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use futures_util::lock::Mutex as AsyncMutex;
use js_sys::{BigInt, Object, Reflect, Uint8Array};
use query_contract::{
    BrowserHttpParquetDatasetDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget,
    QueryError, QueryErrorCode, QueryRequest, QueryResponse, SnapshotResolutionRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;
use wasm_datafusion_session::{
    ArrowIpcPhase, BrowserDataFusionCancellation, BrowserDataFusionCursorCancellation,
    BrowserDataFusionIpcCursor, BrowserDataFusionIpcCursorItem, BrowserDataFusionQueryTerminal,
    BrowserDataFusionSession, IpcCursorMetrics, IpcPreview, IpcStreamLimits, IpcTransportChunk,
    QueryTerminalStatus,
};
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
    DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{ObjectSource, ParquetMetadataCache, ScanTarget};
use wasm_query_runtime::{BrowserObjectAccessMode, BrowserRuntimeConfig};

const DEFAULT_QUERY_SESSION_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_QUERY_PREVIEW_LIMIT: usize = 501;
const IPC_METADATA_VERSION: u32 = 1;
const DEFAULT_IPC_TRANSPORT_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    objects: Vec<ManifestObjectInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestObjectInput {
    relative_path: String,
    url: String,
    #[serde(default)]
    size_bytes: Option<u64>,
    #[serde(default)]
    etag: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParquetPreflightTargetInput {
    path: String,
    url: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    #[serde(default)]
    stats: Option<String>,
}

#[derive(Debug, Serialize)]
struct ParquetPreflightOutput {
    path: String,
    url: String,
    #[serde(serialize_with = "serialize_decimal_string")]
    size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_etag: Option<String>,
    partition_values: BTreeMap<String, Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta_stats: Option<String>,
    #[serde(serialize_with = "serialize_decimal_string")]
    footer_length_bytes: u32,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_group_count: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    fields: Vec<ParquetPreflightField>,
    field_stats: BTreeMap<String, ParquetPreflightFieldStats>,
}

#[derive(Debug, Serialize)]
struct ParquetPreflightField {
    name: String,
    physical_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    logical_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    converted_type: Option<String>,
    repetition: String,
    nullable: bool,
}

#[derive(Debug, Serialize)]
struct ParquetPreflightFieldStats {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal_string"
    )]
    min_i64: Option<i64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal_string"
    )]
    max_i64: Option<i64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal_string"
    )]
    null_count: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SandboxOpenTableOutput {
    cache_metrics: SandboxCacheMetrics,
}

#[derive(Debug, Serialize)]
struct SandboxSqlMetadata {
    metadata_version: u32,
    response: QueryResponse,
    preview: QueryPreviewOutput,
    #[serde(serialize_with = "serialize_decimal_string")]
    arrow_ipc_byte_length: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    preview_string_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    preview_duration_ms: u64,
    cache_metrics: SandboxCacheMetrics,
}

#[derive(Debug, Serialize)]
struct SandboxSqlTerminalMetadata {
    metadata_version: u32,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<QueryError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<QueryResponse>,
    preview: QueryPreviewOutput,
    #[serde(serialize_with = "serialize_decimal_string")]
    arrow_ipc_byte_length: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    preview_string_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    preview_duration_ms: u64,
    cache_metrics: SandboxCacheMetrics,
    cursor_metrics: SandboxCursorMetrics,
}

#[derive(Debug, Serialize)]
struct SandboxCursorMetrics {
    #[serde(serialize_with = "serialize_decimal_string")]
    peak_input_batch_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    peak_pending_encoded_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    peak_pending_encoded_capacity: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    peak_transport_chunk_bytes: u64,
}

#[derive(Debug, Serialize)]
struct QueryPreviewOutput {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    preview_row_limit: usize,
    truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
struct SandboxCacheMetrics {
    #[serde(serialize_with = "serialize_decimal_string")]
    session_cached_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    session_table_count: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    max_session_cached_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SandboxWorkerArtifactReport {
    runtime_sku: &'static str,
    default_worker_sku: bool,
    result_transport: &'static str,
    capabilities: SandboxWorkerArtifactCapabilities,
    identity: SandboxWorkerArtifactIdentity,
    startup: SandboxWorkerStartupReport,
    memory: SandboxWorkerMemoryReport,
}

#[derive(Debug, Serialize)]
struct SandboxWorkerArtifactCapabilities {
    session_shell: bool,
    browser_datafusion: bool,
    descriptor_backed_delta: bool,
    browser_safe_http: bool,
    cancellation: bool,
    arrow_ipc_preview: bool,
}

#[derive(Debug, Serialize)]
struct SandboxWorkerArtifactIdentity {
    package_name: &'static str,
    package_version: &'static str,
    wasm_artifact: &'static str,
    worker_entrypoint: &'static str,
}

#[derive(Debug, Serialize)]
struct SandboxWorkerStartupReport {
    target: &'static str,
    access_mode: &'static str,
    #[serde(serialize_with = "serialize_decimal_string")]
    max_session_cached_bytes: u64,
    default_preview_limit: usize,
    datafusion_query_budget: SandboxWorkerQueryBudgetReport,
}

#[derive(Debug, Serialize)]
struct SandboxWorkerQueryBudgetReport {
    max_scan_bytes: Option<u64>,
    max_output_ipc_bytes: Option<u64>,
    max_batches_in_flight: Option<usize>,
    max_rows_returned: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SandboxWorkerMemoryReport {
    #[serde(serialize_with = "serialize_decimal_string")]
    datafusion_session_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    command_envelope_json_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    max_session_cached_bytes: u64,
}

fn serialize_decimal_string<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_optional_decimal_string<T, S>(
    value: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

#[wasm_bindgen]
pub struct SandboxQueryCancellation {
    cancellation: BrowserDataFusionCancellation,
}

#[wasm_bindgen]
impl SandboxQueryCancellation {
    pub fn cancel(&self) {
        self.cancellation.cancel_running_queries();
    }
}

#[wasm_bindgen]
pub struct SandboxSqlStream {
    cursor: Rc<AsyncMutex<Option<BrowserDataFusionIpcCursor>>>,
    cancellation: BrowserDataFusionCursorCancellation,
    pull_active: Rc<Cell<bool>>,
    cache_metrics: SandboxCacheMetrics,
}

#[wasm_bindgen]
impl SandboxSqlStream {
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if self.pull_active.replace(true) {
            return Err(JsValue::from_str(
                "Arrow IPC cursor fault: only one next() pull may be active",
            ));
        }
        let _pull = ActivePullGuard {
            active: Rc::clone(&self.pull_active),
        };
        let mut cursor = self.cursor.lock().await;
        let Some(cursor) = cursor.as_mut() else {
            return Ok(JsValue::NULL);
        };
        let item = cursor
            .next()
            .await
            .map_err(|error| JsValue::from_str(&format!("Arrow IPC cursor fault: {error}")))?;
        let Some(item) = item else {
            return Ok(JsValue::NULL);
        };

        match item {
            BrowserDataFusionIpcCursorItem::Chunk(chunk) => ipc_chunk_js_value(chunk),
            BrowserDataFusionIpcCursorItem::Terminal(terminal) => {
                ipc_terminal_js_value(terminal, self.cache_metrics.clone())
            }
        }
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn cancel_for_deadline(&self) {
        self.cancellation.cancel_for_deadline();
    }

    pub async fn close(&self) {
        self.cancellation.cancel();
        let mut cursor = self.cursor.lock().await;
        if let Some(cursor) = cursor.as_mut() {
            cursor.close();
        }
        *cursor = None;
    }
}

struct ActivePullGuard {
    active: Rc<Cell<bool>>,
}

impl Drop for ActivePullGuard {
    fn drop(&mut self) {
        self.active.set(false);
    }
}

#[wasm_bindgen]
pub struct SandboxQuerySession {
    session: BrowserDataFusionSession,
    cancellation: BrowserDataFusionCancellation,
}

#[wasm_bindgen]
impl SandboxQuerySession {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<SandboxQuerySession, JsValue> {
        let session = BrowserDataFusionSession::new(
            default_sandbox_runtime_config(),
            DEFAULT_QUERY_SESSION_CACHE_BYTES,
        )
        .map_err(query_error_to_js_value)?;
        let cancellation = session.cancellation_handle();

        Ok(Self {
            session,
            cancellation,
        })
    }

    pub fn cancellation(&self) -> SandboxQueryCancellation {
        SandboxQueryCancellation {
            cancellation: self.cancellation.clone(),
        }
    }

    pub async fn open_delta_table(
        &mut self,
        name: String,
        snapshot_json: String,
    ) -> Result<String, JsValue> {
        let snapshot = serde_json::from_str::<BrowserHttpSnapshotDescriptor>(&snapshot_json)
            .map_err(|error| JsValue::from_str(&format!("invalid snapshot descriptor: {error}")))?;
        self.session
            .open_delta_table(name.clone(), snapshot)
            .await
            .map_err(query_error_to_js_value)?;

        serde_json::to_string(&SandboxOpenTableOutput {
            cache_metrics: cache_metrics(&self.session),
        })
        .map_err(|error| {
            JsValue::from_str(&format!(
                "open table metadata serialization failed: {error}"
            ))
        })
    }

    pub async fn open_parquet_dataset(
        &mut self,
        name: String,
        dataset_json: String,
    ) -> Result<String, JsValue> {
        let dataset = serde_json::from_str::<BrowserHttpParquetDatasetDescriptor>(&dataset_json)
            .map_err(|error| JsValue::from_str(&format!("invalid Parquet descriptor: {error}")))?;
        self.session
            .open_parquet_dataset(name.clone(), dataset)
            .await
            .map_err(query_error_to_js_value)?;

        serde_json::to_string(&SandboxOpenTableOutput {
            cache_metrics: cache_metrics(&self.session),
        })
        .map_err(|error| {
            JsValue::from_str(&format!(
                "open Parquet dataset metadata serialization failed: {error}"
            ))
        })
    }

    pub async fn inspect_parquet(&mut self, name: String, path: String) -> Result<String, JsValue> {
        let summary = self
            .session
            .inspect_parquet(&name, &path)
            .await
            .map_err(query_error_to_js_value)?;
        serde_json::to_string(&summary).map_err(|error| {
            JsValue::from_str(&format!("Parquet inspection serialization failed: {error}"))
        })
    }

    pub fn dispose_table(&mut self, name: String) -> Result<String, JsValue> {
        self.session.dispose_table(&name);
        Ok(name)
    }

    pub async fn start_sql_stream(
        &mut self,
        name: String,
        request_json: String,
        preview_limit: u32,
    ) -> Result<SandboxSqlStream, JsValue> {
        let request = serde_json::from_str::<QueryRequest>(&request_json)
            .map_err(|error| JsValue::from_str(&format!("invalid query request: {error}")))?;
        let (cursor, cache_metrics) = self
            .start_browser_sql_cursor(&name, &request, preview_limit)
            .await
            .map_err(query_error_to_js_value)?;
        let cancellation = cursor.cancel_handle();
        Ok(SandboxSqlStream {
            cursor: Rc::new(AsyncMutex::new(Some(cursor))),
            cancellation,
            pull_active: Rc::new(Cell::new(false)),
            cache_metrics,
        })
    }

    pub async fn sql(
        &mut self,
        name: String,
        request_json: String,
        preview_limit: u32,
    ) -> Result<JsValue, JsValue> {
        let request = serde_json::from_str::<QueryRequest>(&request_json)
            .map_err(|error| JsValue::from_str(&format!("invalid query request: {error}")))?;
        let (mut cursor, cache_metrics) = self
            .start_browser_sql_cursor(&name, &request, preview_limit)
            .await
            .map_err(query_error_to_js_value)?;
        let mut arrow_ipc_bytes = Vec::new();

        while let Some(item) = cursor
            .next()
            .await
            .map_err(|error| JsValue::from_str(&format!("Arrow IPC cursor fault: {error}")))?
        {
            match item {
                BrowserDataFusionIpcCursorItem::Chunk(chunk) => {
                    arrow_ipc_bytes
                        .len()
                        .checked_add(chunk.bytes.len())
                        .ok_or_else(|| JsValue::from_str("Arrow IPC byte length overflow"))?;
                    arrow_ipc_bytes
                        .try_reserve(chunk.bytes.len())
                        .map_err(|_| JsValue::from_str("Arrow IPC byte reservation failed"))?;
                    arrow_ipc_bytes.extend_from_slice(&chunk.bytes);
                }
                BrowserDataFusionIpcCursorItem::Terminal(terminal) => {
                    if terminal.status != QueryTerminalStatus::Succeeded {
                        return Err(query_error_to_js_value(terminal.error.unwrap_or_else(
                            || {
                                preview_runtime_error(
                                    "Arrow IPC cursor stopped without a query error",
                                )
                            },
                        )));
                    }
                    let metadata = sandbox_sql_metadata(terminal, cache_metrics)?;
                    let expected_length = usize::try_from(metadata.arrow_ipc_byte_length)
                        .map_err(|_| JsValue::from_str("Arrow IPC byte length overflow"))?;
                    if arrow_ipc_bytes.len() != expected_length {
                        return Err(JsValue::from_str(
                            "Arrow IPC cursor byte count did not match terminal metadata",
                        ));
                    }
                    return sql_bridge_value(&metadata, &arrow_ipc_bytes);
                }
            }
        }

        Err(JsValue::from_str(
            "Arrow IPC cursor closed before a terminal outcome",
        ))
    }
}

impl SandboxQuerySession {
    async fn start_browser_sql_cursor(
        &mut self,
        name: &str,
        request: &QueryRequest,
        preview_limit: u32,
    ) -> Result<(BrowserDataFusionIpcCursor, SandboxCacheMetrics), QueryError> {
        let preview_rows = usize::try_from(preview_limit)
            .unwrap_or(DEFAULT_QUERY_PREVIEW_LIMIT)
            .min(DEFAULT_QUERY_PREVIEW_LIMIT);
        let max_total_encoded_bytes = request
            .options
            .runtime_limits
            .and_then(|limits| limits.max_arrow_ipc_bytes);
        let max_pending_encoded_batch_bytes = max_total_encoded_bytes
            .and_then(|max| usize::try_from(max).ok())
            .map(|max| max.min(DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES))
            .unwrap_or(DEFAULT_MAX_PENDING_ENCODED_BATCH_BYTES);
        let cursor = self
            .session
            .start_arrow_ipc_cursor(
                name,
                request,
                IpcStreamLimits {
                    max_transport_chunk_bytes: DEFAULT_IPC_TRANSPORT_CHUNK_BYTES,
                    max_pending_encoded_batch_bytes,
                    max_total_encoded_bytes,
                    preview_rows,
                    max_preview_string_bytes: request
                        .options
                        .runtime_limits
                        .and_then(|limits| limits.max_preview_string_bytes),
                },
            )
            .await?;
        Ok((cursor, cache_metrics(&self.session)))
    }
}

#[wasm_bindgen]
pub fn sandbox_query_worker_artifact_report() -> Result<String, JsValue> {
    let report = build_sandbox_query_worker_artifact_report().map_err(query_error_to_js_value)?;
    serde_json::to_string(&report).map_err(|error| {
        JsValue::from_str(&format!(
            "sandbox query worker artifact report serialization failed: {error}"
        ))
    })
}

#[wasm_bindgen]
pub async fn resolve_delta_snapshot_from_manifest(
    manifest_json: String,
    table_uri: String,
) -> Result<String, JsValue> {
    let manifest_input = serde_json::from_str::<ManifestInput>(&manifest_json)
        .map_err(|error| JsValue::from_str(&format!("invalid Delta log manifest: {error}")))?;
    let objects = manifest_input
        .objects
        .into_iter()
        .map(|object| {
            BrowserDeltaLogObject::new(object.relative_path, object.url)
                .with_metadata(object.size_bytes, object.etag)
        })
        .collect::<Vec<_>>();
    let manifest = BrowserDeltaLogManifest::new(table_uri.clone(), objects)
        .map_err(query_error_to_js_value)?;
    let resolver = SnapshotResolver::new(
        BrowserHttpDeltaLogStorageHandler::new(manifest),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri,
            snapshot_version: None,
        })
        .await
        .map_err(query_error_to_js_value)?;

    serde_json::to_string(&snapshot)
        .map_err(|error| JsValue::from_str(&format!("snapshot serialization failed: {error}")))
}

#[wasm_bindgen]
pub async fn preflight_parquet_metadata_for_targets(
    targets_json: String,
) -> Result<String, JsValue> {
    let targets = serde_json::from_str::<Vec<ParquetPreflightTargetInput>>(&targets_json).map_err(
        |error| JsValue::from_str(&format!("invalid Parquet preflight targets: {error}")),
    )?;
    let reader = HttpRangeReader::new();
    let metadata_cache = ParquetMetadataCache::default();
    let mut outputs = Vec::with_capacity(targets.len());

    for target in targets {
        let scan_target = ScanTarget {
            object_source: ObjectSource::new(target.url.clone()),
            object_etag: None,
            path: target.path.clone(),
            size_bytes: target.size_bytes,
            partition_values: target.partition_values.clone(),
        };
        let footer = wasm_parquet_engine::read_parquet_footer_for_target_with_cache(
            &reader,
            &scan_target,
            None,
            Some(&metadata_cache),
            None,
        )
        .await
        .map_err(query_error_to_js_value)?;
        let metadata = wasm_parquet_engine::parse_parquet_metadata(&scan_target, &footer)
            .map_err(query_error_to_js_value)?;
        outputs.push(ParquetPreflightOutput {
            path: target.path,
            url: target.url,
            size_bytes: metadata.object_size_bytes,
            object_etag: footer.object_etag().map(str::to_string),
            partition_values: target.partition_values,
            delta_stats: target.stats,
            footer_length_bytes: metadata.footer_length_bytes,
            row_group_count: metadata.row_group_count,
            row_count: metadata.row_count,
            fields: metadata
                .fields
                .into_iter()
                .map(parquet_preflight_field)
                .collect(),
            field_stats: metadata
                .field_stats
                .into_iter()
                .map(|(name, stats)| {
                    (
                        name,
                        ParquetPreflightFieldStats {
                            min_i64: stats.min_i64,
                            max_i64: stats.max_i64,
                            null_count: stats.null_count,
                        },
                    )
                })
                .collect(),
        });
    }

    serde_json::to_string(&outputs).map_err(|error| {
        JsValue::from_str(&format!("Parquet preflight serialization failed: {error}"))
    })
}

fn default_sandbox_runtime_config() -> BrowserRuntimeConfig {
    BrowserRuntimeConfig {
        object_access_mode: BrowserObjectAccessMode::BrowserSafeHttp,
        allow_cloud_credentials: false,
        ..BrowserRuntimeConfig::default()
    }
}

fn build_sandbox_query_worker_artifact_report() -> Result<SandboxWorkerArtifactReport, QueryError> {
    let session = BrowserDataFusionSession::new(
        default_sandbox_runtime_config(),
        DEFAULT_QUERY_SESSION_CACHE_BYTES,
    )?;
    let query_budget = session.datafusion_query_budget();

    Ok(SandboxWorkerArtifactReport {
        runtime_sku: "browser_datafusion",
        default_worker_sku: true,
        result_transport: "arrow_ipc",
        capabilities: SandboxWorkerArtifactCapabilities {
            session_shell: true,
            browser_datafusion: true,
            descriptor_backed_delta: true,
            browser_safe_http: true,
            cancellation: true,
            arrow_ipc_preview: true,
        },
        identity: SandboxWorkerArtifactIdentity {
            package_name: "axon-web-wasm",
            package_version: env!("CARGO_PKG_VERSION"),
            wasm_artifact: "axon_web_wasm_bg.wasm",
            worker_entrypoint: "apps/axon-web/src/sandbox-query-worker.ts",
        },
        startup: SandboxWorkerStartupReport {
            target: "browser_wasm",
            access_mode: "browser_safe_http",
            max_session_cached_bytes: DEFAULT_QUERY_SESSION_CACHE_BYTES,
            default_preview_limit: DEFAULT_QUERY_PREVIEW_LIMIT,
            datafusion_query_budget: SandboxWorkerQueryBudgetReport {
                max_scan_bytes: query_budget.max_scan_bytes,
                max_output_ipc_bytes: query_budget.max_output_ipc_bytes,
                max_batches_in_flight: query_budget.max_batches_in_flight,
                max_rows_returned: query_budget.max_rows_returned,
            },
        },
        memory: SandboxWorkerMemoryReport {
            datafusion_session_bytes: wasm_datafusion_session::memory_baseline_bytes(),
            command_envelope_json_bytes: sample_command_envelope_json_bytes(),
            max_session_cached_bytes: DEFAULT_QUERY_SESSION_CACHE_BYTES,
        },
    })
}

fn sample_command_envelope_json_bytes() -> u64 {
    u64::try_from(
        json!({
            "sql": {
                "request_id": "daxis-worker-artifact-report",
                "name": "orders",
                "query": {
                    "table_uri": "daxis://catalog/main/sales/orders",
                    "sql": "SELECT COUNT(*) AS rows FROM orders",
                    "preferred_target": "browser_wasm"
                },
                "output": "arrow_ipc_stream"
            }
        })
        .to_string()
        .len(),
    )
    .unwrap_or(u64::MAX)
}

fn cache_metrics(session: &BrowserDataFusionSession) -> SandboxCacheMetrics {
    SandboxCacheMetrics {
        session_cached_bytes: session.cached_bytes(),
        session_table_count: u64::try_from(session.table_count()).unwrap_or(u64::MAX),
        max_session_cached_bytes: session.max_cached_bytes(),
    }
}

fn sql_bridge_value(metadata: &SandboxSqlMetadata, bytes: &[u8]) -> Result<JsValue, JsValue> {
    let metadata_json = serde_json::to_string(metadata).map_err(|error| {
        JsValue::from_str(&format!(
            "SQL result metadata serialization failed: {error}"
        ))
    })?;
    let object = Object::new();
    Reflect::set(
        &object,
        &JsValue::from_str("metadata_json"),
        &JsValue::from_str(&metadata_json),
    )?;
    let arrow_ipc_bytes = Uint8Array::from(bytes);
    Reflect::set(
        &object,
        &JsValue::from_str("arrow_ipc_bytes"),
        arrow_ipc_bytes.as_ref(),
    )?;

    Ok(object.into())
}

fn sandbox_sql_metadata(
    terminal: BrowserDataFusionQueryTerminal,
    cache_metrics: SandboxCacheMetrics,
) -> Result<SandboxSqlMetadata, JsValue> {
    let preview = query_preview_output(terminal.preview);
    let preview_string_bytes = preview_string_bytes(&preview).map_err(query_error_to_js_value)?;
    let mut response = terminal.response.ok_or_else(|| {
        JsValue::from_str("Arrow IPC cursor succeeded without query response metadata")
    })?;
    response.metrics.arrow_ipc_bytes = Some(terminal.encoded_bytes);
    response.metrics.preview_rows = Some(u64::try_from(preview.rows.len()).unwrap_or(u64::MAX));
    response.metrics.preview_string_bytes = Some(preview_string_bytes);
    response.metrics.preview_duration_ms = Some(0);
    Ok(SandboxSqlMetadata {
        metadata_version: IPC_METADATA_VERSION,
        response,
        row_count: terminal.row_count,
        arrow_ipc_byte_length: terminal.encoded_bytes,
        preview,
        preview_string_bytes,
        preview_duration_ms: 0,
        cache_metrics,
    })
}

fn ipc_chunk_js_value(chunk: IpcTransportChunk) -> Result<JsValue, JsValue> {
    let bytes = Uint8Array::from(chunk.bytes.as_ref());
    if bytes.byte_offset() != 0 || bytes.byte_length() != bytes.buffer().byte_length() {
        return Err(JsValue::from_str(
            "Arrow IPC cursor fault: transport buffer was not exact-sized",
        ));
    }
    let value = Object::new();
    Reflect::set(
        &value,
        &JsValue::from_str("sequence"),
        BigInt::from(chunk.sequence).as_ref(),
    )?;
    Reflect::set(
        &value,
        &JsValue::from_str("phase"),
        &JsValue::from_str(match chunk.phase {
            ArrowIpcPhase::Schema => "schema",
            ArrowIpcPhase::Data => "data",
            ArrowIpcPhase::EndOfStream => "end_of_stream",
        }),
    )?;
    let logical_batch_sequence = chunk
        .logical_batch_sequence
        .map(BigInt::from)
        .map(JsValue::from)
        .unwrap_or(JsValue::NULL);
    Reflect::set(
        &value,
        &JsValue::from_str("logical_batch_sequence"),
        &logical_batch_sequence,
    )?;
    Reflect::set(
        &value,
        &JsValue::from_str("fragment_index"),
        BigInt::from(chunk.fragment_index).as_ref(),
    )?;
    Reflect::set(
        &value,
        &JsValue::from_str("end_of_logical_batch"),
        &JsValue::from_bool(chunk.end_of_logical_batch),
    )?;
    Reflect::set(
        &value,
        &JsValue::from_str("rows_completed"),
        BigInt::from(chunk.rows_completed).as_ref(),
    )?;
    Reflect::set(
        &value,
        &JsValue::from_str("byte_length"),
        BigInt::from(u64::from(bytes.byte_length())).as_ref(),
    )?;
    Reflect::set(&value, &JsValue::from_str("bytes"), bytes.as_ref())?;

    let envelope = Object::new();
    Reflect::set(&envelope, &JsValue::from_str("chunk"), value.as_ref())?;
    Ok(envelope.into())
}

fn ipc_terminal_js_value(
    terminal: BrowserDataFusionQueryTerminal,
    cache_metrics: SandboxCacheMetrics,
) -> Result<JsValue, JsValue> {
    let preview_string_bytes = terminal.preview.string_bytes;
    let preview = query_preview_output(terminal.preview);
    let cursor_metrics = sandbox_cursor_metrics(&terminal.cursor_metrics);
    let mut response = terminal.response;
    if let Some(response) = response.as_mut() {
        response.metrics.arrow_ipc_bytes = Some(terminal.encoded_bytes);
        response.metrics.preview_rows = Some(u64::try_from(preview.rows.len()).unwrap_or(u64::MAX));
        response.metrics.preview_string_bytes = Some(preview_string_bytes);
        response.metrics.preview_duration_ms = Some(0);
    }
    let metadata = SandboxSqlTerminalMetadata {
        metadata_version: IPC_METADATA_VERSION,
        status: terminal_status_name(terminal.status),
        error: terminal.error,
        response,
        preview,
        arrow_ipc_byte_length: terminal.encoded_bytes,
        row_count: terminal.row_count,
        preview_string_bytes,
        preview_duration_ms: 0,
        cache_metrics,
        cursor_metrics,
    };
    let metadata_json = serde_json::to_string(&metadata).map_err(|error| {
        JsValue::from_str(&format!(
            "Arrow IPC terminal metadata serialization failed: {error}"
        ))
    })?;
    let value = Object::new();
    Reflect::set(
        &value,
        &JsValue::from_str("metadata_json"),
        &JsValue::from_str(&metadata_json),
    )?;
    let envelope = Object::new();
    Reflect::set(&envelope, &JsValue::from_str("terminal"), value.as_ref())?;
    Ok(envelope.into())
}

fn query_preview_output(preview: IpcPreview) -> QueryPreviewOutput {
    QueryPreviewOutput {
        columns: preview.columns,
        rows: preview.rows,
        row_count: preview.row_count,
        preview_row_limit: preview.preview_row_limit,
        truncated: preview.truncated,
    }
}

fn sandbox_cursor_metrics(metrics: &IpcCursorMetrics) -> SandboxCursorMetrics {
    SandboxCursorMetrics {
        peak_input_batch_bytes: metrics.peak_input_batch_bytes,
        peak_pending_encoded_bytes: metrics.peak_pending_encoded_bytes,
        peak_pending_encoded_capacity: metrics.peak_pending_encoded_capacity,
        peak_transport_chunk_bytes: metrics.peak_transport_chunk_bytes,
    }
}

fn terminal_status_name(status: QueryTerminalStatus) -> &'static str {
    match status {
        QueryTerminalStatus::Succeeded => "succeeded",
        QueryTerminalStatus::Cancelled => "cancelled",
        QueryTerminalStatus::DeadlineExceeded => "deadline_exceeded",
        QueryTerminalStatus::Failed => "failed",
    }
}

fn preview_string_bytes(preview: &QueryPreviewOutput) -> Result<u64, QueryError> {
    preview.rows.iter().try_fold(0_u64, |total, row| {
        row.iter().try_fold(total, |row_total, value| {
            let bytes = value.as_str().map(str::len).unwrap_or(0);
            row_total
                .checked_add(u64::try_from(bytes).unwrap_or(u64::MAX))
                .ok_or_else(|| preview_runtime_error("preview string byte totals overflowed u64"))
        })
    })
}

fn preview_runtime_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        message,
        ExecutionTarget::BrowserWasm,
    )
}

fn parquet_preflight_field(
    field: wasm_parquet_engine::ParquetColumnField,
) -> ParquetPreflightField {
    ParquetPreflightField {
        name: field.name,
        physical_type: format!("{:?}", field.physical_type),
        logical_type: field.logical_type.map(|value| format!("{value:?}")),
        converted_type: field.converted_type.map(|value| format!("{value:?}")),
        repetition: format!("{:?}", field.repetition),
        nullable: field.nullable,
    }
}

fn query_error_to_js_value(error: QueryError) -> JsValue {
    let fallback = format!("{:?}: {}", error.code, error.message);
    JsValue::from_str(&serde_json::to_string(&error).unwrap_or(fallback))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn parquet_preflight_output_serializes_large_numeric_fields_as_decimal_strings() {
        let output = ParquetPreflightOutput {
            path: "part-000.parquet".to_string(),
            url: "https://example.com/part-000.parquet".to_string(),
            size_bytes: 9_007_199_254_740_993,
            partition_values: BTreeMap::new(),
            delta_stats: None,
            footer_length_bytes: u32::MAX,
            row_group_count: 9_007_199_254_740_994,
            row_count: 9_007_199_254_740_995,
            fields: Vec::new(),
            field_stats: BTreeMap::from([(
                "id".to_string(),
                ParquetPreflightFieldStats {
                    min_i64: Some(9_007_199_254_740_996),
                    max_i64: Some(9_007_199_254_740_997),
                    null_count: Some(9_007_199_254_740_998),
                },
            )]),
        };

        let serialized = serde_json::to_value(output).expect("preflight output should serialize");

        assert_eq!(serialized["size_bytes"], json!("9007199254740993"));
        assert_eq!(serialized["footer_length_bytes"], json!("4294967295"));
        assert_eq!(serialized["row_group_count"], json!("9007199254740994"));
        assert_eq!(serialized["row_count"], json!("9007199254740995"));
        assert_eq!(
            serialized["field_stats"]["id"],
            json!({
                "min_i64": "9007199254740996",
                "max_i64": "9007199254740997",
                "null_count": "9007199254740998"
            })
        );
    }

    #[test]
    fn sandbox_query_worker_artifact_report_names_datafusion_default() {
        let report = sandbox_query_worker_artifact_report()
            .expect("sandbox query worker artifact report should serialize");
        let report: Value =
            serde_json::from_str(&report).expect("artifact report should parse as JSON");

        assert_eq!(report["runtime_sku"], "browser_datafusion");
        assert_eq!(report["default_worker_sku"], true);
        assert_eq!(report["result_transport"], "arrow_ipc");
        assert_eq!(report["capabilities"]["browser_datafusion"], true);
        assert_eq!(report["capabilities"]["session_shell"], true);
        assert_eq!(report["identity"]["package_name"], "axon-web-wasm");
        assert_eq!(
            report["identity"]["package_version"],
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(report["identity"]["wasm_artifact"], "axon_web_wasm_bg.wasm");
        assert_eq!(
            report["identity"]["worker_entrypoint"],
            "apps/axon-web/src/sandbox-query-worker.ts"
        );
        assert_eq!(report["startup"]["target"], "browser_wasm");
        assert_eq!(report["startup"]["access_mode"], "browser_safe_http");
        assert_eq!(
            report["startup"]["max_session_cached_bytes"],
            json!("67108864")
        );
        assert!(
            report["memory"]["datafusion_session_bytes"]
                .as_str()
                .expect("session bytes should be a decimal string")
                .parse::<u64>()
                .expect("session bytes should parse")
                > 0
        );
    }

    #[test]
    fn datafusion_worker_artifact_example_matches_generated_report() {
        let generated = sandbox_query_worker_artifact_report()
            .expect("sandbox query worker artifact report should serialize");
        let example = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../docs/program/browser-lakehouse-release-handoff-examples")
                .join("browser-worker-artifact-report.datafusion.json"),
        )
        .expect("DataFusion worker artifact example should exist");

        let generated: Value =
            serde_json::from_str(&generated).expect("generated report should parse");
        let example: Value = serde_json::from_str(&example).expect("example report should parse");

        assert_eq!(example, generated);
    }
}

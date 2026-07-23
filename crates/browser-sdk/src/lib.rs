//! Public browser SDK contracts for worker-hosted query execution.

use bytes::Bytes;
use query_contract::{
    BrowserAccessMode, BrowserHttpParquetDatasetDescriptor, BrowserHttpSnapshotDescriptor,
    ExecutionTarget, FallbackReason, ParquetInspectionSummary, QueryError, QueryMetricsSummary,
    QueryRequest, QueryResponse,
};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

pub const OWNER: &str = "Web platform team";
pub const RESPONSIBILITY: &str =
    "Public browser-facing SDK surface for worker-hosted query execution.";

pub const ARROW_IPC_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
pub const ARROW_IPC_FILE_CONTENT_TYPE: &str = "application/vnd.apache.arrow.file";

pub fn preferred_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerOpenTableCommand {
    pub request_id: String,
    pub name: String,
    pub snapshot: BrowserHttpSnapshotDescriptor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerOpenDeltaTableCommand {
    pub request_id: String,
    pub name: String,
    pub snapshot: BrowserHttpSnapshotDescriptor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerOpenParquetDatasetCommand {
    pub request_id: String,
    pub name: String,
    pub dataset: BrowserHttpParquetDatasetDescriptor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerInspectParquetCommand {
    pub request_id: String,
    pub name: String,
    pub path: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerSqlOutput {
    ArrowIpcStream,
}

impl Default for BrowserWorkerSqlOutput {
    fn default() -> Self {
        Self::ArrowIpcStream
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerSqlDelivery {
    #[default]
    SingleBuffer,
    ChunkedBuffers,
}

const fn is_single_buffer_sql_delivery(delivery: &BrowserWorkerSqlDelivery) -> bool {
    matches!(delivery, BrowserWorkerSqlDelivery::SingleBuffer)
}

const fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerSqlCommand {
    pub request_id: String,
    pub name: String,
    #[serde(rename = "query", alias = "request")]
    pub request: QueryRequest,
    #[serde(default)]
    pub output: BrowserWorkerSqlOutput,
    #[serde(default, skip_serializing_if = "is_single_buffer_sql_delivery")]
    pub delivery: BrowserWorkerSqlDelivery,
    #[serde(default, skip_serializing_if = "is_false")]
    pub browser_safe_defaults: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerDisposeCommand {
    pub request_id: String,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerCommand {
    OpenTable(BrowserWorkerOpenTableCommand),
    OpenDeltaTable(BrowserWorkerOpenDeltaTableCommand),
    OpenParquetDataset(BrowserWorkerOpenParquetDatasetCommand),
    InspectParquet(BrowserWorkerInspectParquetCommand),
    Sql(BrowserWorkerSqlCommand),
    Dispose(BrowserWorkerDisposeCommand),
}

impl BrowserWorkerCommand {
    pub fn open_table(
        request_id: impl Into<String>,
        name: impl Into<String>,
        snapshot: BrowserHttpSnapshotDescriptor,
    ) -> Self {
        Self::OpenTable(BrowserWorkerOpenTableCommand {
            request_id: request_id.into(),
            name: name.into(),
            snapshot,
        })
    }

    pub fn open_delta_table(
        request_id: impl Into<String>,
        name: impl Into<String>,
        snapshot: BrowserHttpSnapshotDescriptor,
    ) -> Self {
        Self::OpenDeltaTable(BrowserWorkerOpenDeltaTableCommand {
            request_id: request_id.into(),
            name: name.into(),
            snapshot,
        })
    }

    pub fn open_parquet_dataset(
        request_id: impl Into<String>,
        name: impl Into<String>,
        dataset: BrowserHttpParquetDatasetDescriptor,
    ) -> Self {
        Self::OpenParquetDataset(BrowserWorkerOpenParquetDatasetCommand {
            request_id: request_id.into(),
            name: name.into(),
            dataset,
        })
    }

    pub fn sql(
        request_id: impl Into<String>,
        name: impl Into<String>,
        request: QueryRequest,
    ) -> Self {
        Self::Sql(BrowserWorkerSqlCommand {
            request_id: request_id.into(),
            name: name.into(),
            request,
            output: BrowserWorkerSqlOutput::ArrowIpcStream,
            delivery: BrowserWorkerSqlDelivery::SingleBuffer,
            browser_safe_defaults: false,
        })
    }

    pub fn inspect_parquet(
        request_id: impl Into<String>,
        name: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self::InspectParquet(BrowserWorkerInspectParquetCommand {
            request_id: request_id.into(),
            name: name.into(),
            path: path.into(),
        })
    }

    pub fn dispose(request_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Dispose(BrowserWorkerDisposeCommand {
            request_id: request_id.into(),
            name: name.into(),
        })
    }

    pub fn request_id(&self) -> &str {
        match self {
            Self::OpenTable(command) => &command.request_id,
            Self::OpenDeltaTable(command) => &command.request_id,
            Self::OpenParquetDataset(command) => &command.request_id,
            Self::InspectParquet(command) => &command.request_id,
            Self::Sql(command) => &command.request_id,
            Self::Dispose(command) => &command.request_id,
        }
    }

    pub fn table_name(&self) -> &str {
        match self {
            Self::OpenTable(command) => &command.name,
            Self::OpenDeltaTable(command) => &command.name,
            Self::OpenParquetDataset(command) => &command.name,
            Self::InspectParquet(command) => &command.name,
            Self::Sql(command) => &command.name,
            Self::Dispose(command) => &command.name,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerEventContext {
    pub phase: BrowserWorkerEventPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
}

impl BrowserWorkerEventContext {
    pub fn instantiate() -> Self {
        Self {
            phase: BrowserWorkerEventPhase::Instantiate,
            request_id: None,
            query_id: None,
            table_name: None,
        }
    }

    pub fn open(request_id: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            phase: BrowserWorkerEventPhase::Open,
            request_id: Some(request_id.into()),
            query_id: None,
            table_name: Some(table_name.into()),
        }
    }

    pub fn query(request_id: impl Into<String>, table_name: impl Into<String>) -> Self {
        let request_id = request_id.into();
        Self {
            phase: BrowserWorkerEventPhase::Query,
            request_id: Some(request_id.clone()),
            query_id: Some(request_id),
            table_name: Some(table_name.into()),
        }
    }

    pub fn inspect(request_id: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            phase: BrowserWorkerEventPhase::Inspect,
            request_id: Some(request_id.into()),
            query_id: None,
            table_name: Some(table_name.into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerEventPhase {
    Instantiate,
    Open,
    Inspect,
    Query,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerProgressStage {
    Started,
    Planning,
    Executing,
    ArrowIpcReady,
    Finished,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerProgressEvent {
    pub context: BrowserWorkerEventContext,
    pub stage: BrowserWorkerProgressStage,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerLogEvent {
    pub context: BrowserWorkerEventContext,
    pub level: BrowserWorkerLogLevel,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerRangeReadMetricsEvent {
    pub context: BrowserWorkerEventContext,
    pub bytes_fetched: u64,
    pub files_touched: u64,
    pub files_skipped: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_fail_open_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_files_pruned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads_avoided: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_candidate_files: Option<u64>,
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_footer_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_footer_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_data_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesced_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesced_gap_bytes_fetched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_overfetch_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_hits: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_misses: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_range_reads_avoided: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_degraded_identity_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_present_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_missing_range_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_cache_hits: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_cache_misses: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_cache_bytes_reused: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_cache_bytes_stored: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_cache_validation_misses: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_cache_degraded_identity_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_readahead_requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_readahead_bytes_fetched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_readahead_bytes_used: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_readahead_wasted_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_resolution_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_log_manifest_list_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_log_manifest_list_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_resolve_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_resolve_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_cache_hit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_reuse_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opened_table_reuse_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_refresh_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_envelope_refresh_count: Option<u64>,
    pub rows_emitted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_bootstrap_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<BrowserAccessMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc_chunk_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_string_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc_encode_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerTransportCacheMetrics {
    pub bytes_reused: u64,
    pub validation_misses: u64,
    pub persistent_cache_errors: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerCacheMetricsEvent {
    pub context: BrowserWorkerEventContext,
    pub session_cached_bytes: u64,
    pub session_table_count: u64,
    pub max_session_cached_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<BrowserWorkerTransportCacheMetrics>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerFallbackEvent {
    pub context: BrowserWorkerEventContext,
    pub reason: FallbackReason,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerCancellationEvent {
    pub context: BrowserWorkerEventContext,
    pub error: QueryError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerTerminalErrorEvent {
    pub context: BrowserWorkerEventContext,
    pub error: QueryError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerArrowIpcChunkEvent {
    pub context: BrowserWorkerEventContext,
    pub request_id: String,
    pub sequence: u64,
    pub byte_offset: u64,
    pub byte_length: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerEventEnvelope {
    Progress(BrowserWorkerProgressEvent),
    Log(BrowserWorkerLogEvent),
    RangeReadMetrics(BrowserWorkerRangeReadMetricsEvent),
    CacheMetrics(BrowserWorkerCacheMetricsEvent),
    Fallback(BrowserWorkerFallbackEvent),
    Cancellation(BrowserWorkerCancellationEvent),
    TerminalError(BrowserWorkerTerminalErrorEvent),
    ArrowIpcChunk(BrowserWorkerArrowIpcChunkEvent),
}

impl BrowserWorkerEventEnvelope {
    pub fn progress(context: BrowserWorkerEventContext, stage: BrowserWorkerProgressStage) -> Self {
        Self::Progress(BrowserWorkerProgressEvent { context, stage })
    }

    pub fn log(
        context: BrowserWorkerEventContext,
        level: BrowserWorkerLogLevel,
        message: impl Into<String>,
    ) -> Self {
        Self::Log(BrowserWorkerLogEvent {
            context,
            level,
            message: message.into(),
        })
    }

    pub fn range_read_metrics(
        context: BrowserWorkerEventContext,
        metrics: QueryMetricsSummary,
    ) -> Self {
        Self::RangeReadMetrics(BrowserWorkerRangeReadMetricsEvent {
            context,
            bytes_fetched: metrics.bytes_fetched,
            files_touched: metrics.files_touched,
            files_skipped: metrics.files_skipped,
            prebootstrap_fail_open_count: metrics.prebootstrap_fail_open_count,
            prebootstrap_files_pruned: metrics.prebootstrap_files_pruned,
            footer_reads_avoided: metrics.footer_reads_avoided,
            prebootstrap_candidate_files: metrics.prebootstrap_candidate_files,
            row_groups_touched: metrics.row_groups_touched,
            row_groups_skipped: metrics.row_groups_skipped,
            footer_reads: metrics.footer_reads,
            bootstrap_footer_range_reads: metrics.bootstrap_footer_range_reads,
            scan_footer_range_reads: metrics.scan_footer_range_reads,
            scan_data_range_reads: metrics.scan_data_range_reads,
            duplicate_range_reads: metrics.duplicate_range_reads,
            coalesced_range_reads: metrics.coalesced_range_reads,
            coalesced_gap_bytes_fetched: metrics.coalesced_gap_bytes_fetched,
            scan_overfetch_bytes: metrics.scan_overfetch_bytes,
            footer_cache_hits: metrics.footer_cache_hits,
            footer_cache_misses: metrics.footer_cache_misses,
            footer_range_reads_avoided: metrics.footer_range_reads_avoided,
            footer_cache_degraded_identity_reads: metrics.footer_cache_degraded_identity_reads,
            identity_present_range_reads: metrics.identity_present_range_reads,
            identity_missing_range_reads: metrics.identity_missing_range_reads,
            range_cache_hits: metrics.range_cache_hits,
            range_cache_misses: metrics.range_cache_misses,
            range_cache_bytes_reused: metrics.range_cache_bytes_reused,
            range_cache_bytes_stored: metrics.range_cache_bytes_stored,
            range_cache_validation_misses: metrics.range_cache_validation_misses,
            range_cache_degraded_identity_reads: metrics.range_cache_degraded_identity_reads,
            range_readahead_requests: metrics.range_readahead_requests,
            range_readahead_bytes_fetched: metrics.range_readahead_bytes_fetched,
            range_readahead_bytes_used: metrics.range_readahead_bytes_used,
            range_readahead_wasted_bytes: metrics.range_readahead_wasted_bytes,
            descriptor_resolution_count: metrics.descriptor_resolution_count,
            delta_log_manifest_list_count: metrics.delta_log_manifest_list_count,
            delta_log_manifest_list_duration_ms: metrics.delta_log_manifest_list_duration_ms,
            snapshot_resolve_count: metrics.snapshot_resolve_count,
            snapshot_resolve_duration_ms: metrics.snapshot_resolve_duration_ms,
            descriptor_cache_hit: metrics.descriptor_cache_hit,
            session_reuse_count: metrics.session_reuse_count,
            opened_table_reuse_count: metrics.opened_table_reuse_count,
            identity_refresh_count: metrics.identity_refresh_count,
            access_envelope_refresh_count: metrics.access_envelope_refresh_count,
            rows_emitted: metrics.rows_emitted,
            snapshot_bootstrap_duration_ms: metrics.snapshot_bootstrap_duration_ms,
            access_mode: metrics.access_mode,
            arrow_ipc_bytes: metrics.arrow_ipc_bytes,
            arrow_ipc_chunk_count: metrics.arrow_ipc_chunk_count,
            preview_rows: metrics.preview_rows,
            preview_string_bytes: metrics.preview_string_bytes,
            planning_duration_ms: metrics.planning_duration_ms,
            arrow_ipc_encode_duration_ms: metrics.arrow_ipc_encode_duration_ms,
            preview_duration_ms: metrics.preview_duration_ms,
        })
    }

    pub fn cache_metrics(
        context: BrowserWorkerEventContext,
        session_cached_bytes: u64,
        session_table_count: u64,
        max_session_cached_bytes: u64,
        transport: Option<BrowserWorkerTransportCacheMetrics>,
    ) -> Self {
        Self::CacheMetrics(BrowserWorkerCacheMetricsEvent {
            context,
            session_cached_bytes,
            session_table_count,
            max_session_cached_bytes,
            transport,
        })
    }

    pub fn fallback(context: BrowserWorkerEventContext, reason: FallbackReason) -> Self {
        Self::Fallback(BrowserWorkerFallbackEvent { context, reason })
    }

    pub fn cancellation(context: BrowserWorkerEventContext, error: QueryError) -> Self {
        Self::Cancellation(BrowserWorkerCancellationEvent { context, error })
    }

    pub fn terminal_error(context: BrowserWorkerEventContext, error: QueryError) -> Self {
        Self::TerminalError(BrowserWorkerTerminalErrorEvent { context, error })
    }

    pub fn arrow_ipc_chunk(
        context: BrowserWorkerEventContext,
        request_id: impl Into<String>,
        sequence: u64,
        byte_offset: u64,
        bytes: Vec<u8>,
    ) -> Self {
        let byte_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Self::ArrowIpcChunk(BrowserWorkerArrowIpcChunkEvent {
            context,
            request_id: request_id.into(),
            sequence,
            byte_offset,
            byte_length,
            bytes,
        })
    }

    pub fn context(&self) -> &BrowserWorkerEventContext {
        match self {
            Self::Progress(event) => &event.context,
            Self::Log(event) => &event.context,
            Self::RangeReadMetrics(event) => &event.context,
            Self::CacheMetrics(event) => &event.context,
            Self::Fallback(event) => &event.context,
            Self::Cancellation(event) => &event.context,
            Self::TerminalError(event) => &event.context,
            Self::ArrowIpcChunk(event) => &event.context,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIpcFormat {
    Stream,
    File,
}

impl ArrowIpcFormat {
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::Stream => ARROW_IPC_STREAM_CONTENT_TYPE,
            Self::File => ARROW_IPC_FILE_CONTENT_TYPE,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIpcDelivery {
    #[default]
    SingleBuffer,
    ChunkedBuffers,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ArrowIpcResultEnvelope {
    pub format: ArrowIpcFormat,
    pub content_type: String,
    pub delivery: ArrowIpcDelivery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<Bytes>,
    pub byte_length: u64,
    pub chunk_count: u64,
}

impl ArrowIpcResultEnvelope {
    pub fn new(format: ArrowIpcFormat, bytes: Vec<u8>) -> Self {
        Self::from_bytes(format, Bytes::from(bytes))
    }

    pub fn from_bytes(format: ArrowIpcFormat, bytes: Bytes) -> Self {
        let byte_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Self {
            format,
            content_type: format.content_type().to_string(),
            delivery: ArrowIpcDelivery::SingleBuffer,
            bytes: Some(bytes),
            byte_length,
            chunk_count: if byte_length == 0 { 0 } else { 1 },
        }
    }

    pub fn chunked(
        format: ArrowIpcFormat,
        byte_length: u64,
        chunk_count: u64,
    ) -> Result<Self, String> {
        if byte_length > 0 && chunk_count == 0 {
            return Err(
                "chunked Arrow IPC result with non-zero byte_length requires chunk_count > 0"
                    .to_string(),
            );
        }
        if byte_length == 0 && chunk_count != 0 {
            return Err(
                "chunked Arrow IPC result with zero byte_length requires chunk_count == 0"
                    .to_string(),
            );
        }

        Ok(Self {
            format,
            content_type: format.content_type().to_string(),
            delivery: ArrowIpcDelivery::ChunkedBuffers,
            bytes: None,
            byte_length,
            chunk_count,
        })
    }
}

impl<'de> Deserialize<'de> for ArrowIpcResultEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawArrowIpcResultEnvelope {
            format: ArrowIpcFormat,
            content_type: String,
            #[serde(default)]
            delivery: ArrowIpcDelivery,
            #[serde(default)]
            bytes: Option<Bytes>,
            #[serde(default)]
            byte_length: Option<u64>,
            #[serde(default)]
            chunk_count: Option<u64>,
        }

        let raw = RawArrowIpcResultEnvelope::deserialize(deserializer)?;
        let expected_content_type = raw.format.content_type();

        if raw.content_type != expected_content_type {
            return Err(de::Error::custom(format!(
                "Arrow IPC content type '{}' does not match expected content type '{}' for format '{:?}'",
                raw.content_type, expected_content_type, raw.format
            )));
        }

        match raw.delivery {
            ArrowIpcDelivery::SingleBuffer => {
                let bytes = raw.bytes.ok_or_else(|| {
                    de::Error::custom("single_buffer Arrow IPC result requires bytes")
                })?;
                let byte_length = u64::try_from(bytes.len()).map_err(|_| {
                    de::Error::custom("single_buffer Arrow IPC byte length overflowed u64")
                })?;
                if let Some(declared_byte_length) = raw.byte_length {
                    if declared_byte_length != byte_length {
                        return Err(de::Error::custom(format!(
                            "single_buffer Arrow IPC byte_length {declared_byte_length} did not match bytes length {byte_length}"
                        )));
                    }
                }
                let chunk_count = raw
                    .chunk_count
                    .unwrap_or(if byte_length == 0 { 0 } else { 1 });
                if chunk_count > 1 {
                    return Err(de::Error::custom(
                        "single_buffer Arrow IPC result chunk_count must be 0 or 1",
                    ));
                }

                Ok(Self {
                    format: raw.format,
                    content_type: raw.content_type,
                    delivery: raw.delivery,
                    bytes: Some(bytes),
                    byte_length,
                    chunk_count,
                })
            }
            ArrowIpcDelivery::ChunkedBuffers => {
                if raw.bytes.is_some() {
                    return Err(de::Error::custom(
                        "chunked_buffers Arrow IPC result must not include final bytes",
                    ));
                }
                let byte_length = raw.byte_length.ok_or_else(|| {
                    de::Error::custom("chunked_buffers Arrow IPC result requires byte_length")
                })?;
                let chunk_count = raw.chunk_count.ok_or_else(|| {
                    de::Error::custom("chunked_buffers Arrow IPC result requires chunk_count")
                })?;
                if byte_length == 0 && chunk_count != 0 {
                    return Err(de::Error::custom(
                        "chunked_buffers Arrow IPC result with zero byte_length requires chunk_count == 0",
                    ));
                }
                if byte_length > 0 && chunk_count == 0 {
                    return Err(de::Error::custom(
                        "chunked_buffers Arrow IPC result with non-zero byte_length requires chunk_count > 0",
                    ));
                }

                Ok(Self {
                    format: raw.format,
                    content_type: raw.content_type,
                    delivery: raw.delivery,
                    bytes: None,
                    byte_length,
                    chunk_count,
                })
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerOpenedEnvelope {
    pub request_id: String,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerSuccessEnvelope {
    pub request_id: String,
    pub response: QueryResponse,
    pub result: ArrowIpcResultEnvelope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerParquetInspectionEnvelope {
    pub request_id: String,
    pub summary: ParquetInspectionSummary,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerDisposedEnvelope {
    pub request_id: String,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerErrorEnvelope {
    pub request_id: String,
    pub error: QueryError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerResponseEnvelope {
    Opened(BrowserWorkerOpenedEnvelope),
    Success(BrowserWorkerSuccessEnvelope),
    ParquetInspection(BrowserWorkerParquetInspectionEnvelope),
    Disposed(BrowserWorkerDisposedEnvelope),
    Error(BrowserWorkerErrorEnvelope),
}

impl BrowserWorkerResponseEnvelope {
    pub fn opened(request_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Opened(BrowserWorkerOpenedEnvelope {
            request_id: request_id.into(),
            name: name.into(),
        })
    }

    pub fn success(
        request_id: impl Into<String>,
        response: QueryResponse,
        result: ArrowIpcResultEnvelope,
    ) -> Self {
        Self::Success(BrowserWorkerSuccessEnvelope {
            request_id: request_id.into(),
            response,
            result,
        })
    }

    pub fn disposed(request_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Disposed(BrowserWorkerDisposedEnvelope {
            request_id: request_id.into(),
            name: name.into(),
        })
    }

    pub fn parquet_inspection(
        request_id: impl Into<String>,
        summary: ParquetInspectionSummary,
    ) -> Self {
        Self::ParquetInspection(BrowserWorkerParquetInspectionEnvelope {
            request_id: request_id.into(),
            summary,
        })
    }

    pub fn error(request_id: impl Into<String>, error: QueryError) -> Self {
        Self::Error(BrowserWorkerErrorEnvelope {
            request_id: request_id.into(),
            error,
        })
    }

    pub fn opened_envelope(&self) -> Option<&BrowserWorkerOpenedEnvelope> {
        match self {
            Self::Opened(opened) => Some(opened),
            _ => None,
        }
    }

    pub fn parquet_inspection_envelope(&self) -> Option<&BrowserWorkerParquetInspectionEnvelope> {
        match self {
            Self::ParquetInspection(inspection) => Some(inspection),
            _ => None,
        }
    }

    pub fn success_envelope(&self) -> Option<&BrowserWorkerSuccessEnvelope> {
        match self {
            Self::Success(success) => Some(success),
            _ => None,
        }
    }

    pub fn disposed_envelope(&self) -> Option<&BrowserWorkerDisposedEnvelope> {
        match self {
            Self::Disposed(disposed) => Some(disposed),
            _ => None,
        }
    }

    pub fn request_id(&self) -> &str {
        match self {
            Self::Opened(opened) => &opened.request_id,
            Self::Success(success) => &success.request_id,
            Self::ParquetInspection(inspection) => &inspection.request_id,
            Self::Disposed(disposed) => &disposed.request_id,
            Self::Error(error) => &error.request_id,
        }
    }

    pub fn fallback_reason(&self) -> Option<&FallbackReason> {
        match self {
            Self::Opened(_) | Self::ParquetInspection(_) | Self::Disposed(_) => None,
            Self::Success(success) => success.response.fallback_reason.as_ref(),
            Self::Error(error) => error.error.fallback_reason.as_ref(),
        }
    }
}

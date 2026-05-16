//! Public browser SDK contracts for worker-hosted query execution.

use query_contract::{
    BrowserAccessMode, BrowserHttpSnapshotDescriptor, ExecutionTarget, FallbackReason, QueryError,
    QueryMetricsSummary, QueryRequest, QueryResponse,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorkerSqlCommand {
    pub request_id: String,
    pub name: String,
    #[serde(rename = "query", alias = "request")]
    pub request: QueryRequest,
    #[serde(default)]
    pub output: BrowserWorkerSqlOutput,
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
            Self::Sql(command) => &command.request_id,
            Self::Dispose(command) => &command.request_id,
        }
    }

    pub fn table_name(&self) -> &str {
        match self {
            Self::OpenTable(command) => &command.name,
            Self::OpenDeltaTable(command) => &command.name,
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
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerEventPhase {
    Instantiate,
    Open,
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
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads: Option<u64>,
    pub rows_emitted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_bootstrap_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<BrowserAccessMode>,
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
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerEventEnvelope {
    Progress(BrowserWorkerProgressEvent),
    Log(BrowserWorkerLogEvent),
    RangeReadMetrics(BrowserWorkerRangeReadMetricsEvent),
    CacheMetrics(BrowserWorkerCacheMetricsEvent),
    Fallback(BrowserWorkerFallbackEvent),
    Cancellation(BrowserWorkerCancellationEvent),
    TerminalError(BrowserWorkerTerminalErrorEvent),
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
            row_groups_touched: metrics.row_groups_touched,
            row_groups_skipped: metrics.row_groups_skipped,
            footer_reads: metrics.footer_reads,
            rows_emitted: metrics.rows_emitted,
            snapshot_bootstrap_duration_ms: metrics.snapshot_bootstrap_duration_ms,
            access_mode: metrics.access_mode,
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

    pub fn context(&self) -> &BrowserWorkerEventContext {
        match self {
            Self::Progress(event) => &event.context,
            Self::Log(event) => &event.context,
            Self::RangeReadMetrics(event) => &event.context,
            Self::CacheMetrics(event) => &event.context,
            Self::Fallback(event) => &event.context,
            Self::Cancellation(event) => &event.context,
            Self::TerminalError(event) => &event.context,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ArrowIpcResultEnvelope {
    pub format: ArrowIpcFormat,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

impl ArrowIpcResultEnvelope {
    pub fn new(format: ArrowIpcFormat, bytes: Vec<u8>) -> Self {
        Self {
            format,
            content_type: format.content_type().to_string(),
            bytes,
        }
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
            bytes: Vec<u8>,
        }

        let raw = RawArrowIpcResultEnvelope::deserialize(deserializer)?;
        let expected_content_type = raw.format.content_type();

        if raw.content_type != expected_content_type {
            return Err(de::Error::custom(format!(
                "Arrow IPC content type '{}' does not match expected content type '{}' for format '{:?}'",
                raw.content_type, expected_content_type, raw.format
            )));
        }

        Ok(Self {
            format: raw.format,
            content_type: raw.content_type,
            bytes: raw.bytes,
        })
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
            Self::Disposed(disposed) => &disposed.request_id,
            Self::Error(error) => &error.request_id,
        }
    }

    pub fn fallback_reason(&self) -> Option<&FallbackReason> {
        match self {
            Self::Opened(_) | Self::Disposed(_) => None,
            Self::Success(success) => success.response.fallback_reason.as_ref(),
            Self::Error(error) => error.error.fallback_reason.as_ref(),
        }
    }
}

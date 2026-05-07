//! Public browser SDK contracts for worker-hosted query execution.

use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, FallbackReason, QueryError, QueryRequest,
    QueryResponse,
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

//! Public browser SDK contracts for worker-hosted query execution.

use query_contract::{ExecutionTarget, FallbackReason, QueryError, QueryRequest, QueryResponse};
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
pub struct BrowserWorkerRequestEnvelope {
    pub request_id: String,
    pub request: QueryRequest,
}

impl BrowserWorkerRequestEnvelope {
    pub fn new(request_id: impl Into<String>, request: QueryRequest) -> Self {
        Self {
            request_id: request_id.into(),
            request,
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
pub struct BrowserWorkerSuccessEnvelope {
    pub request_id: String,
    pub response: QueryResponse,
    pub result: ArrowIpcResultEnvelope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrowserWorkerErrorEnvelope {
    pub request_id: String,
    pub error: QueryError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserWorkerResponseEnvelope {
    Success(BrowserWorkerSuccessEnvelope),
    Error(BrowserWorkerErrorEnvelope),
}

impl BrowserWorkerResponseEnvelope {
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

    pub fn error(request_id: impl Into<String>, error: QueryError) -> Self {
        Self::Error(BrowserWorkerErrorEnvelope {
            request_id: request_id.into(),
            error,
        })
    }

    pub fn success_envelope(&self) -> Option<&BrowserWorkerSuccessEnvelope> {
        match self {
            Self::Success(success) => Some(success),
            Self::Error(_) => None,
        }
    }

    pub fn fallback_reason(&self) -> Option<&FallbackReason> {
        match self {
            Self::Success(success) => success.response.fallback_reason.as_ref(),
            Self::Error(error) => error.error.fallback_reason.as_ref(),
        }
    }
}

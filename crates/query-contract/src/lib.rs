//! Shared contracts for query execution across browser, native, and control-plane layers.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTarget {
    BrowserWasm,
    Native,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKey {
    ChangeDataFeed,
    ColumnMapping,
    DeletionVectors,
    MultiPartitionExecution,
    ProxyAccess,
    RangeReads,
    SignedUrlAccess,
    TimeTravel,
    TimestampNtz,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityState {
    Supported,
    NativeOnly,
    Unsupported,
    Experimental,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct CapabilityReport {
    pub capabilities: BTreeMap<CapabilityKey, CapabilityState>,
}

impl CapabilityReport {
    pub fn from_pairs<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (CapabilityKey, CapabilityState)>,
    {
        Self {
            capabilities: pairs.into_iter().collect(),
        }
    }

    pub fn insert(&mut self, key: CapabilityKey, value: CapabilityState) {
        self.capabilities.insert(key, value);
    }

    pub fn state(&self, key: CapabilityKey) -> Option<CapabilityState> {
        self.capabilities.get(&key).copied()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackReason {
    AccessDenied,
    BrowserRuntimeConstraint,
    NativeRequired,
    NetworkFailure,
    RangeReadUnavailable,
    SecurityPolicy,
    SignedUrlExpired,
    CapabilityGate {
        capability: CapabilityKey,
        required_state: CapabilityState,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryErrorCode {
    AccessDenied,
    ExecutionFailed,
    FallbackRequired,
    InvalidRequest,
    ObjectStoreProtocol,
    SecurityPolicyViolation,
    UnsupportedFeature,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryError {
    pub code: QueryErrorCode,
    pub message: String,
    pub target: ExecutionTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<FallbackReason>,
}

impl QueryError {
    pub fn new(code: QueryErrorCode, message: impl Into<String>, target: ExecutionTarget) -> Self {
        Self {
            code,
            message: message.into(),
            target,
            fallback_reason: None,
        }
    }

    pub fn with_fallback_reason(mut self, fallback_reason: FallbackReason) -> Self {
        self.fallback_reason = Some(fallback_reason);
        self
    }

    pub fn requires_fallback(&self) -> bool {
        self.fallback_reason.is_some()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryRequest {
    pub sql: String,
    pub preferred_target: ExecutionTarget,
}

impl QueryRequest {
    pub fn new(sql: impl Into<String>, preferred_target: ExecutionTarget) -> Self {
        Self {
            sql: sql.into(),
            preferred_target,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryMetricsSummary {
    pub bytes_fetched: u64,
    pub files_touched: u64,
    pub files_skipped: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryResponse {
    pub executed_on: ExecutionTarget,
    pub capabilities: CapabilityReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<FallbackReason>,
    pub metrics: QueryMetricsSummary,
}

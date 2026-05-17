//! Shared contracts for query execution across browser, native, and control-plane layers.

mod delta_protocol_features;

use std::collections::BTreeMap;
use std::net::IpAddr;

pub use delta_protocol_features::{
    delta_protocol_feature, delta_protocol_feature_names, DeltaProtocolFeature,
    DeltaProtocolFeatureClass, DeltaProtocolFeatureEnablement, DeltaProtocolFeatureKind,
    KNOWN_DELTA_PROTOCOL_FEATURES,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

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
    UnknownProtocolFeatures,
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryExecutionOptions {
    #[serde(default)]
    pub include_explain: bool,
    #[serde(default = "default_collect_metrics")]
    pub collect_metrics: bool,
}

impl Default for QueryExecutionOptions {
    fn default() -> Self {
        Self {
            include_explain: false,
            collect_metrics: default_collect_metrics(),
        }
    }
}

const fn default_collect_metrics() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryRequest {
    pub table_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<i64>,
    pub sql: String,
    pub preferred_target: ExecutionTarget,
    #[serde(default)]
    pub options: QueryExecutionOptions,
}

impl QueryRequest {
    pub fn new(
        table_uri: impl Into<String>,
        sql: impl Into<String>,
        preferred_target: ExecutionTarget,
    ) -> Self {
        Self {
            table_uri: table_uri.into(),
            snapshot_version: None,
            sql: sql.into(),
            preferred_target,
            options: QueryExecutionOptions::default(),
        }
    }

    pub fn with_options(mut self, options: QueryExecutionOptions) -> Self {
        self.options = options;
        self
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SnapshotResolutionRequest {
    pub table_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ResolvedFileDescriptor {
    pub path: String,
    pub size_bytes: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionColumnType {
    String,
    Int64,
    Boolean,
    Unsupported,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ResolvedSnapshotDescriptor {
    pub table_uri: String,
    pub snapshot_version: i64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_column_types: BTreeMap<String, PartitionColumnType>,
    #[serde(default, skip_serializing_if = "capability_report_is_empty")]
    pub browser_compatibility: CapabilityReport,
    #[serde(default, skip_serializing_if = "capability_report_is_empty")]
    pub required_capabilities: CapabilityReport,
    pub active_files: Vec<ResolvedFileDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserHttpFileDescriptor {
    pub path: String,
    pub url: String,
    pub size_bytes: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserHttpSnapshotDescriptor {
    pub table_uri: String,
    pub snapshot_version: i64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_column_types: BTreeMap<String, PartitionColumnType>,
    #[serde(default, skip_serializing_if = "capability_report_is_empty")]
    pub browser_compatibility: CapabilityReport,
    #[serde(default, skip_serializing_if = "capability_report_is_empty")]
    pub required_capabilities: CapabilityReport,
    pub active_files: Vec<BrowserHttpFileDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ParquetCompressionSummary {
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
    pub ratio_basis_points: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ParquetInspectionColumnChunk {
    pub column_name: String,
    pub compression: String,
    pub encodings: Vec<String>,
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    pub has_statistics: bool,
    pub has_column_index: bool,
    pub has_offset_index: bool,
    pub has_bloom_filter: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ParquetInspectionRowGroup {
    pub index: u64,
    pub row_count: u64,
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
    pub columns: Vec<ParquetInspectionColumnChunk>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ParquetInspectionColumn {
    pub name: String,
    pub physical_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converted_type: Option<String>,
    pub repetition: String,
    pub nullable: bool,
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    pub encodings: Vec<String>,
    pub compressions: Vec<String>,
    pub has_statistics: bool,
    pub has_column_index: bool,
    pub has_offset_index: bool,
    pub has_bloom_filter: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ParquetInspectionSummary {
    pub path: String,
    pub object_size_bytes: u64,
    pub footer_length_bytes: u32,
    pub metadata_memory_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    pub file_version: i32,
    pub row_group_count: u64,
    pub row_count: u64,
    pub column_count: u64,
    pub compression: ParquetCompressionSummary,
    pub columns: Vec<ParquetInspectionColumn>,
    pub row_groups: Vec<ParquetInspectionRowGroup>,
}

fn capability_report_is_empty(report: &CapabilityReport) -> bool {
    report.capabilities.is_empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserObjectUrlPolicy {
    HttpsOnly,
    HttpsOrLoopbackHttpForHostTests,
}

pub fn validate_browser_object_url(
    url: &str,
    target: ExecutionTarget,
    policy: BrowserObjectUrlPolicy,
    invalid_url_label: &str,
) -> Result<Url, QueryError> {
    let parsed = Url::parse(url).map_err(|error| {
        QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!(
                "invalid {invalid_url_label} '{}': {error}",
                redacted_input_url(url)
            ),
            target,
        )
    })?;

    match parsed.scheme() {
        "https" => Ok(parsed),
        "http" if allows_loopback_http_for_host_tests(&parsed, policy) => Ok(parsed),
        "http" => Err(QueryError::new(
            QueryErrorCode::SecurityPolicyViolation,
            format!(
                "{}: '{}'",
                browser_object_url_security_message(policy),
                redacted_url(&parsed)
            ),
            target,
        )),
        scheme => Err(QueryError::new(
            QueryErrorCode::InvalidRequest,
            format!(
                "browser object URL '{}' uses unsupported scheme '{scheme}'",
                redacted_url(&parsed)
            ),
            target,
        )),
    }
}

fn browser_object_url_security_message(policy: BrowserObjectUrlPolicy) -> &'static str {
    match policy {
        BrowserObjectUrlPolicy::HttpsOnly => "browser object URLs must use HTTPS",
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests => {
            "browser object URLs must use HTTPS unless they target loopback hosts for host-side tests"
        }
    }
}

fn allows_loopback_http_for_host_tests(url: &Url, policy: BrowserObjectUrlPolicy) -> bool {
    matches!(
        policy,
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests
    ) && cfg!(not(target_arch = "wasm32"))
        && match url.host_str() {
            Some("localhost") => true,
            Some(host) => host
                .parse::<IpAddr>()
                .map(|address| address.is_loopback())
                .unwrap_or(false),
            None => false,
        }
}

fn redacted_input_url(url: &str) -> String {
    let end = url.find(['?', '#']).unwrap_or(url.len());
    url[..end].to_string()
}

fn redacted_url(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserAccessMode {
    #[default]
    BrowserSafeHttp,
    CloudObjectStore,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryMetricsSummary {
    /// Bytes scanned by the executed query plan when the runtime can report them; otherwise `0`.
    pub bytes_fetched: u64,
    /// Wall-clock execution time for the query call.
    pub duration_ms: u64,
    /// Data files scanned by the executed query plan when the runtime can report them; otherwise `0`.
    pub files_touched: u64,
    /// Data files skipped by partition or file pruning when the runtime can report them; otherwise `0`.
    pub files_skipped: u64,
    /// Parquet row groups decoded by the executed query plan when the runtime can report them.
    #[serde(default)]
    pub row_groups_touched: u64,
    /// Parquet row groups skipped by row-group pruning when the runtime can report them.
    #[serde(default)]
    pub row_groups_skipped: u64,
    /// Footer-range reads performed during browser snapshot bootstrap when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads: Option<u64>,
    /// Rows emitted by the scan layer when the runtime can report them; otherwise `0`.
    #[serde(default)]
    pub rows_emitted: u64,
    /// Wall-clock duration of browser snapshot bootstrap when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_bootstrap_duration_ms: Option<u64>,
    /// Browser object access mode used for the execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<BrowserAccessMode>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryResponse {
    pub executed_on: ExecutionTarget,
    pub capabilities: CapabilityReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<FallbackReason>,
    pub metrics: QueryMetricsSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<String>,
}

#[cfg(test)]
mod explain_field_tests {
    use super::*;

    #[test]
    fn query_response_round_trips_explain_field() {
        let resp = QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::default(),
            fallback_reason: None,
            metrics: QueryMetricsSummary::default(),
            explain: Some("Scan: events\n  Filter: status = 'fulfilled'".into()),
        };
        let wire = serde_json::to_string(&resp).expect("serialize");
        assert!(wire.contains("\"explain\""), "wire form: {wire}");
        let back: QueryResponse = serde_json::from_str(&wire).expect("deserialize");
        assert_eq!(
            back.explain.as_deref(),
            Some("Scan: events\n  Filter: status = 'fulfilled'")
        );
    }

    #[test]
    fn query_response_omits_explain_when_none() {
        let resp = QueryResponse {
            executed_on: ExecutionTarget::BrowserWasm,
            capabilities: CapabilityReport::default(),
            fallback_reason: None,
            metrics: QueryMetricsSummary::default(),
            explain: None,
        };
        let wire = serde_json::to_string(&resp).expect("serialize");
        assert!(
            !wire.contains("\"explain\""),
            "expected explain to be omitted; wire: {wire}"
        );
    }
}

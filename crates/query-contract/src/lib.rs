//! Shared contracts for query execution across browser, native, and control-plane layers.

mod delta_protocol_features;

use std::collections::{BTreeMap, BTreeSet};
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
    ObjectNotFound,
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

/// Maximum rows a runtime may materialize for one result page, including the sentinel row.
pub const MAX_QUERY_RESULT_PAGE_LIMIT: u64 = 501;

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryResultPage {
    pub limit: u64,
    pub offset: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRuntimeLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_result_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_arrow_ipc_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_preview_string_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_scan_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct QueryExecutionOptions {
    #[serde(default)]
    pub include_explain: bool,
    #[serde(default = "default_collect_metrics")]
    pub collect_metrics: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_page: Option<QueryResultPage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_limits: Option<QueryRuntimeLimits>,
}

impl Default for QueryExecutionOptions {
    fn default() -> Self {
        Self {
            include_explain: false,
            collect_metrics: default_collect_metrics(),
            result_page: None,
            runtime_limits: None,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionLiteralValue {
    String(String),
    Int64(i64),
    Boolean(bool),
    Null,
}

impl PartitionColumnType {
    pub fn normalize_partition_value(&self, value: Option<&str>) -> Option<Option<String>> {
        if matches!(self, Self::Unsupported) {
            return None;
        }

        match value {
            None => Some(None),
            Some(value) => self.normalize_non_null_partition_value(value).map(Some),
        }
    }

    pub fn normalize_partition_literal(
        &self,
        literal: &PartitionLiteralValue,
    ) -> Option<Option<String>> {
        match (self, literal) {
            (Self::Unsupported, _) => None,
            (_, PartitionLiteralValue::Null) => Some(None),
            (Self::String, PartitionLiteralValue::String(value)) => Some(Some(value.clone())),
            (Self::Int64, PartitionLiteralValue::Int64(value)) => Some(Some(value.to_string())),
            (Self::Boolean, PartitionLiteralValue::Boolean(value)) => Some(Some(value.to_string())),
            _ => None,
        }
    }

    pub fn normalize_non_null_partition_value(&self, value: &str) -> Option<String> {
        match self {
            Self::String => Some(value.to_string()),
            Self::Int64 => parse_canonical_partition_i64(value).map(|value| value.to_string()),
            Self::Boolean => parse_canonical_partition_bool(value).map(|value| value.to_string()),
            Self::Unsupported => None,
        }
    }
}

pub fn parse_canonical_partition_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

pub fn parse_canonical_partition_i64(value: &str) -> Option<i64> {
    let parsed = value.parse::<i64>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
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
#[serde(deny_unknown_fields)]
pub struct BrowserHttpFileDescriptor {
    pub path: String,
    pub url: String,
    pub size_bytes: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_etag: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct BrowserHttpParquetDatasetDescriptor {
    pub table_uri: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_column_types: BTreeMap<String, PartitionColumnType>,
    #[serde(default, skip_serializing_if = "capability_report_is_empty")]
    pub browser_compatibility: CapabilityReport,
    #[serde(default, skip_serializing_if = "capability_report_is_empty")]
    pub required_capabilities: CapabilityReport,
    pub files: Vec<BrowserHttpFileDescriptor>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaObjectStoreProvider {
    S3,
    Gcs,
    AzureBlob,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolverRequestedAccessMode {
    #[default]
    Auto,
    SignedUrl,
    Proxy,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolverActualAccessMode {
    SignedUrl,
    Proxy,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct CredentialProfileRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

/// Request sent from browser-facing SDKs to a trusted Delta snapshot descriptor resolver.
///
/// The resolver owns cloud IAM, Delta log inspection, URL signing/proxying, CORS checks,
/// auditing, and request correlation. Browser packages only send a logical table URI plus an
/// opaque storage access profile handle and receive a `BrowserHttpSnapshotDescriptor`.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct DeltaLocationResolveRequest {
    pub provider: DeltaObjectStoreProvider,
    pub table_uri: String,
    pub credential_profile: CredentialProfileRef,
    #[serde(default)]
    pub requested_access_mode: ResolverRequestedAccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct DeltaLocationRefresh {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_after_epoch_ms: Option<u64>,
    pub same_snapshot_required: bool,
}

/// Authoritative resolver envelope for a browser-safe Delta snapshot descriptor.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct DeltaLocationResolveResponse {
    pub descriptor: BrowserHttpSnapshotDescriptor,
    pub provider: DeltaObjectStoreProvider,
    pub table_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_snapshot_version: Option<i64>,
    pub resolved_snapshot_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_access_mode: Option<ResolverRequestedAccessMode>,
    pub actual_access_mode: ResolverActualAccessMode,
    pub expires_at_epoch_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh: Option<DeltaLocationRefresh>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaLocationResolverErrorCode {
    InvalidTableUri,
    InvalidSnapshotVersion,
    CredentialProfileRequired,
    CredentialProfileNotFound,
    ProviderNotSupported,
    AccessModeNotSupported,
    SnapshotVersionNotFound,
    NotADeltaTable,
    ResolverUnavailable,
    StorageAuthFailed,
    StorageCorsFailed,
    DescriptorExpired,
    PolicyBlocked,
    ProxyRequired,
    SignedUrlUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct DeltaLocationResolverError {
    pub code: DeltaLocationResolverErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

impl DeltaLocationResolverError {
    pub fn new(code: DeltaLocationResolverErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: redact_sensitive_text(&message.into()),
            correlation_id: None,
        }
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }
}

pub fn validate_delta_location_resolve_request(
    request: &DeltaLocationResolveRequest,
) -> Result<(), DeltaLocationResolverError> {
    if request
        .snapshot_version
        .is_some_and(|snapshot_version| snapshot_version < 0)
    {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidSnapshotVersion,
            "Delta location snapshot version must be a non-negative integer",
        ));
    }
    validate_credential_profile_ref(&request.credential_profile)?;
    validate_delta_table_uri(request.provider, &request.table_uri)
}

pub fn validate_delta_location_resolve_response(
    response: &DeltaLocationResolveResponse,
    now_epoch_ms: u64,
) -> Result<(), DeltaLocationResolverError> {
    if response.expires_at_epoch_ms <= now_epoch_ms {
        return Err(delta_location_response_error(
            DeltaLocationResolverErrorCode::DescriptorExpired,
            "resolved Delta descriptor has expired",
            response,
        ));
    }

    if response
        .requested_snapshot_version
        .is_some_and(|snapshot_version| snapshot_version < 0)
        || response.resolved_snapshot_version < 0
        || response.descriptor.snapshot_version < 0
    {
        return Err(delta_location_response_error(
            DeltaLocationResolverErrorCode::InvalidSnapshotVersion,
            "resolved Delta snapshot versions must be non-negative integers",
            response,
        ));
    }

    if response.descriptor.snapshot_version != response.resolved_snapshot_version {
        return Err(delta_location_response_error(
            DeltaLocationResolverErrorCode::PolicyBlocked,
            "resolved snapshot metadata did not match the descriptor snapshot version",
            response,
        ));
    }

    if let Some(refresh) = &response.refresh {
        if !refresh.same_snapshot_required {
            return Err(delta_location_response_error(
                DeltaLocationResolverErrorCode::PolicyBlocked,
                "Delta location refresh must require the same resolved snapshot",
                response,
            ));
        }
        if let Some(refresh_url) = &refresh.refresh_url {
            validate_resolver_url(refresh_url, "refresh URL")
                .map_err(|error| attach_delta_location_response_correlation(error, response))?;
        }
    }

    for file in &response.descriptor.active_files {
        validate_browser_object_url(
            &file.url,
            ExecutionTarget::BrowserWasm,
            BrowserObjectUrlPolicy::HttpsOnly,
            "Delta descriptor active file URL",
        )
        .map_err(|error| {
            delta_location_response_error(
                DeltaLocationResolverErrorCode::PolicyBlocked,
                error.message,
                response,
            )
        })?;
    }

    validate_delta_table_uri(response.provider, &response.table_uri)
        .map_err(|error| attach_delta_location_response_correlation(error, response))
}

/// Validate a complete browser resolver exchange, including request/response consistency.
///
/// `validate_delta_location_resolve_response` is useful for checking a standalone resolver
/// envelope. Call this function when the original request is available and the caller needs to
/// prove that the resolver did not answer for a different provider, table, snapshot, or access
/// mode.
pub fn validate_delta_location_resolve_exchange(
    request: &DeltaLocationResolveRequest,
    response: &DeltaLocationResolveResponse,
    now_epoch_ms: u64,
) -> Result<(), DeltaLocationResolverError> {
    validate_delta_location_resolve_request(request)?;
    validate_delta_location_resolve_response(response, now_epoch_ms)?;

    if response.provider != request.provider {
        return Err(delta_location_response_error(
            DeltaLocationResolverErrorCode::PolicyBlocked,
            format!(
                "resolver response provider '{:?}' did not match request provider '{:?}'",
                response.provider, request.provider
            ),
            response,
        ));
    }

    if response.table_uri != request.table_uri {
        return Err(delta_location_response_error(
            DeltaLocationResolverErrorCode::PolicyBlocked,
            "resolver response table URI did not match the requested Delta location",
            response,
        ));
    }

    if let Some(requested_snapshot_version) = request.snapshot_version {
        if response.requested_snapshot_version != Some(requested_snapshot_version) {
            return Err(delta_location_response_error(
                DeltaLocationResolverErrorCode::SnapshotVersionNotFound,
                "resolver response did not confirm the requested Delta snapshot version",
                response,
            ));
        }
        if response.resolved_snapshot_version != requested_snapshot_version {
            return Err(delta_location_response_error(
                DeltaLocationResolverErrorCode::SnapshotVersionNotFound,
                "resolver response resolved a different Delta snapshot version",
                response,
            ));
        }
    }

    if let Some(requested_access_mode) = response.requested_access_mode {
        if requested_access_mode != request.requested_access_mode {
            return Err(delta_location_response_error(
                DeltaLocationResolverErrorCode::AccessModeNotSupported,
                "resolver response requested access mode did not match the SDK request",
                response,
            ));
        }
    }

    let requested_access_mode_satisfied =
        match (request.requested_access_mode, response.actual_access_mode) {
            (ResolverRequestedAccessMode::Auto, _) => true,
            (ResolverRequestedAccessMode::SignedUrl, ResolverActualAccessMode::SignedUrl) => true,
            (ResolverRequestedAccessMode::Proxy, ResolverActualAccessMode::Proxy) => true,
            _ => false,
        };
    if !requested_access_mode_satisfied {
        return Err(delta_location_response_error(
            DeltaLocationResolverErrorCode::AccessModeNotSupported,
            "resolver response actual access mode did not satisfy the SDK request",
            response,
        ));
    }

    Ok(())
}

fn delta_location_response_error(
    code: DeltaLocationResolverErrorCode,
    message: impl Into<String>,
    response: &DeltaLocationResolveResponse,
) -> DeltaLocationResolverError {
    let error = DeltaLocationResolverError::new(code, message);
    if let Some(correlation_id) = &response.correlation_id {
        error.with_correlation_id(correlation_id.clone())
    } else {
        error
    }
}

fn attach_delta_location_response_correlation(
    error: DeltaLocationResolverError,
    response: &DeltaLocationResolveResponse,
) -> DeltaLocationResolverError {
    if error.correlation_id.is_none() {
        if let Some(correlation_id) = &response.correlation_id {
            return error.with_correlation_id(correlation_id.clone());
        }
    }
    error
}

fn validate_credential_profile_ref(
    profile: &CredentialProfileRef,
) -> Result<(), DeltaLocationResolverError> {
    if profile.id.trim().is_empty() {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::CredentialProfileRequired,
            "storage access profile id is required",
        ));
    }

    if contains_secret_material(&profile.id) {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::PolicyBlocked,
            "storage access profile id must be an opaque policy handle, not cloud credential material",
        ));
    }

    Ok(())
}

fn validate_delta_table_uri(
    provider: DeltaObjectStoreProvider,
    table_uri: &str,
) -> Result<(), DeltaLocationResolverError> {
    if contains_secret_material(table_uri) {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            "Delta table URI must not contain credential material or signed URL parameters",
        ));
    }

    let parsed = Url::parse(table_uri).map_err(|error| {
        DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            format!(
                "invalid Delta table URI '{}': {error}",
                redacted_input_url(table_uri)
            ),
        )
    })?;

    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            format!(
                "Delta table URI '{}' must be a logical object-store URI without query strings or fragments",
                redacted_url(&parsed)
            ),
        ));
    }

    match provider {
        DeltaObjectStoreProvider::S3 => validate_s3_table_uri(&parsed),
        DeltaObjectStoreProvider::Gcs => validate_gcs_table_uri(&parsed),
        DeltaObjectStoreProvider::AzureBlob => validate_azure_table_uri(&parsed),
    }
}

fn validate_s3_table_uri(url: &Url) -> Result<(), DeltaLocationResolverError> {
    if url.scheme() != "s3"
        || url.host_str().is_none()
        || !has_non_root_path(url)
        || has_userinfo(url)
    {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            "S3 Delta table URI must look like s3://bucket/table without userinfo",
        ));
    }
    Ok(())
}

fn validate_gcs_table_uri(url: &Url) -> Result<(), DeltaLocationResolverError> {
    if url.scheme() != "gs"
        || url.host_str().is_none()
        || !has_non_root_path(url)
        || has_userinfo(url)
    {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            "GCS Delta table URI must look like gs://bucket/table without userinfo",
        ));
    }
    Ok(())
}

fn validate_azure_table_uri(url: &Url) -> Result<(), DeltaLocationResolverError> {
    match url.scheme() {
        "az" => {
            if url.host_str().is_some() && has_container_and_table_path(url) && !has_userinfo(url) {
                Ok(())
            } else {
                Err(DeltaLocationResolverError::new(
                    DeltaLocationResolverErrorCode::InvalidTableUri,
                    "Azure Blob Delta table URI must look like az://account/container/table",
                ))
            }
        }
        "abfs" => {
            if url.host_str().is_some()
                && !url.username().is_empty()
                && url.password().is_none()
                && has_non_root_path(url)
            {
                Ok(())
            } else {
                Err(DeltaLocationResolverError::new(
                    DeltaLocationResolverErrorCode::InvalidTableUri,
                    "ABFS Delta table URI must look like abfs://container@account.dfs.core.windows.net/table",
                ))
            }
        }
        _ => Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            "Azure Blob resolver only accepts az:// or abfs:// Delta table URIs",
        )),
    }
}

fn validate_resolver_url(url: &str, label: &str) -> Result<(), DeltaLocationResolverError> {
    let parsed = Url::parse(url).map_err(|error| {
        DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            format!("invalid {label} '{}': {error}", redacted_input_url(url)),
        )
    })?;

    if parsed.scheme() != "https" {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidTableUri,
            format!("{label} '{}' must use HTTPS", redacted_url(&parsed)),
        ));
    }

    Ok(())
}

fn has_non_root_path(url: &Url) -> bool {
    let path = url.path().trim_matches('/');
    !path.is_empty()
}

fn has_container_and_table_path(url: &Url) -> bool {
    let mut parts = url
        .path_segments()
        .into_iter()
        .flatten()
        .filter(|part| !part.is_empty());
    parts.next().is_some() && parts.next().is_some()
}

fn has_userinfo(url: &Url) -> bool {
    !url.username().is_empty() || url.password().is_some()
}

fn contains_secret_material(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("service_account")
        || lower.contains("private_key")
        || lower.contains("client_secret")
        || lower.contains("refresh_token")
        || lower.contains("authorization:")
        || lower.contains("bearer ")
        || lower.contains("aws_secret_access_key")
        || lower.contains("azure_client_secret")
        || lower.contains("google_application_credentials")
        || lower.contains("x-amz-signature")
        || lower.contains("x-goog-signature")
        || lower.contains("x-goog-credential")
        || lower.contains("awsaccesskeyid")
        || lower.contains("access_token")
        || lower.contains("signature=")
        || lower.contains("sig=")
        || lower.contains("sv=")
        || looks_like_aws_access_key(value)
}

fn looks_like_aws_access_key(value: &str) -> bool {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|token| {
            token.len() == 20
                && (token.starts_with("AKIA") || token.starts_with("ASIA"))
                && token
                    .chars()
                    .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
        })
}

fn redact_sensitive_text(value: &str) -> String {
    let tokens = value.split_whitespace().collect::<Vec<_>>();
    let mut redacted = Vec::with_capacity(tokens.len());
    let mut index = 0;

    while index < tokens.len() {
        let token = tokens[index];
        let lower = token.to_ascii_lowercase();

        if lower == "authorization:" {
            redacted.push("[REDACTED]".to_string());
            index += 1;
            if index < tokens.len() {
                index += 1;
                if index < tokens.len() {
                    index += 1;
                }
            }
            continue;
        }

        if let Some(after_header) = lower.strip_prefix("authorization:") {
            redacted.push("[REDACTED]".to_string());
            index += 1;
            if !after_header.is_empty() && index < tokens.len() {
                index += 1;
            }
            continue;
        }

        if is_bearer_marker(&lower) {
            redacted.push("[REDACTED]".to_string());
            index += 2;
            continue;
        }

        if contains_secret_material(token) {
            redacted.push("[REDACTED]".to_string());
        } else {
            redacted.push(token.to_string());
        }
        index += 1;
    }

    redact_url_like_secrets(&redacted.join(" "))
}

fn is_bearer_marker(token: &str) -> bool {
    token.trim_matches(|ch: char| !ch.is_ascii_alphanumeric()) == "bearer"
}

fn redact_url_like_secrets(value: &str) -> String {
    value
        .split(' ')
        .map(|token| {
            Url::parse(token)
                .map(|url| redacted_url(&url))
                .unwrap_or_else(|_| token.to_string())
        })
        .collect::<Vec<_>>()
        .join(" ")
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

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "plan_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadAccessPlan {
    BrokeredDelta(BrokeredDeltaReadPlan),
    DeltaSharing(DeltaSharingReadPlan),
    SqlFallbackRequired(SqlFallbackRequiredPlan),
    Blocked(BlockedReadPlan),
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokeredDeltaReadPlan {
    pub table_id: String,
    pub full_name: String,
    pub table_root: String,
    pub grant_id: String,
    pub expires_at_epoch_ms: u64,
    pub delta_access_mode: BrokeredDeltaAccessMode,
    pub policy_authority: BrokeredPolicyAuthority,
    pub object_access: BrokeredObjectAccess,
}

impl BrokeredDeltaReadPlan {
    pub fn to_browser_http_snapshot_descriptor(
        &self,
        resolved_snapshot: ResolvedSnapshotDescriptor,
        object_urls_by_path: &BTreeMap<String, String>,
    ) -> Result<BrowserHttpSnapshotDescriptor, QueryError> {
        if self.policy_authority.direct_external_engine_read
            != DirectExternalEngineReadSupport::Confirmed
        {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                "brokered Delta direct read requires confirmed UC external-engine support",
                ExecutionTarget::BrowserWasm,
            ));
        }
        if !self.object_access.batch_sign {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                "brokered Delta descriptor adaptation requires batch signed object URLs",
                ExecutionTarget::BrowserWasm,
            ));
        }
        if !self.object_access.range_get {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                "brokered Delta descriptor adaptation requires range-readable objects",
                ExecutionTarget::BrowserWasm,
            ));
        }
        if resolved_snapshot.table_uri != self.table_root {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "brokered Delta plan tableRoot did not match resolved snapshot table_uri",
                ExecutionTarget::BrowserWasm,
            ));
        }

        let mut resolved_paths = BTreeSet::new();
        let mut duplicate_paths = BTreeSet::new();
        for file in &resolved_snapshot.active_files {
            if !resolved_paths.insert(file.path.clone()) {
                duplicate_paths.insert(file.path.clone());
            }
        }
        let missing_paths = resolved_snapshot
            .active_files
            .iter()
            .filter(|file| !object_urls_by_path.contains_key(&file.path))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let unexpected_paths = object_urls_by_path
            .keys()
            .filter(|path| !resolved_paths.contains(*path))
            .cloned()
            .collect::<Vec<_>>();

        if !duplicate_paths.is_empty() || !missing_paths.is_empty() || !unexpected_paths.is_empty()
        {
            let mut reasons = Vec::new();
            if !duplicate_paths.is_empty() {
                reasons.push(format!(
                    "resolved snapshot contained duplicate paths [{}]",
                    quote_joined_paths(duplicate_paths.iter())
                ));
            }
            if !missing_paths.is_empty() {
                reasons.push(format!(
                    "missing URLs for [{}]",
                    quote_joined_paths(missing_paths.iter())
                ));
            }
            if !unexpected_paths.is_empty() {
                reasons.push(format!(
                    "unexpected URLs for [{}]",
                    quote_joined_paths(unexpected_paths.iter())
                ));
            }

            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                format!(
                    "brokered Delta signed URL coverage did not match the resolved snapshot: {}",
                    reasons.join("; ")
                ),
                ExecutionTarget::BrowserWasm,
            ));
        }

        let active_files = resolved_snapshot
            .active_files
            .into_iter()
            .map(|file| {
                let url = object_urls_by_path
                    .get(&file.path)
                    .expect("missing signed URL coverage should be rejected before mapping");
                validate_browser_object_url(
                    url,
                    ExecutionTarget::BrowserWasm,
                    BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests,
                    "brokered Delta signed object URL",
                )?;

                Ok(BrowserHttpFileDescriptor {
                    path: file.path,
                    url: url.clone(),
                    size_bytes: file.size_bytes,
                    partition_values: file.partition_values,
                    stats: file.stats,
                    object_etag: None,
                })
            })
            .collect::<Result<Vec<_>, QueryError>>()?;

        Ok(BrowserHttpSnapshotDescriptor {
            table_uri: resolved_snapshot.table_uri,
            snapshot_version: resolved_snapshot.snapshot_version,
            partition_column_types: resolved_snapshot.partition_column_types,
            browser_compatibility: resolved_snapshot.browser_compatibility,
            required_capabilities: resolved_snapshot.required_capabilities,
            active_files,
        })
    }
}

fn quote_joined_paths<'a, I>(paths: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    paths
        .into_iter()
        .map(|path| format!("'{path}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokeredDeltaAccessMode {
    DeltaLog,
    PresignedFiles,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokeredPolicyAuthority {
    pub authority: PolicyAuthorityKind,
    pub direct_external_engine_read: DirectExternalEngineReadSupport,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAuthorityKind {
    UnityCatalog,
    DeltaSharing,
    MockBroker,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectExternalEngineReadSupport {
    Confirmed,
    NotConfirmed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokeredObjectAccess {
    pub list: bool,
    pub head: bool,
    pub get: bool,
    pub range_get: bool,
    pub batch_sign: bool,
    pub proxy_range: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeltaSharingReadPlan {
    pub table_id: String,
    pub full_name: String,
    pub sharing_endpoint: String,
    pub expires_at_epoch_ms: u64,
    pub files: Vec<BrowserHttpFileDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SqlFallbackRequiredPlan {
    pub table_id: String,
    pub full_name: String,
    pub reason: ReadAccessPlanReason,
    pub message: String,
    pub statement_endpoint: String,
    #[serde(
        deserialize_with = "deserialize_required_true",
        serialize_with = "serialize_required_true"
    )]
    #[schemars(schema_with = "required_true_json_schema")]
    pub warehouse_required: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockedReadPlan {
    pub table_id: String,
    pub full_name: String,
    pub reason: ReadAccessPlanReason,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadAccessPlanReason {
    RowFilter,
    ColumnMask,
    View,
    UnknownPolicyState,
    NoDirectExternalEngineReadSupport,
    UnsupportedTableType,
    GrantExpired,
    StorageCorsBlocked,
    BrokerUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantListRequest {
    pub prefix: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantListResponse {
    pub objects: Vec<ObjectGrantObject>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantHeadRequest {
    pub path: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantObject {
    pub path: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantBatchSignRequest {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantBatchSignResponse {
    pub signed_urls: Vec<ObjectGrantSignedUrl>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantSignedUrl {
    pub path: String,
    pub url: String,
    pub expires_at_epoch_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantRangeRequest {
    pub path: String,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectGrantAuditAction {
    List,
    Head,
    BatchSign,
    Range,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectGrantAuditOutcome {
    Allowed,
    Denied,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantAuditRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectGrantAuditEvent {
    pub event_id: String,
    pub event_type: String,
    pub occurred_at_epoch_ms: u64,
    pub tenant_id: String,
    pub workspace_id: String,
    pub user_subject: String,
    pub table_id: String,
    pub full_name: String,
    pub grant_id: String,
    pub query_id: String,
    pub request_id: String,
    pub correlation_id: String,
    pub action: ObjectGrantAuditAction,
    pub object_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<ObjectGrantAuditRange>,
    pub outcome: ObjectGrantAuditOutcome,
}

fn deserialize_required_true<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = bool::deserialize(deserializer)?;
    if value {
        return Ok(true);
    }

    Err(serde::de::Error::custom("warehouseRequired must be true"))
}

fn serialize_required_true<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value {
        return serializer.serialize_bool(true);
    }

    Err(serde::ser::Error::custom("warehouseRequired must be true"))
}

fn required_true_json_schema(
    generator: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    let mut schema: schemars::schema::SchemaObject = <bool>::json_schema(generator).into();
    schema.const_value = Some(serde_json::Value::Bool(true));
    schema.into()
}

fn capability_report_is_empty(report: &CapabilityReport) -> bool {
    report.capabilities.is_empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserObjectUrlPolicy {
    HttpsOnly,
    HttpsOrLoopbackHttpForHostTests,
    HttpsOrBrowserLocalBlob,
    HttpsOrLoopbackHttpForHostTestsOrBrowserLocalBlob,
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
        "blob" if allows_browser_local_blob(policy) => Ok(parsed),
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
        BrowserObjectUrlPolicy::HttpsOrBrowserLocalBlob => {
            "browser object URLs must use HTTPS or browser-local blob URLs"
        }
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTestsOrBrowserLocalBlob => {
            "browser object URLs must use HTTPS, browser-local blob URLs, or loopback HTTP for host-side tests"
        }
    }
}

fn allows_loopback_http_for_host_tests(url: &Url, policy: BrowserObjectUrlPolicy) -> bool {
    matches!(
        policy,
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests
            | BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTestsOrBrowserLocalBlob
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

fn allows_browser_local_blob(policy: BrowserObjectUrlPolicy) -> bool {
    matches!(
        policy,
        BrowserObjectUrlPolicy::HttpsOrBrowserLocalBlob
            | BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTestsOrBrowserLocalBlob
    )
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
    /// Prebootstrap pruning fail-open events observed before Parquet footer reads when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_fail_open_count: Option<u64>,
    /// Data files pruned before Parquet footer reads when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_files_pruned: Option<u64>,
    /// Parquet footer reads avoided by prebootstrap file pruning when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads_avoided: Option<u64>,
    /// Data files retained as prebootstrap candidates when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_candidate_files: Option<u64>,
    /// Parquet row groups decoded by the executed query plan when the runtime can report them.
    #[serde(default)]
    pub row_groups_touched: u64,
    /// Parquet row groups skipped by row-group pruning when the runtime can report them.
    #[serde(default)]
    pub row_groups_skipped: u64,
    /// Footer-range reads performed during browser snapshot bootstrap when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads: Option<u64>,
    /// Exact HTTP range reads used for Parquet trailer/footer bootstrap when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_footer_range_reads: Option<u64>,
    /// Exact HTTP range reads used by scan-time Parquet metadata loading when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_footer_range_reads: Option<u64>,
    /// Exact HTTP range reads used by scan-time Parquet data page loading when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_data_range_reads: Option<u64>,
    /// Exact duplicate HTTP range reads observed across the measured browser query path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_range_reads: Option<u64>,
    /// Physical HTTP range requests that served two or more logical Parquet ranges.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesced_range_reads: Option<u64>,
    /// Gap bytes fetched only because nearby logical Parquet ranges were coalesced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesced_gap_bytes_fetched: Option<u64>,
    /// Parquet footer metadata cache hits observed across bootstrap, inspect, or scan paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_hits: Option<u64>,
    /// Parquet footer metadata cache misses observed across bootstrap, inspect, or scan paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_misses: Option<u64>,
    /// Trailer/footer range reads avoided by Parquet footer metadata cache hits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_range_reads_avoided: Option<u64>,
    /// Footer cache lookups that had to use the conservative missing-identity path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_degraded_identity_reads: Option<u64>,
    /// Exact HTTP range reads that had stable object identity such as ETag plus size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_present_range_reads: Option<u64>,
    /// Exact HTTP range reads that ran without stable object identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_missing_range_reads: Option<u64>,
    /// Snapshot or descriptor resolution attempts performed before query execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_resolution_count: Option<u64>,
    /// Delta log manifest/list requests performed before query execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_log_manifest_list_count: Option<u64>,
    /// Wall-clock duration of Delta log manifest/list work when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_log_manifest_list_duration_ms: Option<u64>,
    /// Snapshot resolver invocations performed before query execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_resolve_count: Option<u64>,
    /// Wall-clock duration of snapshot resolver work when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_resolve_duration_ms: Option<u64>,
    /// In-memory descriptor handoff cache hits observed before query execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_cache_hit: Option<u64>,
    /// Prepared browser query session state reused for this execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_reuse_count: Option<u64>,
    /// Opened DataFusion table registrations reused for this execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opened_table_reuse_count: Option<u64>,
    /// Immutable object identity refreshes performed before reuse when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_refresh_count: Option<u64>,
    /// Access-envelope refreshes performed while preserving immutable state when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_envelope_refresh_count: Option<u64>,
    /// Rows emitted by the scan layer when the runtime can report them; otherwise `0`.
    #[serde(default)]
    pub rows_emitted: u64,
    /// Wall-clock duration of browser snapshot bootstrap when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_bootstrap_duration_ms: Option<u64>,
    /// Browser object access mode used for the execution when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<BrowserAccessMode>,
    /// Arrow IPC payload bytes produced for the query result when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc_bytes: Option<u64>,
    /// Arrow IPC chunks posted for the query result when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc_chunk_count: Option<u64>,
    /// Preview rows materialized from Arrow IPC when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_rows: Option<u64>,
    /// UTF-8 bytes used by string values in the materialized preview when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_string_bytes: Option<u64>,
    /// Wall-clock duration of SQL planning when the runtime can isolate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning_duration_ms: Option<u64>,
    /// Wall-clock duration of Arrow IPC encoding when the runtime can isolate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc_encode_duration_ms: Option<u64>,
    /// Wall-clock duration of preview construction when tracked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_duration_ms: Option<u64>,
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

pub const DAXIS_QUERY_RESULT_SCHEMA_VERSION: &str = "daxis.query_result.v1";
pub const DAXIS_APPROVED_AXON_READ_SCHEMA_VERSION: &str = "daxis.approved_axon_read.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisQueryStatus {
    Executed,
    Rejected,
    Fallback,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisExecutionEngine {
    AxonBrowser,
    RemoteWorker,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisResultTransport {
    ArrowIpc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisSurfaceKind {
    Agent,
    DashboardTile,
    Builder,
    SavedQuery,
    Api,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisIntentKind {
    Sql,
    SemanticQuery,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisInputArtifactKind {
    RawSql,
    SavedSql,
    BuilderPlan,
    SemanticPlan,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisCompiledArtifactKind {
    ValidatedSql,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisQueryFallbackReason {
    UnsupportedSql,
    RemoteRequired,
    EstimatesOverBudget,
    BrowserDisabled,
    DescriptorFailure,
    WorkerUnavailable,
    WorkerVersionMismatch,
    Cancellation,
    RuntimeBudgetOverflow,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisQueryBlockReason {
    MissingAccessPlan,
    PolicyDenied,
    NonReadOnlySql,
    UnvalidatedSql,
    InvalidCatalogReference,
    ExpiredDescriptor,
    UnsupportedSurface,
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisQueryMetrics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_returned: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_touched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_skipped: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_fail_open_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_files_pruned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_reads_avoided: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebootstrap_candidate_files: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups_touched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups_skipped: Option<u64>,
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
    pub snapshot_bootstrap_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<BrowserAccessMode>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisResultEnvelope {
    pub schema_version: String,
    pub status: DaxisQueryStatus,
    #[serde(default)]
    pub execution_engine: Option<DaxisExecutionEngine>,
    #[serde(default)]
    pub result_transport: Option<DaxisResultTransport>,
    #[serde(default)]
    pub fallback_reason: Option<DaxisQueryFallbackReason>,
    #[serde(default)]
    pub block_reason: Option<DaxisQueryBlockReason>,
    pub surface_kind: DaxisSurfaceKind,
    pub intent_kind: DaxisIntentKind,
    pub input_artifact_kind: DaxisInputArtifactKind,
    pub compiled_artifact_kind: DaxisCompiledArtifactKind,
    pub request_id: String,
    pub correlation_id: String,
    pub query_id: String,
    pub execution_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub metrics: DaxisQueryMetrics,
    #[serde(default)]
    pub diagnostics: BTreeMap<String, String>,
}

pub fn validate_daxis_result_envelope(
    envelope: &serde_json::Value,
) -> Result<DaxisResultEnvelope, QueryError> {
    let envelope: DaxisResultEnvelope =
        serde_json::from_value(envelope.clone()).map_err(|error| {
            daxis_contract_error(format!("invalid Daxis query result envelope: {error}"))
        })?;

    if envelope.schema_version != DAXIS_QUERY_RESULT_SCHEMA_VERSION {
        return Err(daxis_contract_error(format!(
            "Daxis query result schema_version must be {DAXIS_QUERY_RESULT_SCHEMA_VERSION}"
        )));
    }
    require_non_empty_daxis_field("request_id", &envelope.request_id)?;
    require_non_empty_daxis_field("correlation_id", &envelope.correlation_id)?;
    require_non_empty_daxis_field("query_id", &envelope.query_id)?;
    require_non_empty_daxis_field("execution_id", &envelope.execution_id)?;
    require_non_empty_daxis_field("workspace_id", &envelope.workspace_id)?;

    if envelope.fallback_reason.is_some() && envelope.block_reason.is_some() {
        return Err(daxis_contract_error(
            "Daxis fallback_reason and block_reason are mutually exclusive",
        ));
    }

    match envelope.status {
        DaxisQueryStatus::Executed => {
            if envelope.execution_engine.is_none() {
                return Err(daxis_contract_error(
                    "executed Daxis result envelope requires execution_engine",
                ));
            }
            if envelope.result_transport.is_none() {
                return Err(daxis_contract_error(
                    "executed Daxis result envelope requires result_transport",
                ));
            }
            if envelope.fallback_reason.is_some() || envelope.block_reason.is_some() {
                return Err(daxis_contract_error(
                    "executed Daxis result envelope must not include fallback or block reasons",
                ));
            }
        }
        DaxisQueryStatus::Rejected => {
            if envelope.block_reason.is_none() {
                return Err(daxis_contract_error(
                    "rejected Daxis result envelope requires block_reason",
                ));
            }
            if envelope.execution_engine.is_some() || envelope.result_transport.is_some() {
                return Err(daxis_contract_error(
                    "rejected Daxis result envelope must not include execution metadata",
                ));
            }
        }
        DaxisQueryStatus::Fallback => {
            if envelope.fallback_reason.is_none() {
                return Err(daxis_contract_error(
                    "fallback Daxis result envelope requires fallback_reason",
                ));
            }
            if envelope.result_transport.is_some() {
                return Err(daxis_contract_error(
                    "fallback Daxis result envelope must not include result_transport",
                ));
            }
        }
        DaxisQueryStatus::Failed | DaxisQueryStatus::Cancelled => {}
    }

    Ok(envelope)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisSqlDialect {
    DaxisSqlV1,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisValidatedSql {
    pub sql: String,
    pub dialect: DaxisSqlDialect,
    pub fingerprint: String,
    pub validation_id: String,
    pub read_only: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisApprovedTableDescriptor {
    pub catalog_id: String,
    pub table_id: String,
    pub descriptor_id: String,
    pub table_uri: String,
    pub full_name: String,
    pub snapshot_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_schema_hash: Option<String>,
    pub descriptor: BrowserHttpSnapshotDescriptor,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisReadAccessDecision {
    Approved,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisAccessProof {
    pub policy_decision_id: String,
    pub read_access_decision: DaxisReadAccessDecision,
    pub expires_at_epoch_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisRuntimeLimits {
    pub max_result_rows: u64,
    pub max_arrow_ipc_bytes: u64,
    pub max_scan_bytes: u64,
    pub timeout_ms: u64,
    pub cancellation_deadline_epoch_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaxisPreferredEngine {
    AxonBrowser,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisRuntimePreference {
    pub preferred_engine: DaxisPreferredEngine,
    pub allow_remote_fallback: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaxisApprovedAxonReadDescriptor {
    pub schema_version: String,
    pub request_id: String,
    pub correlation_id: String,
    pub query_id: String,
    pub execution_id: String,
    pub workspace_id: String,
    pub surface_kind: DaxisSurfaceKind,
    pub intent_kind: DaxisIntentKind,
    pub input_artifact_kind: DaxisInputArtifactKind,
    pub compiled_artifact_kind: DaxisCompiledArtifactKind,
    pub validated_sql: DaxisValidatedSql,
    pub tables: Vec<DaxisApprovedTableDescriptor>,
    pub access_proof: DaxisAccessProof,
    pub limits: DaxisRuntimeLimits,
    pub runtime_preference: DaxisRuntimePreference,
}

pub fn validate_daxis_approved_axon_read_descriptor(
    descriptor: &serde_json::Value,
    now_epoch_ms: u64,
) -> Result<DaxisApprovedAxonReadDescriptor, QueryError> {
    let descriptor: DaxisApprovedAxonReadDescriptor = serde_json::from_value(descriptor.clone())
        .map_err(|error| {
            daxis_contract_error(format!(
                "invalid Daxis-approved Axon read descriptor: {error}"
            ))
        })?;

    if descriptor.schema_version != DAXIS_APPROVED_AXON_READ_SCHEMA_VERSION {
        return Err(daxis_contract_error(format!(
            "Daxis-approved Axon read descriptor schema_version must be {DAXIS_APPROVED_AXON_READ_SCHEMA_VERSION}"
        )));
    }
    require_non_empty_daxis_field("request_id", &descriptor.request_id)?;
    require_non_empty_daxis_field("correlation_id", &descriptor.correlation_id)?;
    require_non_empty_daxis_field("query_id", &descriptor.query_id)?;
    require_non_empty_daxis_field("execution_id", &descriptor.execution_id)?;
    require_non_empty_daxis_field("workspace_id", &descriptor.workspace_id)?;
    validate_daxis_validated_sql(&descriptor.validated_sql)?;

    if descriptor.tables.is_empty() {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor requires at least one table descriptor",
        ));
    }
    if descriptor.tables.len() != 1 {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor v1 requires exactly one table descriptor",
        ));
    }
    for table in &descriptor.tables {
        validate_daxis_table_descriptor(table)?;
    }

    require_non_empty_daxis_field(
        "access_proof.policy_decision_id",
        &descriptor.access_proof.policy_decision_id,
    )?;
    if descriptor.access_proof.expires_at_epoch_ms <= now_epoch_ms {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor access proof has expired",
        ));
    }
    validate_daxis_runtime_limits(&descriptor.limits, now_epoch_ms)?;

    Ok(descriptor)
}

fn validate_daxis_validated_sql(sql: &DaxisValidatedSql) -> Result<(), QueryError> {
    require_non_empty_daxis_field("validated_sql.sql", &sql.sql)?;
    require_non_empty_daxis_field("validated_sql.fingerprint", &sql.fingerprint)?;
    require_non_empty_daxis_field("validated_sql.validation_id", &sql.validation_id)?;
    if !sql.read_only {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor requires validated_sql.read_only=true",
        ));
    }
    Ok(())
}

fn validate_daxis_table_descriptor(table: &DaxisApprovedTableDescriptor) -> Result<(), QueryError> {
    require_non_empty_daxis_field("tables[].catalog_id", &table.catalog_id)?;
    require_non_empty_daxis_field("tables[].table_id", &table.table_id)?;
    require_non_empty_daxis_field("tables[].descriptor_id", &table.descriptor_id)?;
    require_non_empty_daxis_field("tables[].table_uri", &table.table_uri)?;
    require_non_empty_daxis_field("tables[].full_name", &table.full_name)?;

    if table.snapshot_version < 0 {
        return Err(daxis_contract_error(
            "Daxis table descriptor snapshot_version must be non-negative",
        ));
    }
    if table.table_uri != table.descriptor.table_uri {
        return Err(daxis_contract_error(
            "Daxis table descriptor table_uri must match descriptor.table_uri",
        ));
    }
    if table.snapshot_version != table.descriptor.snapshot_version {
        return Err(daxis_contract_error(
            "Daxis table descriptor snapshot_version must match descriptor.snapshot_version",
        ));
    }
    if let (Some(schema_hash), Some(descriptor_schema_hash)) =
        (&table.schema_hash, &table.descriptor_schema_hash)
    {
        if schema_hash != descriptor_schema_hash {
            return Err(daxis_contract_error(
                "Daxis table descriptor schema_hash must match descriptor_schema_hash",
            ));
        }
    }
    for file in &table.descriptor.active_files {
        validate_browser_object_url(
            &file.url,
            ExecutionTarget::BrowserWasm,
            BrowserObjectUrlPolicy::HttpsOnly,
            "Daxis-approved Axon descriptor file URL",
        )?;
    }

    Ok(())
}

fn validate_daxis_runtime_limits(
    limits: &DaxisRuntimeLimits,
    now_epoch_ms: u64,
) -> Result<(), QueryError> {
    if limits.max_result_rows == 0 {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor max_result_rows must be positive",
        ));
    }
    if limits.max_arrow_ipc_bytes == 0 {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor max_arrow_ipc_bytes must be positive",
        ));
    }
    if limits.max_scan_bytes == 0 {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor max_scan_bytes must be positive",
        ));
    }
    if limits.timeout_ms == 0 {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor timeout_ms must be positive",
        ));
    }
    if limits.cancellation_deadline_epoch_ms <= now_epoch_ms {
        return Err(daxis_contract_error(
            "Daxis-approved Axon read descriptor cancellation deadline has expired",
        ));
    }

    Ok(())
}

fn require_non_empty_daxis_field(field: &str, value: &str) -> Result<(), QueryError> {
    if value.trim().is_empty() {
        return Err(daxis_contract_error(format!(
            "Daxis contract field {field} must not be empty"
        )));
    }

    Ok(())
}

fn daxis_contract_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::InvalidRequest,
        message,
        ExecutionTarget::BrowserWasm,
    )
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

#[cfg(test)]
mod partition_column_type_tests {
    use super::*;

    #[test]
    fn unsupported_partition_type_rejects_nulls() {
        assert_eq!(
            PartitionColumnType::Unsupported.normalize_partition_value(None),
            None
        );
        assert_eq!(
            PartitionColumnType::Unsupported
                .normalize_partition_literal(&PartitionLiteralValue::Null),
            None
        );
    }
}

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
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::DescriptorExpired,
            "resolved Delta descriptor has expired",
        ));
    }

    if response
        .requested_snapshot_version
        .is_some_and(|snapshot_version| snapshot_version < 0)
        || response.resolved_snapshot_version < 0
        || response.descriptor.snapshot_version < 0
    {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::InvalidSnapshotVersion,
            "resolved Delta snapshot versions must be non-negative integers",
        ));
    }

    if response.descriptor.snapshot_version != response.resolved_snapshot_version {
        return Err(DeltaLocationResolverError::new(
            DeltaLocationResolverErrorCode::PolicyBlocked,
            "resolved snapshot metadata did not match the descriptor snapshot version",
        ));
    }

    if let Some(refresh) = &response.refresh {
        if !refresh.same_snapshot_required {
            return Err(DeltaLocationResolverError::new(
                DeltaLocationResolverErrorCode::PolicyBlocked,
                "Delta location refresh must require the same resolved snapshot",
            ));
        }
        if let Some(refresh_url) = &refresh.refresh_url {
            validate_resolver_url(refresh_url, "refresh URL")?;
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
            DeltaLocationResolverError::new(
                DeltaLocationResolverErrorCode::PolicyBlocked,
                error.message,
            )
        })?;
    }

    validate_delta_table_uri(response.provider, &response.table_uri)
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

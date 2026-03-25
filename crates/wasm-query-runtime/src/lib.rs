//! Browser-safe runtime envelope with Parquet footer bootstrap for future DataFusion WASM
//! integration.
//!
//! The current in-repo surface validates browser-safe object sources, preserves a tiny generic
//! probe path above HTTP range reads, can materialize browser HTTP snapshot descriptors into
//! runtime-owned object sources, and can bootstrap bounded raw Parquet footer bytes without
//! starting DataFusion registration or browser SQL execution.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use query_contract::{
    validate_browser_object_url, BrowserHttpSnapshotDescriptor, BrowserObjectUrlPolicy,
    CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
};
use reqwest::Url;
use wasm_http_object_store::{HttpByteRange, HttpRangeReadResult, HttpRangeReader};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe query execution for the supported SQL envelope.";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const MAX_PARQUET_FOOTER_SIZE_BYTES: u64 = 16 * 1024 * 1024;
const PARQUET_TRAILER_SIZE_BYTES: u64 = 8;
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BrowserObjectAccessMode {
    #[default]
    BrowserSafeHttp,
    CloudObjectStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserRuntimeConfig {
    pub target_partitions: usize,
    pub object_access_mode: BrowserObjectAccessMode,
    pub allow_cloud_credentials: bool,
    /// Maximum time applied to each outbound HTTP request issued by the browser runtime.
    ///
    /// Multi-step operations such as Parquet footer bootstrap apply this timeout independently to
    /// the trailer read and the footer read; it is not a single end-to-end deadline.
    pub request_timeout_ms: u64,
}

impl Default for BrowserRuntimeConfig {
    fn default() -> Self {
        Self {
            target_partitions: 1,
            object_access_mode: BrowserObjectAccessMode::BrowserSafeHttp,
            allow_cloud_credentials: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
        }
    }
}

impl BrowserRuntimeConfig {
    pub fn validate(&self) -> Result<(), QueryError> {
        if self.request_timeout_ms == 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "browser runtime request timeout must be at least 1 ms",
                runtime_target(),
            ));
        }

        if self.allow_cloud_credentials {
            return Err(QueryError::new(
                QueryErrorCode::SecurityPolicyViolation,
                "browser runtime must not accept cloud credentials",
                runtime_target(),
            ));
        }

        if self.target_partitions != 1 {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                format!(
                    "browser runtime only supports single-partition execution; received {} partition(s)",
                    self.target_partitions
                ),
                runtime_target(),
            )
            .with_fallback_reason(FallbackReason::CapabilityGate {
                capability: CapabilityKey::MultiPartitionExecution,
                required_state: CapabilityState::NativeOnly,
            }));
        }

        if self.object_access_mode != BrowserObjectAccessMode::BrowserSafeHttp {
            return Err(QueryError::new(
                QueryErrorCode::FallbackRequired,
                "browser runtime only supports browser-safe HTTP object access",
                runtime_target(),
            )
            .with_fallback_reason(FallbackReason::BrowserRuntimeConstraint));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserObjectSource {
    url: Url,
}

impl BrowserObjectSource {
    pub fn from_url(url: impl Into<String>) -> Result<Self, QueryError> {
        let url = url.into();
        let url = validate_source_url(&url)?;

        Ok(Self { url })
    }

    pub fn url(&self) -> &str {
        self.url.as_str()
    }
}

/// Runtime-owned file metadata derived from a validated browser HTTP snapshot descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedBrowserFile {
    path: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    object_source: BrowserObjectSource,
}

impl MaterializedBrowserFile {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn partition_values(&self) -> &BTreeMap<String, Option<String>> {
        &self.partition_values
    }

    pub fn object_source(&self) -> &BrowserObjectSource {
        &self.object_source
    }
}

/// Runtime-owned snapshot metadata derived from a validated browser HTTP snapshot descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedBrowserSnapshot {
    table_uri: String,
    snapshot_version: i64,
    active_files: Vec<MaterializedBrowserFile>,
}

impl MaterializedBrowserSnapshot {
    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    pub fn snapshot_version(&self) -> i64 {
        self.snapshot_version
    }

    pub fn active_files(&self) -> &[MaterializedBrowserFile] {
        &self.active_files
    }
}

#[derive(Clone, Debug)]
pub struct BrowserRuntimeSession {
    config: BrowserRuntimeConfig,
    reader: HttpRangeReader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserParquetFooter {
    object_size_bytes: u64,
    footer_length_bytes: u32,
    footer_bytes: Bytes,
}

impl BrowserParquetFooter {
    /// Total size of the source object in bytes, as derived from the Parquet trailer read.
    pub fn object_size_bytes(&self) -> u64 {
        self.object_size_bytes
    }

    /// Footer length encoded in the Parquet trailer.
    pub fn footer_length_bytes(&self) -> u32 {
        self.footer_length_bytes
    }

    /// Raw footer bytes returned by the exact bounded footer range read.
    pub fn footer_bytes(&self) -> &[u8] {
        self.footer_bytes.as_ref()
    }
}

impl BrowserRuntimeSession {
    pub fn new(config: BrowserRuntimeConfig) -> Result<Self, QueryError> {
        Self::with_reader(config, HttpRangeReader::new())
    }

    pub fn with_reader(
        config: BrowserRuntimeConfig,
        reader: HttpRangeReader,
    ) -> Result<Self, QueryError> {
        config.validate()?;

        Ok(Self { config, reader })
    }

    pub fn config(&self) -> &BrowserRuntimeConfig {
        &self.config
    }

    /// Validates and materializes a shared browser HTTP snapshot descriptor without performing any
    /// network I/O.
    ///
    /// File order and metadata are preserved exactly, but duplicate paths and non-HTTPS browser
    /// object URLs are rejected to keep the runtime aligned with the browser-facing descriptor
    /// contract.
    pub fn materialize_snapshot(
        &self,
        descriptor: &BrowserHttpSnapshotDescriptor,
    ) -> Result<MaterializedBrowserSnapshot, QueryError> {
        validate_unique_descriptor_paths(&descriptor.active_files)?;

        let active_files = descriptor
            .active_files
            .iter()
            .map(|file| {
                let object_source = BrowserObjectSource {
                    url: validate_descriptor_source_url(&file.url)?,
                };

                Ok(MaterializedBrowserFile {
                    path: file.path.clone(),
                    size_bytes: file.size_bytes,
                    partition_values: file.partition_values.clone(),
                    object_source,
                })
            })
            .collect::<Result<Vec<_>, QueryError>>()?;

        Ok(MaterializedBrowserSnapshot {
            table_uri: descriptor.table_uri.clone(),
            snapshot_version: descriptor.snapshot_version,
            active_files,
        })
    }

    pub async fn probe(
        &self,
        source: &BrowserObjectSource,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.read_range(source, range).await
    }

    /// Bootstraps the raw Parquet footer by reading the trailing trailer and then the exact footer
    /// byte range.
    ///
    /// The browser runtime rejects oversized footer declarations and fails if the second range
    /// read no longer matches the object metadata returned by the trailer read.
    pub async fn read_parquet_footer(
        &self,
        source: &BrowserObjectSource,
    ) -> Result<BrowserParquetFooter, QueryError> {
        let trailer = self
            .read_range(
                source,
                HttpByteRange::Suffix {
                    length: PARQUET_TRAILER_SIZE_BYTES,
                },
            )
            .await?;
        let (object_size_bytes, footer_length_bytes) = parse_parquet_footer_trailer(&trailer)?;
        let footer_length_bytes_u64 = u64::from(footer_length_bytes);
        if footer_length_bytes_u64 > MAX_PARQUET_FOOTER_SIZE_BYTES {
            return Err(parquet_protocol_error(format!(
                "parquet footer bootstrap for '{}' declared a footer length of {} bytes, which exceeds the browser runtime cap of {} bytes",
                trailer.metadata.url,
                footer_length_bytes,
                MAX_PARQUET_FOOTER_SIZE_BYTES,
            )));
        }
        let footer_offset = object_size_bytes
            .checked_sub(PARQUET_TRAILER_SIZE_BYTES)
            .and_then(|offset| offset.checked_sub(footer_length_bytes_u64))
            .ok_or_else(|| {
                parquet_protocol_error(format!(
                    "parquet footer bootstrap for '{}' declared a footer length of {} bytes, which exceeds the object size of {} bytes",
                    trailer.metadata.url,
                    footer_length_bytes,
                    object_size_bytes,
                ))
            })?;
        let footer = self
            .read_range(
                source,
                HttpByteRange::Bounded {
                    offset: footer_offset,
                    length: footer_length_bytes_u64,
                },
            )
            .await?;
        validate_parquet_footer_read_consistency(&trailer, &footer, object_size_bytes)?;

        Ok(BrowserParquetFooter {
            object_size_bytes,
            footer_length_bytes,
            footer_bytes: footer.bytes,
        })
    }

    async fn read_range(
        &self,
        source: &BrowserObjectSource,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.reader
            .read_range_with_timeout(
                source.url(),
                range,
                Some(Duration::from_millis(self.config.request_timeout_ms)),
            )
            .await
    }
}

fn validate_source_url(url: &str) -> Result<Url, QueryError> {
    validate_browser_object_url(
        url,
        runtime_target(),
        BrowserObjectUrlPolicy::HttpsOrLoopbackHttpForHostTests,
        "browser object URL",
    )
}

fn validate_descriptor_source_url(url: &str) -> Result<Url, QueryError> {
    validate_browser_object_url(
        url,
        runtime_target(),
        BrowserObjectUrlPolicy::HttpsOnly,
        "browser HTTP object URL",
    )
}

fn validate_unique_descriptor_paths(
    active_files: &[query_contract::BrowserHttpFileDescriptor],
) -> Result<(), QueryError> {
    let mut paths = BTreeSet::new();
    let mut duplicate_paths = BTreeSet::new();

    for file in active_files {
        if !paths.insert(file.path.clone()) {
            duplicate_paths.insert(file.path.clone());
        }
    }

    if duplicate_paths.is_empty() {
        return Ok(());
    }

    Err(QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!(
            "browser HTTP snapshot descriptor contained duplicate paths [{}]",
            duplicate_paths
                .iter()
                .map(|path| format!("'{path}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        runtime_target(),
    ))
}

fn parse_parquet_footer_trailer(trailer: &HttpRangeReadResult) -> Result<(u64, u32), QueryError> {
    let object_size_bytes = trailer.metadata.size_bytes.ok_or_else(|| {
        parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' did not return object size metadata",
            trailer.metadata.url,
        ))
    })?;

    if object_size_bytes < PARQUET_TRAILER_SIZE_BYTES {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' requires at least {} bytes, but the object size was {} bytes",
            trailer.metadata.url,
            PARQUET_TRAILER_SIZE_BYTES,
            object_size_bytes,
        )));
    }

    if trailer.bytes.len() != PARQUET_TRAILER_SIZE_BYTES as usize {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' expected {} trailer bytes, but received {} bytes",
            trailer.metadata.url,
            PARQUET_TRAILER_SIZE_BYTES,
            trailer.bytes.len(),
        )));
    }

    if &trailer.bytes[4..8] != PARQUET_MAGIC {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' did not find trailing PAR1 magic",
            trailer.metadata.url,
        )));
    }

    let footer_length_bytes = u32::from_le_bytes(
        trailer.bytes[..4]
            .try_into()
            .expect("footer trailer length"),
    );
    if footer_length_bytes == 0 {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' requires a non-zero footer length",
            trailer.metadata.url,
        )));
    }

    let max_footer_length = object_size_bytes
        .checked_sub(PARQUET_TRAILER_SIZE_BYTES)
        .expect("objects smaller than the parquet trailer should already be rejected");
    if u64::from(footer_length_bytes) > max_footer_length {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' declared a footer length of {} bytes, which exceeds the available {} bytes before the trailer",
            trailer.metadata.url,
            footer_length_bytes,
            max_footer_length,
        )));
    }

    Ok((object_size_bytes, footer_length_bytes))
}

fn validate_parquet_footer_read_consistency(
    trailer: &HttpRangeReadResult,
    footer: &HttpRangeReadResult,
    expected_object_size_bytes: u64,
) -> Result<(), QueryError> {
    let footer_object_size_bytes = footer.metadata.size_bytes.ok_or_else(|| {
        parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' did not return object size metadata on the footer read",
            footer.metadata.url,
        ))
    })?;
    if footer_object_size_bytes != expected_object_size_bytes {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' observed object size {} bytes on the trailer read but {} bytes on the footer read",
            trailer.metadata.url,
            expected_object_size_bytes,
            footer_object_size_bytes,
        )));
    }

    match (&trailer.metadata.etag, &footer.metadata.etag) {
        (Some(trailer_etag), Some(footer_etag)) if trailer_etag != footer_etag => {
            Err(parquet_protocol_error(format!(
                "parquet footer bootstrap for '{}' observed entity tag {} on the trailer read but {} on the footer read",
                trailer.metadata.url,
                trailer_etag,
                footer_etag,
            )))
        }
        (Some(_), None) | (None, Some(_)) => Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' observed entity-tag metadata on only one of the two footer bootstrap reads",
            trailer.metadata.url,
        ))),
        _ => Ok(()),
    }
}

fn parquet_protocol_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ObjectStoreProtocol,
        message,
        runtime_target(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_object_source_stores_a_validated_parsed_url() {
        let source = BrowserObjectSource::from_url("https://example.com/object")
            .expect("https object sources should be supported");

        let parsed_url: &Url = &source.url;

        assert_eq!(parsed_url.as_str(), "https://example.com/object");
        assert_eq!(source.url(), parsed_url.as_str());
    }
}

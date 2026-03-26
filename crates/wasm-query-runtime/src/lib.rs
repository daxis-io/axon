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
use futures_timer::Delay;
use futures_util::{
    future::{select, Either},
    pin_mut,
    stream::{self, StreamExt, TryStreamExt},
};
use parquet::basic::ConvertedType;
use parquet::file::metadata::ParquetMetaDataReader;
use query_contract::{
    validate_browser_object_url, BrowserHttpSnapshotDescriptor, BrowserObjectUrlPolicy,
    CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
};
use reqwest::Url;
use wasm_http_object_store::{HttpByteRange, HttpRangeReadResult, HttpRangeReader};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe query execution for the supported SQL envelope.";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_SNAPSHOT_PREFLIGHT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_SNAPSHOT_PREFLIGHT_MAX_CONCURRENCY: usize = 4;
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
    /// Maximum wall-clock time allowed for full snapshot metadata preflight.
    pub snapshot_preflight_timeout_ms: u64,
    /// Maximum number of files whose metadata bootstrap may be in flight concurrently.
    pub snapshot_preflight_max_concurrency: usize,
}

impl Default for BrowserRuntimeConfig {
    fn default() -> Self {
        Self {
            target_partitions: 1,
            object_access_mode: BrowserObjectAccessMode::BrowserSafeHttp,
            allow_cloud_credentials: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            snapshot_preflight_timeout_ms: DEFAULT_SNAPSHOT_PREFLIGHT_TIMEOUT_MS,
            snapshot_preflight_max_concurrency: DEFAULT_SNAPSHOT_PREFLIGHT_MAX_CONCURRENCY,
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

        if self.snapshot_preflight_timeout_ms == 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "browser snapshot preflight timeout must be at least 1 ms",
                runtime_target(),
            ));
        }

        if self.snapshot_preflight_max_concurrency == 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "browser snapshot preflight concurrency must be at least 1 file",
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
    pub fn new(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
        object_source: BrowserObjectSource,
    ) -> Self {
        Self {
            path: path.into(),
            size_bytes,
            partition_values,
            object_source,
        }
    }

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
    pub fn new(
        table_uri: impl Into<String>,
        snapshot_version: i64,
        active_files: Vec<MaterializedBrowserFile>,
    ) -> Result<Self, QueryError> {
        validate_unique_materialized_paths(&active_files)?;

        Ok(Self {
            table_uri: table_uri.into(),
            snapshot_version,
            active_files,
        })
    }

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserParquetField {
    pub name: String,
    pub physical_type: String,
    pub logical_type: Option<String>,
    pub converted_type: Option<String>,
    pub repetition: String,
    pub nullable: bool,
    pub max_definition_level: i16,
    pub max_repetition_level: i16,
    pub type_length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserParquetFileMetadata {
    pub object_size_bytes: u64,
    pub footer_length_bytes: u32,
    pub row_group_count: u64,
    pub row_count: u64,
    pub fields: Vec<BrowserParquetField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrappedBrowserFile {
    pub path: String,
    pub size_bytes: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
    pub metadata: BrowserParquetFileMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrappedBrowserSnapshot {
    pub table_uri: String,
    pub snapshot_version: i64,
    pub active_files: Vec<BootstrappedBrowserFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSnapshotSchema {
    pub fields: Vec<BrowserParquetField>,
    pub partition_columns: Vec<String>,
    pub total_row_groups: u64,
    pub total_rows: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSnapshotPreflightSummary {
    pub table_uri: String,
    pub snapshot_version: i64,
    pub file_count: u64,
    pub total_bytes: u64,
    pub total_rows: u64,
    pub total_row_groups: u64,
    pub schema: BrowserSnapshotSchema,
}

impl BootstrappedBrowserSnapshot {
    pub fn validate_uniform_schema(&self) -> Result<BrowserSnapshotSchema, QueryError> {
        let expected_fields = self
            .active_files
            .first()
            .map(|file| file.metadata.fields.clone())
            .unwrap_or_default();
        let expected_partition_columns = self
            .active_files
            .first()
            .map(|file| file.partition_values.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut total_row_groups = 0_u64;
        let mut total_rows = 0_u64;
        let mut total_bytes = 0_u64;

        for file in &self.active_files {
            if file.metadata.fields != expected_fields {
                return Err(snapshot_validation_error(format!(
                    "bootstrapped browser snapshot contained a schema mismatch in file '{}': expected [{}], found [{}]",
                    file.path,
                    format_browser_fields(&expected_fields),
                    format_browser_fields(&file.metadata.fields),
                )));
            }
            let partition_columns = file.partition_values.keys().cloned().collect::<Vec<_>>();
            if partition_columns != expected_partition_columns {
                return Err(snapshot_validation_error(format!(
                    "bootstrapped browser snapshot contained partition-column mismatch in file '{}': expected [{}], found [{}]",
                    file.path,
                    format_partition_columns(&expected_partition_columns),
                    format_partition_columns(&partition_columns),
                )));
            }

            total_row_groups = total_row_groups
                .checked_add(file.metadata.row_group_count)
                .ok_or_else(|| {
                    snapshot_validation_error(
                        "bootstrapped browser snapshot row-group totals overflowed u64",
                    )
                })?;
            total_rows = total_rows
                .checked_add(file.metadata.row_count)
                .ok_or_else(|| {
                    snapshot_validation_error(
                        "bootstrapped browser snapshot row-count totals overflowed u64",
                    )
                })?;
            total_bytes = total_bytes.checked_add(file.size_bytes).ok_or_else(|| {
                snapshot_validation_error(
                    "bootstrapped browser snapshot byte totals overflowed u64",
                )
            })?;
        }

        Ok(BrowserSnapshotSchema {
            fields: expected_fields,
            partition_columns: expected_partition_columns,
            total_row_groups,
            total_rows,
            total_bytes,
        })
    }

    pub fn summarize(&self) -> Result<BrowserSnapshotPreflightSummary, QueryError> {
        let schema = self.validate_uniform_schema()?;
        let file_count = u64::try_from(self.active_files.len()).map_err(|_| {
            snapshot_validation_error("bootstrapped browser snapshot file counts overflowed u64")
        })?;

        Ok(BrowserSnapshotPreflightSummary {
            table_uri: self.table_uri.clone(),
            snapshot_version: self.snapshot_version,
            file_count,
            total_bytes: schema.total_bytes,
            total_rows: schema.total_rows,
            total_row_groups: schema.total_row_groups,
            schema,
        })
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

                Ok(MaterializedBrowserFile::new(
                    file.path.clone(),
                    file.size_bytes,
                    file.partition_values.clone(),
                    object_source,
                ))
            })
            .collect::<Result<Vec<_>, QueryError>>()?;

        MaterializedBrowserSnapshot::new(
            descriptor.table_uri.clone(),
            descriptor.snapshot_version,
            active_files,
        )
    }

    pub async fn probe(
        &self,
        source: &BrowserObjectSource,
        range: HttpByteRange,
    ) -> Result<HttpRangeReadResult, QueryError> {
        self.read_range(source, range).await
    }

    pub async fn read_parquet_footer_for_file(
        &self,
        file: &MaterializedBrowserFile,
    ) -> Result<BrowserParquetFooter, QueryError> {
        let footer = self.read_parquet_footer(file.object_source()).await?;
        if footer.object_size_bytes() != file.size_bytes() {
            return Err(parquet_protocol_error(format!(
                "parquet footer bootstrap for file '{}' observed object size {} bytes, but the descriptor declared {} bytes",
                file.path(),
                footer.object_size_bytes(),
                file.size_bytes(),
            )));
        }

        Ok(footer)
    }

    pub async fn read_parquet_metadata_for_file(
        &self,
        file: &MaterializedBrowserFile,
    ) -> Result<BrowserParquetFileMetadata, QueryError> {
        let footer = self.read_parquet_footer_for_file(file).await?;
        parse_parquet_metadata(file, &footer)
    }

    pub async fn bootstrap_snapshot_metadata(
        &self,
        snapshot: &MaterializedBrowserSnapshot,
    ) -> Result<BootstrappedBrowserSnapshot, QueryError> {
        let fetches = stream::iter(snapshot.active_files().iter())
            .map(|file| self.bootstrap_file_metadata(file))
            .buffered(self.config.snapshot_preflight_max_concurrency)
            .try_collect::<Vec<_>>();
        let timeout = Delay::new(Duration::from_millis(
            self.config.snapshot_preflight_timeout_ms,
        ));
        pin_mut!(fetches);
        pin_mut!(timeout);

        let active_files = match select(fetches, timeout).await {
            Either::Left((result, _)) => result?,
            Either::Right((_, _)) => {
                return Err(snapshot_preflight_timeout_error(
                    snapshot.table_uri(),
                    snapshot.snapshot_version(),
                    snapshot.active_files().len(),
                    self.config.snapshot_preflight_timeout_ms,
                ))
            }
        };

        Ok(BootstrappedBrowserSnapshot {
            table_uri: snapshot.table_uri().to_string(),
            snapshot_version: snapshot.snapshot_version(),
            active_files,
        })
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

    async fn bootstrap_file_metadata(
        &self,
        file: &MaterializedBrowserFile,
    ) -> Result<BootstrappedBrowserFile, QueryError> {
        let metadata = self.read_parquet_metadata_for_file(file).await?;

        Ok(BootstrappedBrowserFile {
            path: file.path().to_string(),
            size_bytes: file.size_bytes(),
            partition_values: file.partition_values().clone(),
            metadata,
        })
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
    validate_unique_paths(
        active_files.iter().map(|file| file.path.as_str()),
        "browser HTTP snapshot descriptor contained duplicate paths",
    )
}

fn validate_unique_materialized_paths(
    active_files: &[MaterializedBrowserFile],
) -> Result<(), QueryError> {
    validate_unique_paths(
        active_files.iter().map(|file| file.path()),
        "materialized browser snapshot contained duplicate paths",
    )
}

fn validate_unique_paths<'a>(
    paths: impl IntoIterator<Item = &'a str>,
    message_prefix: &str,
) -> Result<(), QueryError> {
    let mut seen_paths = BTreeSet::new();
    let mut duplicate_paths = BTreeSet::new();

    for path in paths {
        if !seen_paths.insert(path.to_string()) {
            duplicate_paths.insert(path.to_string());
        }
    }

    if duplicate_paths.is_empty() {
        return Ok(());
    }

    Err(QueryError::new(
        QueryErrorCode::InvalidRequest,
        format!(
            "{message_prefix} [{}]",
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

fn snapshot_validation_error(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::InvalidRequest, message, runtime_target())
}

fn snapshot_preflight_timeout_error(
    table_uri: &str,
    snapshot_version: i64,
    file_count: usize,
    timeout_ms: u64,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!(
            "browser snapshot preflight for '{}' at version {} did not finish within {} ms across {} file(s)",
            table_uri,
            snapshot_version,
            timeout_ms,
            file_count,
        ),
        runtime_target(),
    )
}

fn parse_parquet_metadata(
    file: &MaterializedBrowserFile,
    footer: &BrowserParquetFooter,
) -> Result<BrowserParquetFileMetadata, QueryError> {
    let metadata =
        ParquetMetaDataReader::decode_metadata(footer.footer_bytes()).map_err(|error| {
            parquet_protocol_error(format!(
                "parquet metadata decode for file '{}' failed: {error}",
                file.path(),
            ))
        })?;
    let file_metadata = metadata.file_metadata();
    let row_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        parquet_protocol_error(format!(
            "parquet metadata for file '{}' reported a negative row count",
            file.path(),
        ))
    })?;
    let row_group_count = u64::try_from(metadata.num_row_groups()).map_err(|_| {
        parquet_protocol_error(format!(
            "parquet metadata for file '{}' reported too many row groups to fit in u64",
            file.path(),
        ))
    })?;
    let fields = file_metadata
        .schema_descr()
        .columns()
        .iter()
        .map(|column| browser_parquet_field_from_descriptor(column.as_ref()))
        .collect::<Vec<_>>();

    Ok(BrowserParquetFileMetadata {
        object_size_bytes: footer.object_size_bytes(),
        footer_length_bytes: footer.footer_length_bytes(),
        row_group_count,
        row_count,
        fields,
    })
}

fn browser_parquet_field_from_descriptor(
    column: &parquet::schema::types::ColumnDescriptor,
) -> BrowserParquetField {
    let repetition = column.self_type().get_basic_info().repetition();
    let converted_type = match column.converted_type() {
        ConvertedType::NONE => None,
        converted_type => Some(converted_type.to_string()),
    };
    let logical_type = column
        .logical_type_ref()
        .map(|logical_type| format!("{logical_type:?}"));
    let type_length = matches!(
        column.physical_type(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
    )
    .then(|| column.type_length());
    let has_decimal_annotation = column.converted_type() == ConvertedType::DECIMAL
        || matches!(
            column.logical_type_ref(),
            Some(parquet::basic::LogicalType::Decimal { .. })
        );
    let precision = has_decimal_annotation.then(|| column.type_precision());
    let scale = has_decimal_annotation.then(|| column.type_scale());

    BrowserParquetField {
        name: column.path().string(),
        physical_type: format!("{:?}", column.physical_type()),
        logical_type,
        converted_type,
        repetition: repetition.to_string(),
        nullable: column.self_type().is_optional(),
        max_definition_level: column.max_def_level(),
        max_repetition_level: column.max_rep_level(),
        type_length,
        precision,
        scale,
    }
}

fn format_browser_fields(fields: &[BrowserParquetField]) -> String {
    if fields.is_empty() {
        return "<empty>".to_string();
    }

    fields
        .iter()
        .map(|field| {
            let logical_type = field
                .logical_type
                .as_deref()
                .unwrap_or("<none>");
            let converted_type = field
                .converted_type
                .as_deref()
                .unwrap_or("<none>");
            format!(
                "{}: {} logical={} converted={} repetition={} nullable={} def={} rep={} length={} precision={} scale={}",
                field.name,
                field.physical_type,
                logical_type,
                converted_type,
                field.repetition,
                field.nullable,
                field.max_definition_level,
                field.max_repetition_level,
                field.type_length.map(|value| value.to_string()).unwrap_or_else(|| "<none>".to_string()),
                field.precision.map(|value| value.to_string()).unwrap_or_else(|| "<none>".to_string()),
                field.scale.map(|value| value.to_string()).unwrap_or_else(|| "<none>".to_string()),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_partition_columns(partition_columns: &[String]) -> String {
    if partition_columns.is_empty() {
        return "<empty>".to_string();
    }

    partition_columns.join(", ")
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

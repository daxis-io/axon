//! Browser-side Parquet planning and scan primitives.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures_util::{
    future::BoxFuture,
    stream::{self, BoxStream},
    FutureExt, StreamExt, TryStreamExt,
};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::{
    Compression as RawParquetCompression, ConvertedType as RawParquetConvertedType,
    EdgeInterpolationAlgorithm as RawParquetEdgeInterpolationAlgorithm,
    LogicalType as RawParquetLogicalType, Repetition as RawParquetRepetition,
    TimeUnit as RawParquetTimeUnit, Type as RawParquetPhysicalType,
};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::file::{
    metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
    statistics::Statistics as ParquetStatistics,
};
use parquet::record::Field as ParquetField;
use query_contract::{
    ExecutionTarget, FallbackReason, ParquetCompressionSummary, ParquetInspectionColumn,
    ParquetInspectionColumnChunk, ParquetInspectionRowGroup, ParquetInspectionSummary,
    PartitionColumnType, QueryError, QueryErrorCode,
};
use wasm_http_object_store::{
    HttpByteRange, HttpMetadataProbeRequirements, HttpRangeReadResult, HttpRangeReader,
    HttpRangeValidation,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-side Parquet planning and scan primitives.";
pub const MAX_PARQUET_FOOTER_SIZE_BYTES: u64 = 16 * 1024 * 1024;
pub const PARQUET_TRAILER_SIZE_BYTES: u64 = 8;
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
pub const DEFAULT_STREAM_BATCH_SIZE: usize = 1024;

pub fn runtime_target() -> ExecutionTarget {
    ExecutionTarget::BrowserWasm
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectSource {
    pub url: String,
}

impl ObjectSource {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanTarget {
    pub object_source: ObjectSource,
    pub object_etag: Option<String>,
    pub path: String,
    pub size_bytes: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
}

impl ScanTarget {
    fn object_identity(&self) -> Option<ParquetObjectIdentity> {
        self.object_etag
            .as_deref()
            .and_then(strong_object_etag)
            .map(|etag| ParquetObjectIdentity::new(etag, self.size_bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetFooter {
    object_size_bytes: u64,
    object_etag: Option<String>,
    footer_length_bytes: u32,
    footer_bytes: Bytes,
}

impl ParquetFooter {
    pub fn object_size_bytes(&self) -> u64 {
        self.object_size_bytes
    }

    pub fn object_etag(&self) -> Option<&str> {
        self.object_etag.as_deref()
    }

    pub fn footer_length_bytes(&self) -> u32 {
        self.footer_length_bytes
    }

    pub fn footer_bytes(&self) -> &[u8] {
        self.footer_bytes.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetPhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetConvertedType {
    Utf8,
    Map,
    MapKeyValue,
    List,
    Enum,
    Decimal,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Json,
    Bson,
    Interval,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetTimeUnit {
    Millis,
    Micros,
    Nanos,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetEdgeInterpolationAlgorithm {
    Spherical,
    Vincenty,
    Thomas,
    Andoyer,
    Karney,
    Unknown(i32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetLogicalType {
    String,
    Map,
    List,
    Enum,
    Decimal {
        scale: i32,
        precision: i32,
    },
    Date,
    Time {
        is_adjusted_to_utc: bool,
        unit: ParquetTimeUnit,
    },
    Timestamp {
        is_adjusted_to_utc: bool,
        unit: ParquetTimeUnit,
    },
    Integer {
        bit_width: i8,
        is_signed: bool,
    },
    Unknown,
    Json,
    Bson,
    Uuid,
    Float16,
    Variant {
        specification_version: Option<i8>,
    },
    Geometry {
        crs: Option<String>,
    },
    Geography {
        crs: Option<String>,
        algorithm: Option<ParquetEdgeInterpolationAlgorithm>,
    },
    Unrecognized {
        field_id: i16,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetRepetition {
    Required,
    Optional,
    Repeated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetColumnField {
    pub name: String,
    pub physical_type: ParquetPhysicalType,
    pub logical_type: Option<ParquetLogicalType>,
    pub converted_type: Option<ParquetConvertedType>,
    pub repetition: ParquetRepetition,
    pub nullable: bool,
    pub max_definition_level: i16,
    pub max_repetition_level: i16,
    pub type_length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParquetFieldStats {
    pub min_i64: Option<i64>,
    pub max_i64: Option<i64>,
    pub null_count: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetFileMetadata {
    pub object_size_bytes: u64,
    pub footer_length_bytes: u32,
    pub row_group_count: u64,
    pub row_count: u64,
    pub compression_codecs: Vec<String>,
    pub fields: Vec<ParquetColumnField>,
    pub field_stats: BTreeMap<String, ParquetFieldStats>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    String(String),
}

pub type ParquetInputRow = BTreeMap<String, ParquetScalarValue>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParquetIntegerComparison {
    Eq(i64),
    Gt(i64),
    Gte(i64),
    Lt(i64),
    Lte(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetRowGroupPruningPredicate {
    pub column: String,
    pub comparison: ParquetIntegerComparison,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParquetRowGroupPlan {
    pub row_groups: Vec<usize>,
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanTargetMetricsSnapshot {
    pub files_touched: u64,
    pub files_skipped: u64,
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
    pub rows_emitted: u64,
    pub bytes_fetched: u64,
    pub footer_reads: u64,
    pub metadata_probe_round_trips: u64,
    pub range_read_metrics: ParquetRangeReadMetrics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetRangeReadPhase {
    BootstrapFooter,
    ScanFooter,
    ScanData,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ParquetObjectIdentity {
    pub object_etag: String,
    pub size_bytes: u64,
}

impl ParquetObjectIdentity {
    pub fn new(object_etag: impl Into<String>, size_bytes: u64) -> Self {
        Self {
            object_etag: object_etag.into(),
            size_bytes,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetRangeReadKey {
    pub resource: String,
    pub object_identity: Option<ParquetObjectIdentity>,
    pub offset: u64,
    pub length: u64,
}

impl ParquetRangeReadKey {
    pub fn new(resource: impl Into<String>, offset: u64, length: u64) -> Self {
        Self::new_with_identity(resource, None, offset, length)
    }

    pub fn new_with_identity(
        resource: impl Into<String>,
        object_identity: Option<ParquetObjectIdentity>,
        offset: u64,
        length: u64,
    ) -> Self {
        Self {
            resource: resource.into(),
            object_identity,
            offset,
            length,
        }
    }
}

// Duplicate range metrics are keyed by logical byte range. Object identity is
// reported as a separate dimension so preserving ETags cannot hide duplicate
// trailer/footer reads before a shared footer cache exists.
impl PartialEq for ParquetRangeReadKey {
    fn eq(&self, other: &Self) -> bool {
        self.resource == other.resource
            && self.offset == other.offset
            && self.length == other.length
    }
}

impl Eq for ParquetRangeReadKey {}

impl PartialOrd for ParquetRangeReadKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ParquetRangeReadKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.resource
            .cmp(&other.resource)
            .then_with(|| self.offset.cmp(&other.offset))
            .then_with(|| self.length.cmp(&other.length))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParquetRangeReadMetrics {
    pub bootstrap_footer_range_reads: u64,
    pub scan_footer_range_reads: u64,
    pub scan_data_range_reads: u64,
    pub duplicate_range_reads: u64,
    pub identity_present_range_reads: u64,
    pub identity_missing_range_reads: u64,
    pub footer_cache_hits: u64,
    pub footer_cache_misses: u64,
    pub footer_range_reads_avoided: u64,
    pub footer_cache_degraded_identity_reads: u64,
    pub exact_range_read_keys: BTreeSet<ParquetRangeReadKey>,
}

impl ParquetRangeReadMetrics {
    pub fn record(&mut self, phase: ParquetRangeReadPhase, key: ParquetRangeReadKey) {
        if key.object_identity.is_some() {
            self.identity_present_range_reads = self.identity_present_range_reads.saturating_add(1);
        } else {
            self.identity_missing_range_reads = self.identity_missing_range_reads.saturating_add(1);
        }
        if !self.exact_range_read_keys.insert(key) {
            self.duplicate_range_reads = self.duplicate_range_reads.saturating_add(1);
        }
        match phase {
            ParquetRangeReadPhase::BootstrapFooter => {
                self.bootstrap_footer_range_reads =
                    self.bootstrap_footer_range_reads.saturating_add(1);
            }
            ParquetRangeReadPhase::ScanFooter => {
                self.scan_footer_range_reads = self.scan_footer_range_reads.saturating_add(1);
            }
            ParquetRangeReadPhase::ScanData => {
                self.scan_data_range_reads = self.scan_data_range_reads.saturating_add(1);
            }
        }
    }

    pub fn record_footer_cache_hit(&mut self, range_reads_avoided: u64) {
        self.footer_cache_hits = self.footer_cache_hits.saturating_add(1);
        self.footer_range_reads_avoided = self
            .footer_range_reads_avoided
            .saturating_add(range_reads_avoided);
    }

    pub fn record_footer_cache_miss(&mut self) {
        self.footer_cache_misses = self.footer_cache_misses.saturating_add(1);
    }

    pub fn record_footer_cache_degraded_identity_read(&mut self) {
        self.footer_cache_degraded_identity_reads =
            self.footer_cache_degraded_identity_reads.saturating_add(1);
    }

    pub fn merge(&self, right: &Self) -> Self {
        merge_parquet_range_read_metrics(self, right)
    }
}

pub fn merge_parquet_range_read_metrics(
    left: &ParquetRangeReadMetrics,
    right: &ParquetRangeReadMetrics,
) -> ParquetRangeReadMetrics {
    let mut merged = ParquetRangeReadMetrics {
        bootstrap_footer_range_reads: left
            .bootstrap_footer_range_reads
            .saturating_add(right.bootstrap_footer_range_reads),
        scan_footer_range_reads: left
            .scan_footer_range_reads
            .saturating_add(right.scan_footer_range_reads),
        scan_data_range_reads: left
            .scan_data_range_reads
            .saturating_add(right.scan_data_range_reads),
        duplicate_range_reads: left
            .duplicate_range_reads
            .saturating_add(right.duplicate_range_reads),
        identity_present_range_reads: left
            .identity_present_range_reads
            .saturating_add(right.identity_present_range_reads),
        identity_missing_range_reads: left
            .identity_missing_range_reads
            .saturating_add(right.identity_missing_range_reads),
        footer_cache_hits: left
            .footer_cache_hits
            .saturating_add(right.footer_cache_hits),
        footer_cache_misses: left
            .footer_cache_misses
            .saturating_add(right.footer_cache_misses),
        footer_range_reads_avoided: left
            .footer_range_reads_avoided
            .saturating_add(right.footer_range_reads_avoided),
        footer_cache_degraded_identity_reads: left
            .footer_cache_degraded_identity_reads
            .saturating_add(right.footer_cache_degraded_identity_reads),
        exact_range_read_keys: left.exact_range_read_keys.clone(),
    };
    for key in &right.exact_range_read_keys {
        if !merged.exact_range_read_keys.insert(key.clone()) {
            merged.duplicate_range_reads = merged.duplicate_range_reads.saturating_add(1);
        }
    }
    merged
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParquetMetadataCacheMetrics {
    pub footer_cache_hits: u64,
    pub footer_cache_misses: u64,
    pub footer_range_reads_avoided: u64,
    pub footer_cache_degraded_identity_reads: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ParquetMetadataCacheKey {
    pub resource: String,
    pub object_identity: ParquetObjectIdentity,
}

impl ParquetMetadataCacheKey {
    pub fn new(resource: impl Into<String>, object_identity: ParquetObjectIdentity) -> Self {
        Self {
            resource: resource.into(),
            object_identity,
        }
    }
}

#[derive(Clone, Debug)]
struct ParquetCachedMetadata {
    footer: ParquetFooter,
    metadata: Option<Arc<ParquetMetaData>>,
}

#[derive(Clone, Debug, Default)]
pub struct ParquetMetadataCache {
    entries: Arc<Mutex<BTreeMap<ParquetMetadataCacheKey, ParquetCachedMetadata>>>,
    metrics: Arc<Mutex<ParquetMetadataCacheMetrics>>,
}

impl ParquetMetadataCache {
    fn get(
        &self,
        key: &ParquetMetadataCacheKey,
    ) -> Result<Option<ParquetCachedMetadata>, QueryError> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| execution_runtime_error("parquet metadata cache was poisoned"))?
            .get(key)
            .cloned())
    }

    fn insert(
        &self,
        key: ParquetMetadataCacheKey,
        cached: ParquetCachedMetadata,
    ) -> Result<(), QueryError> {
        self.entries
            .lock()
            .map_err(|_| execution_runtime_error("parquet metadata cache was poisoned"))?
            .insert(key, cached);
        Ok(())
    }

    fn record_hit(&self, range_reads_avoided: u64) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet metadata cache metrics were poisoned"))?;
        metrics.footer_cache_hits = metrics.footer_cache_hits.saturating_add(1);
        metrics.footer_range_reads_avoided = metrics
            .footer_range_reads_avoided
            .saturating_add(range_reads_avoided);
        Ok(())
    }

    fn record_miss(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet metadata cache metrics were poisoned"))?;
        metrics.footer_cache_misses = metrics.footer_cache_misses.saturating_add(1);
        Ok(())
    }

    fn record_degraded_identity_read(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet metadata cache metrics were poisoned"))?;
        metrics.footer_cache_degraded_identity_reads = metrics
            .footer_cache_degraded_identity_reads
            .saturating_add(1);
        Ok(())
    }

    pub fn snapshot(&self) -> ParquetMetadataCacheMetrics {
        self.metrics
            .lock()
            .expect("parquet metadata cache metrics should not be poisoned")
            .clone()
    }
}

fn cache_key_for_target_identity(target: &ScanTarget) -> Option<ParquetMetadataCacheKey> {
    target.object_identity().map(|identity| {
        ParquetMetadataCacheKey::new(canonical_parquet_cache_resource(target), identity)
    })
}

fn cache_key_for_target_footer(
    target: &ScanTarget,
    footer: &ParquetFooter,
) -> Option<ParquetMetadataCacheKey> {
    if footer.object_size_bytes() != target.size_bytes {
        return None;
    }
    footer
        .object_etag()
        .and_then(strong_object_etag)
        .map(|etag| {
            ParquetMetadataCacheKey::new(
                canonical_parquet_cache_resource(target),
                ParquetObjectIdentity::new(etag, footer.object_size_bytes()),
            )
        })
}

fn canonical_parquet_cache_resource(target: &ScanTarget) -> String {
    let redacted_url_end = target
        .object_source
        .url
        .find(['?', '#'])
        .unwrap_or(target.object_source.url.len());
    format!(
        "{}|{}",
        &target.object_source.url[..redacted_url_end],
        target.path
    )
}

async fn validate_cached_metadata_identity(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
) -> Result<(), QueryError> {
    let expected = target.object_identity().ok_or_else(|| {
        parquet_protocol_error(format!(
            "cached parquet metadata for '{}' requires strong object identity",
            target.path
        ))
    })?;
    let observed = reader
        .probe_metadata_with_timeout(
            &target.object_source.url,
            HttpMetadataProbeRequirements {
                require_size: true,
                require_etag: true,
            },
            request_timeout,
        )
        .await?;
    let observed_size = observed.size_bytes.ok_or_else(|| {
        parquet_protocol_error(format!(
            "cached parquet metadata identity validation for '{}' required object size metadata",
            target.path
        ))
    })?;
    if observed_size != expected.size_bytes {
        return Err(parquet_protocol_error(format!(
            "cached parquet metadata identity validation for '{}' expected object size {} bytes, but the current object reported {} bytes",
            target.path, expected.size_bytes, observed_size
        )));
    }
    let observed_etag = observed.etag.as_deref().and_then(strong_object_etag).ok_or_else(|| {
        parquet_protocol_error(format!(
            "cached parquet metadata identity validation for '{}' required a strong ETag, but the current object did not expose one",
            target.path
        ))
    })?;
    if observed_etag != expected.object_etag {
        return Err(parquet_protocol_error(format!(
            "cached parquet metadata identity validation for '{}' expected ETag {}, but the current object reported {}",
            target.path, expected.object_etag, observed_etag
        )));
    }
    Ok(())
}

fn record_footer_cache_hit(
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
    range_reads_avoided: u64,
) -> Result<(), QueryError> {
    if let Some(cache) = cache {
        cache.record_hit(range_reads_avoided)?;
    }
    if let Some(metrics) = metrics {
        metrics.record_footer_cache_hit(range_reads_avoided);
    }
    Ok(())
}

fn record_footer_cache_miss(
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<(), QueryError> {
    if let Some(cache) = cache {
        cache.record_miss()?;
    }
    if let Some(metrics) = metrics {
        metrics.record_footer_cache_miss();
    }
    Ok(())
}

fn record_footer_cache_degraded_identity_read(
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<(), QueryError> {
    if let Some(cache) = cache {
        cache.record_degraded_identity_read()?;
    }
    if let Some(metrics) = metrics {
        metrics.record_footer_cache_degraded_identity_read();
    }
    Ok(())
}

pub fn bootstrap_footer_range_read_metrics_for_target(
    target: &ScanTarget,
    footer_length_bytes: u32,
) -> Result<ParquetRangeReadMetrics, QueryError> {
    let footer_length_bytes = u64::from(footer_length_bytes);
    let trailer_offset = target
        .size_bytes
        .checked_sub(PARQUET_TRAILER_SIZE_BYTES)
        .ok_or_else(|| {
            parquet_protocol_error(format!(
                "parquet footer bootstrap for file '{}' requires at least {} bytes, but the descriptor declared {} bytes",
                target.path, PARQUET_TRAILER_SIZE_BYTES, target.size_bytes
            ))
        })?;
    let footer_offset = trailer_offset
        .checked_sub(footer_length_bytes)
        .ok_or_else(|| {
            parquet_protocol_error(format!(
                "parquet footer bootstrap for file '{}' declared a footer length of {} bytes, which exceeds the object size of {} bytes",
                target.path, footer_length_bytes, target.size_bytes
            ))
        })?;
    let mut metrics = ParquetRangeReadMetrics::default();
    metrics.record(
        ParquetRangeReadPhase::BootstrapFooter,
        ParquetRangeReadKey::new_with_identity(
            target.path.clone(),
            target.object_identity(),
            trailer_offset,
            PARQUET_TRAILER_SIZE_BYTES,
        ),
    );
    metrics.record(
        ParquetRangeReadPhase::BootstrapFooter,
        ParquetRangeReadKey::new_with_identity(
            target.path.clone(),
            target.object_identity(),
            footer_offset,
            footer_length_bytes,
        ),
    );
    Ok(metrics)
}

pub trait ScanTargetMetricsHandle {
    fn snapshot(&self) -> ScanTargetMetricsSnapshot;
}

pub struct ScanTargetBatchStream<S> {
    pub batches: S,
    pub metrics: Arc<dyn ScanTargetMetricsHandle + Send + Sync>,
}

#[derive(Clone, Default)]
struct SharedScanTargetMetricsHandle {
    snapshot: Arc<Mutex<ScanTargetMetricsSnapshot>>,
}

impl SharedScanTargetMetricsHandle {
    fn new(snapshot: ScanTargetMetricsSnapshot) -> Self {
        Self {
            snapshot: Arc::new(Mutex::new(snapshot)),
        }
    }

    fn record_rows_emitted(&self, rows_emitted: u64) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot.rows_emitted = snapshot
            .rows_emitted
            .checked_add(rows_emitted)
            .ok_or_else(|| execution_runtime_error("scan target metrics row counts overflowed"))?;
        Ok(())
    }

    fn record_bytes_fetched(&self, bytes_fetched: u64) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot.bytes_fetched = snapshot
            .bytes_fetched
            .checked_add(bytes_fetched)
            .ok_or_else(|| execution_runtime_error("scan byte totals overflowed u64"))?;
        Ok(())
    }

    fn set_metadata_probe_round_trips(&self, round_trips: u64) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot.metadata_probe_round_trips = round_trips;
        Ok(())
    }

    fn record_range_read(
        &self,
        phase: ParquetRangeReadPhase,
        resource: String,
        object_identity: Option<ParquetObjectIdentity>,
        offset: u64,
        length: u64,
    ) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot.range_read_metrics.record(
            phase,
            ParquetRangeReadKey::new_with_identity(resource, object_identity, offset, length),
        );
        Ok(())
    }

    fn record_footer_cache_hit(&self, range_reads_avoided: u64) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot
            .range_read_metrics
            .record_footer_cache_hit(range_reads_avoided);
        Ok(())
    }

    fn record_footer_cache_miss(&self) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot.range_read_metrics.record_footer_cache_miss();
        Ok(())
    }

    fn record_footer_cache_degraded_identity_read(&self) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot
            .range_read_metrics
            .record_footer_cache_degraded_identity_read();
        Ok(())
    }
}

impl ScanTargetMetricsHandle for SharedScanTargetMetricsHandle {
    fn snapshot(&self) -> ScanTargetMetricsSnapshot {
        self.snapshot
            .lock()
            .expect("scan target metrics handle should not be poisoned")
            .clone()
    }
}

#[derive(Clone, Default)]
struct AggregateScanTargetMetricsHandle {
    children: Arc<Mutex<Vec<Arc<dyn ScanTargetMetricsHandle + Send + Sync>>>>,
}

impl AggregateScanTargetMetricsHandle {
    fn add_child(
        &self,
        child: Arc<dyn ScanTargetMetricsHandle + Send + Sync>,
    ) -> Result<(), QueryError> {
        self.children
            .lock()
            .map_err(|_| execution_runtime_error("aggregate scan metrics handle was poisoned"))?
            .push(child);
        Ok(())
    }
}

impl ScanTargetMetricsHandle for AggregateScanTargetMetricsHandle {
    fn snapshot(&self) -> ScanTargetMetricsSnapshot {
        self.children
            .lock()
            .expect("aggregate scan metrics handle should not be poisoned")
            .iter()
            .map(|child| child.snapshot())
            .fold(
                ScanTargetMetricsSnapshot::default(),
                |mut aggregate, snapshot| {
                    aggregate.files_touched = aggregate
                        .files_touched
                        .saturating_add(snapshot.files_touched);
                    aggregate.files_skipped = aggregate
                        .files_skipped
                        .saturating_add(snapshot.files_skipped);
                    aggregate.row_groups_touched = aggregate
                        .row_groups_touched
                        .saturating_add(snapshot.row_groups_touched);
                    aggregate.row_groups_skipped = aggregate
                        .row_groups_skipped
                        .saturating_add(snapshot.row_groups_skipped);
                    aggregate.rows_emitted =
                        aggregate.rows_emitted.saturating_add(snapshot.rows_emitted);
                    aggregate.bytes_fetched = aggregate
                        .bytes_fetched
                        .saturating_add(snapshot.bytes_fetched);
                    aggregate.footer_reads =
                        aggregate.footer_reads.saturating_add(snapshot.footer_reads);
                    aggregate.metadata_probe_round_trips = aggregate
                        .metadata_probe_round_trips
                        .saturating_add(snapshot.metadata_probe_round_trips);
                    aggregate.range_read_metrics = merge_parquet_range_read_metrics(
                        &aggregate.range_read_metrics,
                        &snapshot.range_read_metrics,
                    );
                    aggregate
                },
            )
    }
}

#[derive(Debug)]
struct WrappedQueryError(QueryError);

impl std::fmt::Display for WrappedQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl StdError for WrappedQueryError {}

fn parquet_error_from_query_error(error: QueryError) -> ParquetError {
    ParquetError::External(Box::new(WrappedQueryError(error)))
}

fn query_error_from_parquet_error(
    error: ParquetError,
    context: impl FnOnce(&str) -> String,
) -> QueryError {
    match error {
        ParquetError::External(external) => match external.downcast::<WrappedQueryError>() {
            Ok(wrapped) => wrapped.0,
            Err(external) => execution_runtime_error(context(&external.to_string())),
        },
        other => execution_runtime_error(context(&other.to_string())),
    }
}

#[derive(Clone)]
struct HttpRangeAsyncFileReader {
    reader: HttpRangeReader,
    target: ScanTarget,
    request_timeout: Option<Duration>,
    metadata_cache: Option<ParquetMetadataCache>,
    metrics: SharedScanTargetMetricsHandle,
    count_metadata_fetches: bool,
    metadata_probe_round_trips: u64,
}

impl HttpRangeAsyncFileReader {
    fn new(
        reader: HttpRangeReader,
        target: ScanTarget,
        request_timeout: Option<Duration>,
        metadata_cache: Option<ParquetMetadataCache>,
        metrics: SharedScanTargetMetricsHandle,
    ) -> Self {
        Self {
            reader,
            target,
            request_timeout,
            metadata_cache,
            metrics,
            count_metadata_fetches: false,
            metadata_probe_round_trips: 0,
        }
    }

    fn validation(&self) -> Option<HttpRangeValidation> {
        self.target
            .object_etag
            .as_deref()
            .and_then(strong_object_etag)
            .map(HttpRangeValidation::if_range_etag)
    }

    async fn fetch_range_owned(
        reader: HttpRangeReader,
        object_url: String,
        validation: Option<HttpRangeValidation>,
        request_timeout: Option<Duration>,
        metrics: SharedScanTargetMetricsHandle,
        phase: ParquetRangeReadPhase,
        metric_resource: String,
        object_identity: Option<ParquetObjectIdentity>,
        range: Range<u64>,
    ) -> Result<Bytes, QueryError> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| execution_runtime_error("parquet byte range underflowed u64"))?;
        if length == 0 {
            return Ok(Bytes::new());
        }

        let read = reader
            .read_range_with_validation(
                &object_url,
                HttpByteRange::Bounded {
                    offset: range.start,
                    length,
                },
                validation,
                request_timeout,
            )
            .await?;
        metrics.record_bytes_fetched(
            u64::try_from(read.bytes.len())
                .map_err(|_| execution_runtime_error("scan byte totals overflowed u64"))?,
        )?;
        metrics.record_range_read(phase, metric_resource, object_identity, range.start, length)?;
        Ok(read.bytes)
    }

    async fn load_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> parquet::errors::Result<Arc<ParquetMetaData>> {
        if let Some(cache) = &self.metadata_cache {
            match cache_key_for_target_identity(&self.target) {
                Some(key) => {
                    if let Some(cached) = cache.get(&key).map_err(parquet_error_from_query_error)? {
                        validate_cached_metadata_identity(
                            &self.reader,
                            &self.target,
                            self.request_timeout,
                        )
                        .await
                        .map_err(parquet_error_from_query_error)?;
                        cache
                            .record_hit(2)
                            .map_err(parquet_error_from_query_error)?;
                        self.metrics
                            .record_footer_cache_hit(2)
                            .map_err(parquet_error_from_query_error)?;
                        self.metrics
                            .set_metadata_probe_round_trips(1)
                            .map_err(parquet_error_from_query_error)?;
                        let metadata = match cached.metadata {
                            Some(metadata) => metadata,
                            None => {
                                let metadata = Arc::new(
                                    decode_parquet_metadata(
                                        &self.target,
                                        &cached.footer,
                                        "parquet metadata decode",
                                    )
                                    .map_err(parquet_error_from_query_error)?,
                                );
                                cache
                                    .insert(
                                        key,
                                        ParquetCachedMetadata {
                                            footer: cached.footer,
                                            metadata: Some(Arc::clone(&metadata)),
                                        },
                                    )
                                    .map_err(parquet_error_from_query_error)?;
                                metadata
                            }
                        };
                        return Ok(metadata);
                    }
                    cache
                        .record_miss()
                        .map_err(parquet_error_from_query_error)?;
                    self.metrics
                        .record_footer_cache_miss()
                        .map_err(parquet_error_from_query_error)?;
                }
                None => {
                    cache
                        .record_miss()
                        .map_err(parquet_error_from_query_error)?;
                    cache
                        .record_degraded_identity_read()
                        .map_err(parquet_error_from_query_error)?;
                    self.metrics
                        .record_footer_cache_miss()
                        .map_err(parquet_error_from_query_error)?;
                    self.metrics
                        .record_footer_cache_degraded_identity_read()
                        .map_err(parquet_error_from_query_error)?;
                }
            }
        }
        self.count_metadata_fetches = true;
        self.metadata_probe_round_trips = 0;
        let file_size = self.target.size_bytes;
        let metadata_options = options.map(|option| option.metadata_options().clone());
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::from(
                options.is_some_and(|option| option.page_index()),
            ))
            .with_metadata_options(metadata_options)
            .load_and_finish(&mut *self, file_size)
            .await;
        self.count_metadata_fetches = false;
        let metadata = metadata?;
        self.metrics
            .set_metadata_probe_round_trips(self.metadata_probe_round_trips)
            .map_err(parquet_error_from_query_error)?;
        Ok(Arc::new(metadata))
    }
}

impl AsyncFileReader for HttpRangeAsyncFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        #[cfg(target_arch = "wasm32")]
        {
            if self.count_metadata_fetches {
                self.metadata_probe_round_trips = self
                    .metadata_probe_round_trips
                    .checked_add(1)
                    .expect("metadata probe round trips should not overflow");
            }
            let reader = self.reader.clone();
            let object_url = self.target.object_source.url.clone();
            let metric_resource = self.target.path.clone();
            let object_identity = self.target.object_identity();
            let validation = self.validation();
            let request_timeout = self.request_timeout;
            let metrics = self.metrics.clone();
            let phase = if self.count_metadata_fetches {
                ParquetRangeReadPhase::ScanFooter
            } else {
                ParquetRangeReadPhase::ScanData
            };
            let (future, handle) = async move {
                Self::fetch_range_owned(
                    reader,
                    object_url,
                    validation,
                    request_timeout,
                    metrics,
                    phase,
                    metric_resource,
                    object_identity,
                    range,
                )
                .await
                .map_err(parquet_error_from_query_error)
            }
            .remote_handle();
            wasm_bindgen_futures::spawn_local(future);
            return async move { handle.await }.boxed();
        }

        #[cfg(not(target_arch = "wasm32"))]
        async move {
            if self.count_metadata_fetches {
                self.metadata_probe_round_trips = self
                    .metadata_probe_round_trips
                    .checked_add(1)
                    .expect("metadata probe round trips should not overflow");
            }
            let phase = if self.count_metadata_fetches {
                ParquetRangeReadPhase::ScanFooter
            } else {
                ParquetRangeReadPhase::ScanData
            };
            Self::fetch_range_owned(
                self.reader.clone(),
                self.target.object_source.url.clone(),
                self.validation(),
                self.request_timeout,
                self.metrics.clone(),
                phase,
                self.target.path.clone(),
                self.target.object_identity(),
                range,
            )
            .await
            .map_err(parquet_error_from_query_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        #[cfg(target_arch = "wasm32")]
        {
            let mut reader = self.clone();
            let options = options.cloned();
            let (future, handle) =
                async move { reader.load_metadata(options.as_ref()).await }.remote_handle();
            wasm_bindgen_futures::spawn_local(future);
            return async move { handle.await }.boxed();
        }

        #[cfg(not(target_arch = "wasm32"))]
        async move { self.load_metadata(options).await }.boxed()
    }
}

pub async fn read_parquet_footer(
    reader: &HttpRangeReader,
    source: &ObjectSource,
    request_timeout: Option<Duration>,
) -> Result<ParquetFooter, QueryError> {
    let trailer = reader
        .read_range_with_timeout(
            &source.url,
            HttpByteRange::Suffix {
                length: PARQUET_TRAILER_SIZE_BYTES,
            },
            request_timeout,
        )
        .await?;
    let (object_size_bytes, footer_length_bytes) =
        parse_parquet_footer_trailer(&trailer, None, &source.url)?;

    read_footer_payload(
        reader,
        source,
        request_timeout,
        trailer,
        object_size_bytes,
        footer_length_bytes,
    )
    .await
}

pub async fn read_parquet_footer_for_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
) -> Result<ParquetFooter, QueryError> {
    if target.size_bytes < PARQUET_TRAILER_SIZE_BYTES {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for file '{}' requires at least {} bytes, but the descriptor declared {} bytes",
            target.path,
            PARQUET_TRAILER_SIZE_BYTES,
            target.size_bytes,
        )));
    }

    let trailer_offset = target
        .size_bytes
        .checked_sub(PARQUET_TRAILER_SIZE_BYTES)
        .expect("target sizes smaller than trailer bytes should already be rejected");
    let trailer = reader
        .read_range_with_timeout(
            &target.object_source.url,
            HttpByteRange::Bounded {
                offset: trailer_offset,
                length: PARQUET_TRAILER_SIZE_BYTES,
            },
            request_timeout,
        )
        .await?;
    let (_, footer_length_bytes) =
        parse_parquet_footer_trailer(&trailer, Some(target.size_bytes), &target.path)?;

    read_footer_payload(
        reader,
        &target.object_source,
        request_timeout,
        trailer,
        target.size_bytes,
        footer_length_bytes,
    )
    .await
}

pub async fn read_parquet_footer_for_target_with_cache(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<ParquetFooter, QueryError> {
    read_parquet_cached_footer_for_target(reader, target, request_timeout, cache, metrics).await
}

pub async fn read_parquet_metadata_for_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
) -> Result<ParquetFileMetadata, QueryError> {
    read_parquet_metadata_for_target_with_cache(reader, target, request_timeout, None, None).await
}

pub async fn read_parquet_metadata_for_target_with_cache(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<ParquetFileMetadata, QueryError> {
    let cached =
        read_parquet_cached_metadata_for_target(reader, target, request_timeout, cache, metrics)
            .await?;
    let metadata = cached
        .metadata
        .as_ref()
        .expect("metadata cache helper should return decoded parquet metadata");
    parquet_file_metadata_from_decoded(target, &cached.footer, metadata.as_ref())
}

pub async fn inspect_parquet_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
) -> Result<ParquetInspectionSummary, QueryError> {
    inspect_parquet_target_with_cache(reader, target, request_timeout, None, None).await
}

pub async fn inspect_parquet_target_with_cache(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<ParquetInspectionSummary, QueryError> {
    let cached =
        read_parquet_cached_metadata_for_target(reader, target, request_timeout, cache, metrics)
            .await?;
    let metadata = cached
        .metadata
        .as_ref()
        .expect("metadata cache helper should return decoded parquet metadata");
    parquet_inspection_from_metadata(target, &cached.footer, metadata.as_ref())
}

pub async fn stream_scan_target_batches(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
) -> Result<ScanTargetBatchStream<BoxStream<'static, Result<RecordBatch, QueryError>>>, QueryError>
{
    stream_scan_target_batches_with_row_group_pruning(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        None,
    )
    .await
}

pub async fn stream_scan_target_batches_with_row_group_pruning(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    row_group_predicate: Option<&ParquetRowGroupPruningPredicate>,
) -> Result<ScanTargetBatchStream<BoxStream<'static, Result<RecordBatch, QueryError>>>, QueryError>
{
    stream_scan_target_batches_with_row_group_pruning_and_cache(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        row_group_predicate,
        None,
    )
    .await
}

pub async fn stream_scan_target_batches_with_row_group_pruning_and_cache(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    row_group_predicate: Option<&ParquetRowGroupPruningPredicate>,
    cache: Option<&ParquetMetadataCache>,
) -> Result<ScanTargetBatchStream<BoxStream<'static, Result<RecordBatch, QueryError>>>, QueryError>
{
    let metrics_handle = SharedScanTargetMetricsHandle::new(ScanTargetMetricsSnapshot {
        files_touched: 1,
        files_skipped: 0,
        row_groups_touched: 0,
        row_groups_skipped: 0,
        rows_emitted: 0,
        bytes_fetched: 0,
        footer_reads: 1,
        metadata_probe_round_trips: 0,
        range_read_metrics: ParquetRangeReadMetrics::default(),
    });
    let metrics: Arc<dyn ScanTargetMetricsHandle + Send + Sync> = Arc::new(metrics_handle.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(HttpRangeAsyncFileReader::new(
        reader.clone(),
        target.clone(),
        request_timeout,
        cache.cloned(),
        metrics_handle.clone(),
    ))
    .await
    .map_err(|error| {
        query_error_from_parquet_error(error, |message| {
            format!(
                "browser runtime could not open parquet file '{}' for batch decoding: {message}",
                target.path
            )
        })
    })?;
    let row_group_plan = plan_parquet_row_groups(builder.metadata(), row_group_predicate)?;
    {
        let mut snapshot = metrics_handle.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during planning")
        })?;
        snapshot.row_groups_touched = row_group_plan.row_groups_touched;
        snapshot.row_groups_skipped = row_group_plan.row_groups_skipped;
    }
    if row_group_plan.row_groups.is_empty() {
        return Ok(ScanTargetBatchStream {
            batches: stream::empty().boxed(),
            metrics,
        });
    }
    let projection =
        projection_mask_for_required_columns(builder.parquet_schema(), target, required_columns);
    let target_for_stream = target.clone();
    let required_columns = required_columns.to_vec();
    let partition_column_types = partition_column_types.clone();
    let batches = builder
        .with_row_groups(row_group_plan.row_groups)
        .with_projection(projection)
        .with_batch_size(DEFAULT_STREAM_BATCH_SIZE)
        .build()
        .map_err(|error| {
            query_error_from_parquet_error(error, |message| {
                format!(
                    "browser runtime could not build a parquet batch reader for file '{}': {message}",
                    target.path
                )
            })
        })?
        .map(move |result| {
            let batch = result.map_err(|error| {
                query_error_from_parquet_error(error, |message| {
                    format!(
                        "browser runtime could not decode a parquet record batch for file '{}': {message}",
                        target_for_stream.path
                    )
                })
            })?;
            let rows_emitted = u64::try_from(batch.num_rows())
                .map_err(|_| execution_runtime_error("record batch row counts overflowed u64"))?;
            metrics_handle.record_rows_emitted(rows_emitted)?;
            merge_partition_columns_into_batch(
                batch,
                &target_for_stream,
                &partition_column_types,
                &required_columns,
            )
        })
        .boxed();

    Ok(ScanTargetBatchStream { batches, metrics })
}

struct MultiScanState {
    reader: HttpRangeReader,
    targets: Vec<ScanTarget>,
    next_target_index: usize,
    current: Option<BoxStream<'static, Result<RecordBatch, QueryError>>>,
    required_columns: Vec<String>,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    aggregate_metrics: AggregateScanTargetMetricsHandle,
}

/// Streams record batches across multiple scan targets in target order.
///
/// The returned stream opens each file lazily as the previous file is drained, and its metrics
/// handle aggregates the per-file snapshots for all files opened so far. This is the planned
/// multi-file primitive for runtime scan orchestration.
pub async fn stream_scan_targets(
    reader: &HttpRangeReader,
    targets: &[ScanTarget],
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
) -> Result<ScanTargetBatchStream<BoxStream<'static, Result<RecordBatch, QueryError>>>, QueryError>
{
    let aggregate_handle = AggregateScanTargetMetricsHandle::default();
    let metrics: Arc<dyn ScanTargetMetricsHandle + Send + Sync> =
        Arc::new(aggregate_handle.clone());
    let state = MultiScanState {
        reader: reader.clone(),
        targets: targets.to_vec(),
        next_target_index: 0,
        current: None,
        required_columns: required_columns.to_vec(),
        partition_column_types: partition_column_types.clone(),
        request_timeout,
        aggregate_metrics: aggregate_handle,
    };
    let batches = stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(current) = &mut state.current {
                match current.next().await {
                    Some(batch) => return batch.map(|batch| Some((batch, state))),
                    None => state.current = None,
                }
            }

            let Some(target) = state.targets.get(state.next_target_index).cloned() else {
                return Ok(None);
            };
            state.next_target_index += 1;

            let stream = stream_scan_target_batches(
                &state.reader,
                &target,
                &state.required_columns,
                &state.partition_column_types,
                state.request_timeout,
            )
            .await?;
            state
                .aggregate_metrics
                .add_child(Arc::clone(&stream.metrics))?;
            state.current = Some(stream.batches.boxed());
        }
    })
    .boxed();

    Ok(ScanTargetBatchStream { batches, metrics })
}

pub async fn scan_target_input_rows(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
) -> Result<Vec<ParquetInputRow>, QueryError> {
    scan_target_input_rows_with_cache(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        None,
    )
    .await
}

pub async fn scan_target_input_rows_with_cache(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    cache: Option<&ParquetMetadataCache>,
) -> Result<Vec<ParquetInputRow>, QueryError> {
    let stream = stream_scan_target_batches_with_row_group_pruning_and_cache(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        None,
        cache,
    )
    .await?;
    stream
        .batches
        .try_fold(Vec::new(), |mut rows, batch| async move {
            rows.extend(record_batch_to_input_rows(target, &batch)?);
            Ok(rows)
        })
        .await
}

pub fn plan_parquet_row_groups(
    metadata: &ParquetMetaData,
    predicate: Option<&ParquetRowGroupPruningPredicate>,
) -> Result<ParquetRowGroupPlan, QueryError> {
    let row_group_count = metadata.num_row_groups();
    let all_row_groups = (0..row_group_count).collect::<Vec<_>>();
    let Some(predicate) = predicate else {
        return Ok(ParquetRowGroupPlan {
            row_groups_touched: u64::try_from(row_group_count)
                .map_err(|_| execution_runtime_error("parquet row-group counts overflowed u64"))?,
            row_groups_skipped: 0,
            row_groups: all_row_groups,
        });
    };
    let Some(column_index) = parquet_column_index_for_name(metadata, &predicate.column) else {
        return Ok(ParquetRowGroupPlan {
            row_groups_touched: u64::try_from(row_group_count)
                .map_err(|_| execution_runtime_error("parquet row-group counts overflowed u64"))?,
            row_groups_skipped: 0,
            row_groups: all_row_groups,
        });
    };

    let mut retained = Vec::new();
    let mut skipped = 0_u64;
    for row_group_index in 0..row_group_count {
        let row_group = metadata.row_group(row_group_index);
        if row_group_integer_stats_match(row_group, column_index, &predicate.comparison) {
            retained.push(row_group_index);
        } else {
            skipped = skipped.checked_add(1).ok_or_else(|| {
                execution_runtime_error("parquet row-group skipped counts overflowed u64")
            })?;
        }
    }

    Ok(ParquetRowGroupPlan {
        row_groups_touched: u64::try_from(retained.len())
            .map_err(|_| execution_runtime_error("parquet row-group counts overflowed u64"))?,
        row_groups_skipped: skipped,
        row_groups: retained,
    })
}

pub fn decode_parquet_input_rows(
    target: &ScanTarget,
    bytes: Bytes,
    required_columns: &[String],
) -> Result<Vec<ParquetInputRow>, QueryError> {
    let reader = SerializedFileReader::new(bytes).map_err(|error| {
        execution_runtime_error(format!(
            "browser runtime could not open parquet file '{}': {error}",
            target.path
        ))
    })?;
    let row_iter = reader.get_row_iter(None).map_err(|error| {
        execution_runtime_error(format!(
            "browser runtime could not iterate parquet rows for file '{}': {error}",
            target.path
        ))
    })?;
    let mut rows = Vec::new();

    for row in row_iter {
        let row = row.map_err(|error| {
            execution_runtime_error(format!(
                "browser runtime could not decode a parquet row for file '{}': {error}",
                target.path
            ))
        })?;
        rows.push(parquet_row_to_input_row(target, &row, required_columns)?);
    }

    Ok(rows)
}

pub fn parquet_row_to_input_row(
    target: &ScanTarget,
    row: &parquet::record::Row,
    required_columns: &[String],
) -> Result<ParquetInputRow, QueryError> {
    if required_columns.is_empty() {
        return Ok(BTreeMap::new());
    }

    let required_columns = required_columns
        .iter()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();
    let mut values = BTreeMap::new();

    for (column_name, field) in row.get_column_iter() {
        let normalized_name = normalize_name(column_name);
        if !required_columns.contains(&normalized_name) {
            continue;
        }

        values.insert(
            normalized_name,
            parquet_field_to_scalar(&target.path, column_name, field)?,
        );
    }

    Ok(values)
}

fn projection_mask_for_required_columns(
    schema: &parquet::schema::types::SchemaDescriptor,
    target: &ScanTarget,
    required_columns: &[String],
) -> ProjectionMask {
    if required_columns.is_empty() {
        return ProjectionMask::none(schema.num_columns());
    }

    let required_columns = required_columns
        .iter()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();
    let partition_columns = target
        .partition_values
        .keys()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();
    let projected_columns = schema
        .columns()
        .iter()
        .map(|column| column.path().string())
        .filter(|column| {
            let normalized = normalize_name(column);
            required_columns.contains(&normalized) && !partition_columns.contains(&normalized)
        })
        .collect::<Vec<_>>();

    if projected_columns.is_empty() {
        ProjectionMask::none(schema.num_columns())
    } else {
        ProjectionMask::columns(schema, projected_columns.iter().map(String::as_str))
    }
}

fn merge_partition_columns_into_batch(
    batch: RecordBatch,
    target: &ScanTarget,
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    required_columns: &[String],
) -> Result<RecordBatch, QueryError> {
    let required_columns = required_columns
        .iter()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            Arc::new(ArrowField::new(
                normalize_name(field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();

    for (column, value) in &target.partition_values {
        let normalized_column = normalize_name(column);
        if !required_columns.is_empty() && !required_columns.contains(&normalized_column) {
            continue;
        }

        let partition_type = partition_column_type_for_name(partition_column_types, column)
            .ok_or_else(|| missing_partition_column_type_error(column))?;
        columns.push(partition_value_to_array(
            column,
            value,
            partition_type,
            batch.num_rows(),
        )?);
        fields.push(Arc::new(ArrowField::new(
            normalized_column,
            columns
                .last()
                .expect("pushed partition array should exist")
                .data_type()
                .clone(),
            true,
        )));
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    if columns.is_empty() {
        return Ok(batch);
    }

    RecordBatch::try_new(schema, columns).map_err(|error| {
        execution_runtime_error(format!(
            "browser runtime could not assemble a projected record batch for file '{}': {error}",
            target.path
        ))
    })
}

fn partition_value_to_array(
    column: &str,
    value: &Option<String>,
    partition_type: PartitionColumnType,
    row_count: usize,
) -> Result<ArrayRef, QueryError> {
    match partition_type {
        PartitionColumnType::String => Ok(Arc::new(StringArray::from(
            std::iter::repeat(value.as_deref())
                .take(row_count)
                .collect::<Vec<_>>(),
        ))),
        PartitionColumnType::Boolean => match value {
            Some(value) => parse_canonical_partition_bool(value)
                .map(|value| {
                    Arc::new(BooleanArray::from(
                        std::iter::repeat(Some(value))
                            .take(row_count)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                })
                .ok_or_else(|| invalid_partition_value_error(column, value, partition_type)),
            None => Ok(Arc::new(BooleanArray::from(
                std::iter::repeat(None::<bool>)
                    .take(row_count)
                    .collect::<Vec<_>>(),
            ))),
        },
        PartitionColumnType::Int64 => match value {
            Some(value) => parse_canonical_partition_i64(value)
                .map(|value| {
                    Arc::new(Int64Array::from(
                        std::iter::repeat(Some(value))
                            .take(row_count)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                })
                .ok_or_else(|| invalid_partition_value_error(column, value, partition_type)),
            None => Ok(Arc::new(Int64Array::from(
                std::iter::repeat(None::<i64>)
                    .take(row_count)
                    .collect::<Vec<_>>(),
            ))),
        },
        PartitionColumnType::Unsupported => Err(unsupported_partition_type_error(column)),
    }
}

fn record_batch_to_input_rows(
    target: &ScanTarget,
    batch: &RecordBatch,
) -> Result<Vec<ParquetInputRow>, QueryError> {
    let mut rows = (0..batch.num_rows())
        .map(|_| BTreeMap::new())
        .collect::<Vec<ParquetInputRow>>();

    for (field, column) in batch.schema().fields().iter().zip(batch.columns().iter()) {
        let normalized_name = normalize_name(field.name());
        for (row_index, row) in rows.iter_mut().enumerate() {
            row.insert(
                normalized_name.clone(),
                arrow_array_value_to_scalar(
                    &target.path,
                    field.name(),
                    column.as_ref(),
                    row_index,
                )?,
            );
        }
    }

    Ok(rows)
}

fn arrow_array_value_to_scalar(
    file_path: &str,
    column_name: &str,
    column: &dyn Array,
    row_index: usize,
) -> Result<ParquetScalarValue, QueryError> {
    if matches!(column.data_type(), ArrowDataType::Null) || column.is_null(row_index) {
        return Ok(ParquetScalarValue::Null);
    }

    match column.data_type() {
        ArrowDataType::Boolean => column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|column| ParquetScalarValue::Boolean(column.value(row_index)))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::Int8 => column
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|column| ParquetScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::Int16 => column
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|column| ParquetScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::Int32 => column
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|column| ParquetScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|column| ParquetScalarValue::Int64(column.value(row_index)))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::UInt8 => column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|column| ParquetScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::UInt16 => column
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|column| ParquetScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::UInt32 => column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|column| ParquetScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::UInt64 => column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .and_then(|column| i64::try_from(column.value(row_index)).ok())
            .map(ParquetScalarValue::Int64)
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        ArrowDataType::Utf8 => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|column| ParquetScalarValue::String(column.value(row_index).to_string()))
            .ok_or_else(|| unsupported_execution_array(file_path, column_name, column.data_type())),
        _ => Err(unsupported_execution_array(
            file_path,
            column_name,
            column.data_type(),
        )),
    }
}

pub fn parse_parquet_metadata(
    target: &ScanTarget,
    footer: &ParquetFooter,
) -> Result<ParquetFileMetadata, QueryError> {
    let metadata = decode_parquet_metadata(target, footer, "parquet metadata decode")?;
    parquet_file_metadata_from_decoded(target, footer, &metadata)
}

fn decode_parquet_metadata(
    target: &ScanTarget,
    footer: &ParquetFooter,
    context: &str,
) -> Result<ParquetMetaData, QueryError> {
    ParquetMetaDataReader::decode_metadata(footer.footer_bytes()).map_err(|error| {
        parquet_protocol_error(format!(
            "{context} for file '{}' failed: {error}",
            target.path
        ))
    })
}

fn parquet_file_metadata_from_decoded(
    target: &ScanTarget,
    footer: &ParquetFooter,
    metadata: &ParquetMetaData,
) -> Result<ParquetFileMetadata, QueryError> {
    let file_metadata = metadata.file_metadata();
    let row_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        parquet_protocol_error(format!(
            "parquet metadata for file '{}' reported a negative row count",
            target.path
        ))
    })?;
    let row_group_count = u64::try_from(metadata.num_row_groups()).map_err(|_| {
        parquet_protocol_error(format!(
            "parquet metadata for file '{}' reported too many row groups to fit in u64",
            target.path
        ))
    })?;
    let fields = file_metadata
        .schema_descr()
        .columns()
        .iter()
        .map(|column| parquet_field_from_descriptor(column.as_ref()))
        .collect::<Vec<_>>();
    let compression_codecs = parquet_compression_codecs(&metadata);
    let field_stats = parquet_field_stats_from_metadata(&metadata);

    Ok(ParquetFileMetadata {
        object_size_bytes: footer.object_size_bytes(),
        footer_length_bytes: footer.footer_length_bytes(),
        row_group_count,
        row_count,
        compression_codecs,
        fields,
        field_stats,
    })
}

pub fn inspect_parquet_footer(
    target: &ScanTarget,
    footer: &ParquetFooter,
) -> Result<ParquetInspectionSummary, QueryError> {
    let metadata = decode_parquet_metadata(target, footer, "parquet inspection metadata decode")?;
    parquet_inspection_from_metadata(target, footer, &metadata)
}

fn parquet_inspection_from_metadata(
    target: &ScanTarget,
    footer: &ParquetFooter,
    metadata: &ParquetMetaData,
) -> Result<ParquetInspectionSummary, QueryError> {
    let file_metadata = metadata.file_metadata();
    let row_count = non_negative_i64_to_u64(file_metadata.num_rows(), || {
        format!(
            "parquet inspection for file '{}' reported a negative row count",
            target.path
        )
    })?;
    let row_group_count = usize_to_u64(metadata.num_row_groups(), "row group count", &target.path)?;
    let column_count = usize_to_u64(
        file_metadata.schema_descr().columns().len(),
        "column count",
        &target.path,
    )?;
    let row_groups = parquet_inspection_row_groups(target, metadata)?;
    let compressed_size_bytes = row_groups.iter().try_fold(0_u64, |total, row_group| {
        total
            .checked_add(row_group.compressed_size_bytes)
            .ok_or_else(|| {
                parquet_protocol_error(format!(
                    "parquet inspection for file '{}' overflowed compressed byte totals",
                    target.path
                ))
            })
    })?;
    let uncompressed_size_bytes = row_groups.iter().try_fold(0_u64, |total, row_group| {
        total
            .checked_add(row_group.uncompressed_size_bytes)
            .ok_or_else(|| {
                parquet_protocol_error(format!(
                    "parquet inspection for file '{}' overflowed uncompressed byte totals",
                    target.path
                ))
            })
    })?;
    let metadata_memory_size_bytes =
        usize_to_u64(metadata.memory_size(), "metadata memory size", &target.path)?;

    Ok(ParquetInspectionSummary {
        path: target.path.clone(),
        object_size_bytes: footer.object_size_bytes(),
        footer_length_bytes: footer.footer_length_bytes(),
        metadata_memory_size_bytes,
        created_by: file_metadata.created_by().map(str::to_string),
        file_version: file_metadata.version(),
        row_group_count,
        row_count,
        column_count,
        compression: compression_summary(compressed_size_bytes, uncompressed_size_bytes),
        columns: parquet_inspection_columns(target, metadata)?,
        row_groups,
    })
}

fn parquet_inspection_row_groups(
    target: &ScanTarget,
    metadata: &ParquetMetaData,
) -> Result<Vec<ParquetInspectionRowGroup>, QueryError> {
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(index, row_group)| {
            let row_count = non_negative_i64_to_u64(row_group.num_rows(), || {
                format!(
                    "parquet inspection for file '{}' row group {} reported a negative row count",
                    target.path, index
                )
            })?;
            let uncompressed_size_bytes = non_negative_i64_to_u64(row_group.total_byte_size(), || {
                format!(
                    "parquet inspection for file '{}' row group {} reported a negative uncompressed byte size",
                    target.path, index
                )
            })?;
            let compressed_size_bytes = non_negative_i64_to_u64(row_group.compressed_size(), || {
                format!(
                    "parquet inspection for file '{}' row group {} reported a negative compressed byte size",
                    target.path, index
                )
            })?;
            let index = usize_to_u64(index, "row group index", &target.path)?;
            let columns = row_group
                .columns()
                .iter()
                .map(|column| parquet_inspection_column_chunk(target, column))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ParquetInspectionRowGroup {
                index,
                row_count,
                compressed_size_bytes,
                uncompressed_size_bytes,
                columns,
            })
        })
        .collect()
}

fn parquet_inspection_columns(
    target: &ScanTarget,
    metadata: &ParquetMetaData,
) -> Result<Vec<ParquetInspectionColumn>, QueryError> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(column_index, descriptor)| {
            let field = parquet_field_from_descriptor(descriptor.as_ref());
            let mut compressed_size_bytes = 0_u64;
            let mut uncompressed_size_bytes = 0_u64;
            let mut null_count = Some(0_u64);
            let mut encodings = BTreeSet::new();
            let mut compressions = BTreeSet::new();
            let mut has_statistics = false;
            let mut has_column_index = false;
            let mut has_offset_index = false;
            let mut has_bloom_filter = false;

            for row_group in metadata.row_groups() {
                let chunk = row_group.column(column_index);
                let chunk_summary = parquet_inspection_column_chunk(target, chunk)?;
                compressed_size_bytes = compressed_size_bytes
                    .checked_add(chunk_summary.compressed_size_bytes)
                    .ok_or_else(|| {
                        parquet_protocol_error(format!(
                            "parquet inspection for file '{}' column '{}' overflowed compressed byte totals",
                            target.path, field.name
                        ))
                    })?;
                uncompressed_size_bytes = uncompressed_size_bytes
                    .checked_add(chunk_summary.uncompressed_size_bytes)
                    .ok_or_else(|| {
                        parquet_protocol_error(format!(
                            "parquet inspection for file '{}' column '{}' overflowed uncompressed byte totals",
                            target.path, field.name
                        ))
                    })?;
                null_count = match (null_count, chunk_summary.null_count) {
                    (Some(total), Some(count)) => Some(total.checked_add(count).ok_or_else(|| {
                        parquet_protocol_error(format!(
                            "parquet inspection for file '{}' column '{}' overflowed null counts",
                            target.path, field.name
                        ))
                    })?),
                    _ => None,
                };
                encodings.extend(chunk_summary.encodings);
                compressions.insert(chunk_summary.compression);
                has_statistics |= chunk_summary.has_statistics;
                has_column_index |= chunk_summary.has_column_index;
                has_offset_index |= chunk_summary.has_offset_index;
                has_bloom_filter |= chunk_summary.has_bloom_filter;
            }

            Ok(ParquetInspectionColumn {
                name: field.name,
                physical_type: format!("{:?}", field.physical_type),
                logical_type: field.logical_type.map(|value| format!("{value:?}")),
                converted_type: field.converted_type.map(|value| format!("{value:?}")),
                repetition: format!("{:?}", field.repetition),
                nullable: field.nullable,
                compressed_size_bytes,
                uncompressed_size_bytes,
                null_count,
                encodings: encodings.into_iter().collect(),
                compressions: compressions.into_iter().collect(),
                has_statistics,
                has_column_index,
                has_offset_index,
                has_bloom_filter,
            })
        })
        .collect()
}

fn parquet_inspection_column_chunk(
    target: &ScanTarget,
    column: &parquet::file::metadata::ColumnChunkMetaData,
) -> Result<ParquetInspectionColumnChunk, QueryError> {
    let column_name = column.column_path().string();
    let compressed_size_bytes = non_negative_i64_to_u64(column.compressed_size(), || {
        format!(
            "parquet inspection for file '{}' column '{}' reported a negative compressed byte size",
            target.path, column_name
        )
    })?;
    let uncompressed_size_bytes = non_negative_i64_to_u64(column.uncompressed_size(), || {
        format!(
            "parquet inspection for file '{}' column '{}' reported a negative uncompressed byte size",
            target.path, column_name
        )
    })?;
    let has_statistics = column.statistics().is_some();
    let null_count = column.statistics().and_then(|stats| stats.null_count_opt());

    Ok(ParquetInspectionColumnChunk {
        column_name,
        compression: format!("{:?}", column.compression()),
        encodings: column
            .encodings()
            .map(|encoding| format!("{encoding:?}"))
            .collect(),
        compressed_size_bytes,
        uncompressed_size_bytes,
        null_count,
        has_statistics,
        has_column_index: column.column_index_offset().is_some()
            || column.column_index_length().is_some(),
        has_offset_index: column.offset_index_offset().is_some()
            || column.offset_index_length().is_some(),
        has_bloom_filter: column.bloom_filter_offset().is_some()
            || column.bloom_filter_length().is_some(),
    })
}

fn parquet_compression_codecs(metadata: &ParquetMetaData) -> Vec<String> {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .map(|column| parquet_compression_codec_name(column.compression()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parquet_compression_codec_name(compression: RawParquetCompression) -> String {
    format!("{compression:?}")
        .split('(')
        .next()
        .unwrap_or("UNKNOWN")
        .to_string()
}

fn compression_summary(
    compressed_size_bytes: u64,
    uncompressed_size_bytes: u64,
) -> ParquetCompressionSummary {
    let ratio_basis_points = if uncompressed_size_bytes == 0 {
        0
    } else {
        ((u128::from(compressed_size_bytes) * 10_000) / u128::from(uncompressed_size_bytes))
            .min(u128::from(u64::MAX)) as u64
    };

    ParquetCompressionSummary {
        compressed_size_bytes,
        uncompressed_size_bytes,
        ratio_basis_points,
    }
}

fn non_negative_i64_to_u64(
    value: i64,
    context: impl FnOnce() -> String,
) -> Result<u64, QueryError> {
    u64::try_from(value).map_err(|_| parquet_protocol_error(context()))
}

fn usize_to_u64(value: usize, label: &str, path: &str) -> Result<u64, QueryError> {
    u64::try_from(value).map_err(|_| {
        parquet_protocol_error(format!(
            "parquet inspection for file '{path}' reported a {label} too large to fit in u64"
        ))
    })
}

async fn read_footer_payload(
    reader: &HttpRangeReader,
    source: &ObjectSource,
    request_timeout: Option<Duration>,
    trailer: HttpRangeReadResult,
    object_size_bytes: u64,
    footer_length_bytes: u32,
) -> Result<ParquetFooter, QueryError> {
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
                trailer.metadata.url, footer_length_bytes, object_size_bytes,
            ))
        })?;
    let footer = reader
        .read_range_with_timeout(
            &source.url,
            HttpByteRange::Bounded {
                offset: footer_offset,
                length: footer_length_bytes_u64,
            },
            request_timeout,
        )
        .await?;
    validate_parquet_footer_read_consistency(&trailer, &footer, object_size_bytes)?;

    Ok(ParquetFooter {
        object_size_bytes,
        object_etag: trailer.metadata.etag.clone(),
        footer_length_bytes,
        footer_bytes: footer.bytes,
    })
}

async fn read_parquet_cached_footer_for_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<ParquetFooter, QueryError> {
    let mut metrics = metrics;
    if let Some(cache) = cache {
        match cache_key_for_target_identity(target) {
            Some(key) => {
                if let Some(cached) = cache.get(&key)? {
                    validate_cached_metadata_identity(reader, target, request_timeout).await?;
                    record_footer_cache_hit(Some(cache), metrics.as_deref_mut(), 2)?;
                    return Ok(cached.footer);
                }
                record_footer_cache_miss(Some(cache), metrics.as_deref_mut())?;
            }
            None => {
                record_footer_cache_miss(Some(cache), metrics.as_deref_mut())?;
                record_footer_cache_degraded_identity_read(Some(cache), metrics.as_deref_mut())?;
            }
        }
    }

    let footer = read_parquet_footer_for_target(reader, target, request_timeout).await?;
    if let Some(cache) = cache {
        if let Some(key) = cache_key_for_target_footer(target, &footer) {
            cache.insert(
                key,
                ParquetCachedMetadata {
                    footer: footer.clone(),
                    metadata: None,
                },
            )?;
        }
    }

    Ok(footer)
}

async fn read_parquet_cached_metadata_for_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
    cache: Option<&ParquetMetadataCache>,
    metrics: Option<&mut ParquetRangeReadMetrics>,
) -> Result<ParquetCachedMetadata, QueryError> {
    let mut metrics = metrics;
    if let Some(cache) = cache {
        match cache_key_for_target_identity(target) {
            Some(key) => {
                if let Some(cached) = cache.get(&key)? {
                    validate_cached_metadata_identity(reader, target, request_timeout).await?;
                    let cached = match cached.metadata {
                        Some(_) => cached,
                        None => {
                            let metadata = Arc::new(decode_parquet_metadata(
                                target,
                                &cached.footer,
                                "parquet metadata decode",
                            )?);
                            let cached = ParquetCachedMetadata {
                                footer: cached.footer,
                                metadata: Some(metadata),
                            };
                            cache.insert(key, cached.clone())?;
                            cached
                        }
                    };
                    record_footer_cache_hit(Some(cache), metrics.as_deref_mut(), 2)?;
                    return Ok(cached);
                }
                record_footer_cache_miss(Some(cache), metrics.as_deref_mut())?;
            }
            None => {
                record_footer_cache_miss(Some(cache), metrics.as_deref_mut())?;
                record_footer_cache_degraded_identity_read(Some(cache), metrics.as_deref_mut())?;
            }
        }
    }

    let footer = read_parquet_footer_for_target(reader, target, request_timeout).await?;
    let metadata = Arc::new(decode_parquet_metadata(
        target,
        &footer,
        "parquet metadata decode",
    )?);
    let cached = ParquetCachedMetadata {
        footer,
        metadata: Some(metadata),
    };

    if let Some(cache) = cache {
        if let Some(key) = cache_key_for_target_footer(target, &cached.footer) {
            cache.insert(key, cached.clone())?;
        }
    }

    Ok(cached)
}

fn parse_parquet_footer_trailer(
    trailer: &HttpRangeReadResult,
    known_object_size_bytes: Option<u64>,
    scan_path: &str,
) -> Result<(u64, u32), QueryError> {
    let object_size_bytes = match known_object_size_bytes {
        Some(known_size_bytes) => {
            if let Some(observed_size_bytes) = trailer.metadata.size_bytes {
                if observed_size_bytes != known_size_bytes {
                    return Err(parquet_protocol_error(format!(
                        "parquet footer bootstrap for file '{}' observed object size {} bytes on the trailer read, but the descriptor declared {} bytes",
                        scan_path, observed_size_bytes, known_size_bytes,
                    )));
                }
            }
            known_size_bytes
        }
        None => trailer.metadata.size_bytes.ok_or_else(|| {
            parquet_protocol_error(format!(
                "parquet footer bootstrap for '{}' did not return object size metadata",
                trailer.metadata.url,
            ))
        })?,
    };

    if object_size_bytes < PARQUET_TRAILER_SIZE_BYTES {
        return Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' requires at least {} bytes, but the object size was {} bytes",
            trailer.metadata.url, PARQUET_TRAILER_SIZE_BYTES, object_size_bytes,
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
            trailer.metadata.url, footer_length_bytes, max_footer_length,
        )));
    }

    Ok((object_size_bytes, footer_length_bytes))
}

fn validate_parquet_footer_read_consistency(
    trailer: &HttpRangeReadResult,
    footer: &HttpRangeReadResult,
    expected_object_size_bytes: u64,
) -> Result<(), QueryError> {
    if let Some(footer_object_size_bytes) = footer.metadata.size_bytes {
        if footer_object_size_bytes != expected_object_size_bytes {
            return Err(parquet_protocol_error(format!(
                "parquet footer bootstrap for '{}' observed object size {} bytes on the trailer read but {} bytes on the footer read",
                trailer.metadata.url, expected_object_size_bytes, footer_object_size_bytes,
            )));
        }
    }

    match (&trailer.metadata.etag, &footer.metadata.etag) {
        (Some(trailer_etag), Some(footer_etag)) if trailer_etag != footer_etag => {
            Err(parquet_protocol_error(format!(
                "parquet footer bootstrap for '{}' observed entity tag {} on the trailer read but {} on the footer read",
                trailer.metadata.url, trailer_etag, footer_etag,
            )))
        }
        (Some(_), None) | (None, Some(_)) => Err(parquet_protocol_error(format!(
            "parquet footer bootstrap for '{}' observed entity-tag metadata on only one of the two footer bootstrap reads",
            trailer.metadata.url,
        ))),
        _ => Ok(()),
    }
}

fn partition_column_type_for_name(
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    column: &str,
) -> Option<PartitionColumnType> {
    let normalized_column = normalize_name(column);
    partition_column_types
        .iter()
        .find_map(|(name, partition_type)| {
            (normalize_name(name) == normalized_column).then_some(*partition_type)
        })
}

fn missing_partition_column_type_error(column: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser execution requires explicit partition type metadata for column '{}'",
            column
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::NativeRequired)
}

fn unsupported_partition_type_error(column: &str) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser execution requires native fallback for unsupported partition column '{}'",
            column
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::NativeRequired)
}

fn invalid_partition_value_error(
    column: &str,
    value: &str,
    partition_type: PartitionColumnType,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser execution could not coerce partition value '{}' for column '{}' as {:?}",
            value, column, partition_type
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::NativeRequired)
}

fn parse_canonical_partition_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_canonical_partition_i64(value: &str) -> Option<i64> {
    let parsed = value.parse::<i64>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

fn parquet_field_to_scalar(
    file_path: &str,
    column_name: &str,
    field: &ParquetField,
) -> Result<ParquetScalarValue, QueryError> {
    match field {
        ParquetField::Null => Ok(ParquetScalarValue::Null),
        ParquetField::Bool(value) => Ok(ParquetScalarValue::Boolean(*value)),
        ParquetField::Byte(value) => Ok(ParquetScalarValue::Int64(i64::from(*value))),
        ParquetField::Short(value) => Ok(ParquetScalarValue::Int64(i64::from(*value))),
        ParquetField::Int(value) => Ok(ParquetScalarValue::Int64(i64::from(*value))),
        ParquetField::Long(value) => Ok(ParquetScalarValue::Int64(*value)),
        ParquetField::UByte(value) => Ok(ParquetScalarValue::Int64(i64::from(*value))),
        ParquetField::UShort(value) => Ok(ParquetScalarValue::Int64(i64::from(*value))),
        ParquetField::UInt(value) => Ok(ParquetScalarValue::Int64(i64::from(*value))),
        ParquetField::ULong(value) => i64::try_from(*value)
            .map(ParquetScalarValue::Int64)
            .map_err(|_| unsupported_execution_scalar(file_path, column_name, field)),
        ParquetField::Str(value) => Ok(ParquetScalarValue::String(value.clone())),
        _ => Err(unsupported_execution_scalar(file_path, column_name, field)),
    }
}

fn unsupported_execution_scalar(
    file_path: &str,
    column_name: &str,
    field: &ParquetField,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser runtime cannot execute parquet field '{}' in file '{}' with value kind '{field:?}'",
            column_name, file_path
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::NativeRequired)
}

fn unsupported_execution_array(
    file_path: &str,
    column_name: &str,
    data_type: &ArrowDataType,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser runtime cannot execute arrow field '{}' in file '{}' with data type '{data_type:?}'",
            column_name, file_path
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::NativeRequired)
}

fn parquet_field_stats_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> BTreeMap<String, ParquetFieldStats> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(column_index, column)| {
            let field_stats =
                parquet_integer_field_stats(column.as_ref(), metadata.row_groups(), column_index)?;
            Some((column.path().string(), field_stats))
        })
        .collect()
}

fn parquet_integer_field_stats(
    column: &parquet::schema::types::ColumnDescriptor,
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    column_index: usize,
) -> Option<ParquetFieldStats> {
    if !matches!(
        column.physical_type(),
        RawParquetPhysicalType::INT32 | RawParquetPhysicalType::INT64
    ) {
        return None;
    }
    if column.converted_type() == RawParquetConvertedType::DECIMAL
        || matches!(
            column.logical_type_ref(),
            Some(RawParquetLogicalType::Decimal { .. })
        )
    {
        return None;
    }

    let mut min_i64 = None;
    let mut max_i64 = None;
    let mut min_complete = true;
    let mut max_complete = true;
    let mut null_count = Some(0_u64);
    let mut saw_statistics = false;

    for row_group in row_groups {
        let stats = row_group.column(column_index).statistics()?;
        saw_statistics = true;
        let (row_group_min, row_group_max) = parquet_integer_min_max(stats)?;

        match row_group_min {
            Some(value) => {
                min_i64 = Some(min_i64.map_or(value, |current: i64| current.min(value)));
            }
            None => {
                min_complete = false;
            }
        }

        match row_group_max {
            Some(value) => {
                max_i64 = Some(max_i64.map_or(value, |current: i64| current.max(value)));
            }
            None => {
                max_complete = false;
            }
        }

        null_count = match (null_count, stats.null_count_opt()) {
            (Some(total), Some(count)) => total.checked_add(count),
            _ => None,
        };
    }

    if !saw_statistics {
        return None;
    }

    let field_stats = ParquetFieldStats {
        min_i64: min_complete.then_some(min_i64).flatten(),
        max_i64: max_complete.then_some(max_i64).flatten(),
        null_count,
    };

    (field_stats.min_i64.is_some()
        || field_stats.max_i64.is_some()
        || field_stats.null_count.is_some())
    .then_some(field_stats)
}

fn parquet_integer_min_max(stats: &ParquetStatistics) -> Option<(Option<i64>, Option<i64>)> {
    match stats {
        ParquetStatistics::Int32(stats) => Some((
            stats.min_opt().copied().map(i64::from),
            stats.max_opt().copied().map(i64::from),
        )),
        ParquetStatistics::Int64(stats) => {
            Some((stats.min_opt().copied(), stats.max_opt().copied()))
        }
        _ => None,
    }
}

fn parquet_column_index_for_name(metadata: &ParquetMetaData, column_name: &str) -> Option<usize> {
    let normalized_column = normalize_name(column_name);
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| normalize_name(&column.path().string()) == normalized_column)
}

fn row_group_integer_stats_match(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    column_index: usize,
    comparison: &ParquetIntegerComparison,
) -> bool {
    let Some(stats) = row_group.column(column_index).statistics() else {
        return true;
    };
    let Some((Some(min_i64), Some(max_i64))) = parquet_integer_min_max(stats) else {
        return true;
    };

    match comparison {
        ParquetIntegerComparison::Eq(value) => min_i64 <= *value && max_i64 >= *value,
        ParquetIntegerComparison::Gt(value) => max_i64 > *value,
        ParquetIntegerComparison::Gte(value) => max_i64 >= *value,
        ParquetIntegerComparison::Lt(value) => min_i64 < *value,
        ParquetIntegerComparison::Lte(value) => min_i64 <= *value,
    }
}

fn parquet_field_from_descriptor(
    column: &parquet::schema::types::ColumnDescriptor,
) -> ParquetColumnField {
    let repetition = column.self_type().get_basic_info().repetition();
    let converted_type = parquet_converted_type(column.converted_type());
    let logical_type = column.logical_type_ref().map(parquet_logical_type);
    let type_length = matches!(
        column.physical_type(),
        RawParquetPhysicalType::FIXED_LEN_BYTE_ARRAY
    )
    .then(|| column.type_length());
    let has_decimal_annotation = column.converted_type() == RawParquetConvertedType::DECIMAL
        || matches!(
            column.logical_type_ref(),
            Some(RawParquetLogicalType::Decimal { .. })
        );
    let precision = has_decimal_annotation.then(|| column.type_precision());
    let scale = has_decimal_annotation.then(|| column.type_scale());

    ParquetColumnField {
        name: column.path().string(),
        physical_type: parquet_physical_type(column.physical_type()),
        logical_type,
        converted_type,
        repetition: parquet_repetition(repetition),
        nullable: column.self_type().is_optional(),
        max_definition_level: column.max_def_level(),
        max_repetition_level: column.max_rep_level(),
        type_length,
        precision,
        scale,
    }
}

fn parquet_physical_type(physical_type: RawParquetPhysicalType) -> ParquetPhysicalType {
    match physical_type {
        RawParquetPhysicalType::BOOLEAN => ParquetPhysicalType::Boolean,
        RawParquetPhysicalType::INT32 => ParquetPhysicalType::Int32,
        RawParquetPhysicalType::INT64 => ParquetPhysicalType::Int64,
        RawParquetPhysicalType::INT96 => ParquetPhysicalType::Int96,
        RawParquetPhysicalType::FLOAT => ParquetPhysicalType::Float,
        RawParquetPhysicalType::DOUBLE => ParquetPhysicalType::Double,
        RawParquetPhysicalType::BYTE_ARRAY => ParquetPhysicalType::ByteArray,
        RawParquetPhysicalType::FIXED_LEN_BYTE_ARRAY => ParquetPhysicalType::FixedLenByteArray,
    }
}

fn parquet_repetition(repetition: RawParquetRepetition) -> ParquetRepetition {
    match repetition {
        RawParquetRepetition::REQUIRED => ParquetRepetition::Required,
        RawParquetRepetition::OPTIONAL => ParquetRepetition::Optional,
        RawParquetRepetition::REPEATED => ParquetRepetition::Repeated,
    }
}

fn parquet_converted_type(converted_type: RawParquetConvertedType) -> Option<ParquetConvertedType> {
    Some(match converted_type {
        RawParquetConvertedType::NONE => return None,
        RawParquetConvertedType::UTF8 => ParquetConvertedType::Utf8,
        RawParquetConvertedType::MAP => ParquetConvertedType::Map,
        RawParquetConvertedType::MAP_KEY_VALUE => ParquetConvertedType::MapKeyValue,
        RawParquetConvertedType::LIST => ParquetConvertedType::List,
        RawParquetConvertedType::ENUM => ParquetConvertedType::Enum,
        RawParquetConvertedType::DECIMAL => ParquetConvertedType::Decimal,
        RawParquetConvertedType::DATE => ParquetConvertedType::Date,
        RawParquetConvertedType::TIME_MILLIS => ParquetConvertedType::TimeMillis,
        RawParquetConvertedType::TIME_MICROS => ParquetConvertedType::TimeMicros,
        RawParquetConvertedType::TIMESTAMP_MILLIS => ParquetConvertedType::TimestampMillis,
        RawParquetConvertedType::TIMESTAMP_MICROS => ParquetConvertedType::TimestampMicros,
        RawParquetConvertedType::UINT_8 => ParquetConvertedType::UInt8,
        RawParquetConvertedType::UINT_16 => ParquetConvertedType::UInt16,
        RawParquetConvertedType::UINT_32 => ParquetConvertedType::UInt32,
        RawParquetConvertedType::UINT_64 => ParquetConvertedType::UInt64,
        RawParquetConvertedType::INT_8 => ParquetConvertedType::Int8,
        RawParquetConvertedType::INT_16 => ParquetConvertedType::Int16,
        RawParquetConvertedType::INT_32 => ParquetConvertedType::Int32,
        RawParquetConvertedType::INT_64 => ParquetConvertedType::Int64,
        RawParquetConvertedType::JSON => ParquetConvertedType::Json,
        RawParquetConvertedType::BSON => ParquetConvertedType::Bson,
        RawParquetConvertedType::INTERVAL => ParquetConvertedType::Interval,
    })
}

fn parquet_logical_type(logical_type: &RawParquetLogicalType) -> ParquetLogicalType {
    match logical_type {
        RawParquetLogicalType::String => ParquetLogicalType::String,
        RawParquetLogicalType::Map => ParquetLogicalType::Map,
        RawParquetLogicalType::List => ParquetLogicalType::List,
        RawParquetLogicalType::Enum => ParquetLogicalType::Enum,
        RawParquetLogicalType::Decimal { scale, precision } => ParquetLogicalType::Decimal {
            scale: *scale,
            precision: *precision,
        },
        RawParquetLogicalType::Date => ParquetLogicalType::Date,
        RawParquetLogicalType::Time {
            is_adjusted_to_u_t_c,
            unit,
        } => ParquetLogicalType::Time {
            is_adjusted_to_utc: *is_adjusted_to_u_t_c,
            unit: parquet_time_unit(unit),
        },
        RawParquetLogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            unit,
        } => ParquetLogicalType::Timestamp {
            is_adjusted_to_utc: *is_adjusted_to_u_t_c,
            unit: parquet_time_unit(unit),
        },
        RawParquetLogicalType::Integer {
            bit_width,
            is_signed,
        } => ParquetLogicalType::Integer {
            bit_width: *bit_width,
            is_signed: *is_signed,
        },
        RawParquetLogicalType::Unknown => ParquetLogicalType::Unknown,
        RawParquetLogicalType::Json => ParquetLogicalType::Json,
        RawParquetLogicalType::Bson => ParquetLogicalType::Bson,
        RawParquetLogicalType::Uuid => ParquetLogicalType::Uuid,
        RawParquetLogicalType::Float16 => ParquetLogicalType::Float16,
        RawParquetLogicalType::Variant {
            specification_version,
        } => ParquetLogicalType::Variant {
            specification_version: *specification_version,
        },
        RawParquetLogicalType::Geometry { crs } => {
            ParquetLogicalType::Geometry { crs: crs.clone() }
        }
        RawParquetLogicalType::Geography { crs, algorithm } => ParquetLogicalType::Geography {
            crs: crs.clone(),
            algorithm: algorithm.as_ref().map(parquet_edge_interpolation_algorithm),
        },
        RawParquetLogicalType::_Unknown { field_id } => ParquetLogicalType::Unrecognized {
            field_id: *field_id,
        },
    }
}

fn parquet_time_unit(unit: &RawParquetTimeUnit) -> ParquetTimeUnit {
    match unit {
        RawParquetTimeUnit::MILLIS => ParquetTimeUnit::Millis,
        RawParquetTimeUnit::MICROS => ParquetTimeUnit::Micros,
        RawParquetTimeUnit::NANOS => ParquetTimeUnit::Nanos,
    }
}

fn parquet_edge_interpolation_algorithm(
    algorithm: &RawParquetEdgeInterpolationAlgorithm,
) -> ParquetEdgeInterpolationAlgorithm {
    match algorithm {
        RawParquetEdgeInterpolationAlgorithm::SPHERICAL => {
            ParquetEdgeInterpolationAlgorithm::Spherical
        }
        RawParquetEdgeInterpolationAlgorithm::VINCENTY => {
            ParquetEdgeInterpolationAlgorithm::Vincenty
        }
        RawParquetEdgeInterpolationAlgorithm::THOMAS => ParquetEdgeInterpolationAlgorithm::Thomas,
        RawParquetEdgeInterpolationAlgorithm::ANDOYER => ParquetEdgeInterpolationAlgorithm::Andoyer,
        RawParquetEdgeInterpolationAlgorithm::KARNEY => ParquetEdgeInterpolationAlgorithm::Karney,
        RawParquetEdgeInterpolationAlgorithm::_Unknown(code) => {
            ParquetEdgeInterpolationAlgorithm::Unknown(*code)
        }
    }
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn strong_object_etag(etag: &str) -> Option<String> {
    let etag = etag.trim();
    if etag.starts_with("W/") || etag.starts_with("w/") {
        return None;
    }
    if !(etag.starts_with('"') && etag.ends_with('"')) {
        return None;
    }
    Some(etag.to_string())
}

fn parquet_protocol_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ObjectStoreProtocol,
        message,
        runtime_target(),
    )
}

fn execution_runtime_error(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::ExecutionFailed, message, runtime_target())
}

#[cfg(test)]
mod tests {
    use super::{
        merge_parquet_range_read_metrics, ParquetObjectIdentity, ParquetRangeReadKey,
        ParquetRangeReadMetrics, ParquetRangeReadPhase,
    };

    #[test]
    fn range_read_metrics_store_compact_keys_without_signed_url_material() {
        let mut metrics = ParquetRangeReadMetrics::default();

        metrics.record(
            ParquetRangeReadPhase::BootstrapFooter,
            ParquetRangeReadKey::new("part-000.parquet", 96, 8),
        );
        metrics.record(
            ParquetRangeReadPhase::ScanFooter,
            ParquetRangeReadKey::new("part-000.parquet", 96, 8),
        );

        assert_eq!(metrics.bootstrap_footer_range_reads, 1);
        assert_eq!(metrics.scan_footer_range_reads, 1);
        assert_eq!(metrics.duplicate_range_reads, 1);
        assert_eq!(metrics.exact_range_read_keys.len(), 1);
        assert!(metrics
            .exact_range_read_keys
            .iter()
            .all(|key| !key.resource.contains("X-Goog-Signature")));
    }

    #[test]
    fn range_read_metric_merge_counts_cross_snapshot_duplicates_once() {
        let mut left = ParquetRangeReadMetrics::default();
        left.record(
            ParquetRangeReadPhase::BootstrapFooter,
            ParquetRangeReadKey::new("part-000.parquet", 96, 8),
        );
        let mut right = ParquetRangeReadMetrics::default();
        right.record(
            ParquetRangeReadPhase::ScanFooter,
            ParquetRangeReadKey::new("part-000.parquet", 96, 8),
        );

        let merged = merge_parquet_range_read_metrics(&left, &right);

        assert_eq!(merged.bootstrap_footer_range_reads, 1);
        assert_eq!(merged.scan_footer_range_reads, 1);
        assert_eq!(merged.duplicate_range_reads, 1);
        assert_eq!(merged.exact_range_read_keys.len(), 1);
    }

    #[test]
    fn range_read_metrics_distinguish_identity_present_and_missing_reads() {
        let mut metrics = ParquetRangeReadMetrics::default();

        metrics.record(
            ParquetRangeReadPhase::ScanData,
            ParquetRangeReadKey::new_with_identity(
                "part-000.parquet",
                Some(ParquetObjectIdentity::new("\"etag-v1\"", 128)),
                32,
                16,
            ),
        );
        metrics.record(
            ParquetRangeReadPhase::ScanData,
            ParquetRangeReadKey::new_with_identity("part-001.parquet", None, 48, 16),
        );

        assert_eq!(metrics.identity_present_range_reads, 1);
        assert_eq!(metrics.identity_missing_range_reads, 1);
        assert!(metrics.exact_range_read_keys.iter().all(|key| {
            !key.resource.contains("X-Goog-Signature")
                && key.object_identity.as_ref().map_or(true, |identity| {
                    !identity.object_etag.contains("X-Goog-Signature")
                })
        }));
    }

    #[test]
    fn range_read_metrics_count_duplicates_across_identity_dimensions() {
        let mut metrics = ParquetRangeReadMetrics::default();

        metrics.record(
            ParquetRangeReadPhase::BootstrapFooter,
            ParquetRangeReadKey::new("part-000.parquet", 96, 8),
        );
        metrics.record(
            ParquetRangeReadPhase::ScanFooter,
            ParquetRangeReadKey::new_with_identity(
                "part-000.parquet",
                Some(ParquetObjectIdentity::new("\"etag-v1\"", 128)),
                96,
                8,
            ),
        );

        assert_eq!(metrics.bootstrap_footer_range_reads, 1);
        assert_eq!(metrics.scan_footer_range_reads, 1);
        assert_eq!(metrics.identity_present_range_reads, 1);
        assert_eq!(metrics.identity_missing_range_reads, 1);
        assert_eq!(metrics.duplicate_range_reads, 1);
        assert_eq!(metrics.exact_range_read_keys.len(), 1);
    }
}

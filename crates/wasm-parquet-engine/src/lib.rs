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
    ByteExtent, ExtentCacheEntry, ExtentCacheKey, HttpByteRange, HttpMetadataProbeRequirements,
    HttpRangeReadResult, HttpRangeReader, HttpRangeValidation, MemoryPersistentExtentCache,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-side Parquet planning and scan primitives.";
pub const MAX_PARQUET_FOOTER_SIZE_BYTES: u64 = 16 * 1024 * 1024;
pub const PARQUET_TRAILER_SIZE_BYTES: u64 = 8;
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
pub const DEFAULT_STREAM_BATCH_SIZE: usize = 1024;
const MAX_COALESCED_INDIVIDUAL_GAP_BYTES: u64 = 16 * 1024;
const MAX_COALESCED_CUMULATIVE_GAP_BYTES: u64 = 64 * 1024;
const MAX_COALESCED_GAP_AMPLIFICATION_NUMERATOR: u64 = 1;
const MAX_COALESCED_GAP_AMPLIFICATION_DENOMINATOR: u64 = 4;
const MAX_COALESCED_PHYSICAL_RANGE_BYTES: u64 = 512 * 1024;

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParquetRangeCacheMetrics {
    pub range_cache_hits: u64,
    pub range_cache_misses: u64,
    pub range_cache_stores: u64,
    pub range_cache_degraded_identity_reads: u64,
    pub range_cache_identity_drift_misses: u64,
}

#[derive(Clone, Debug)]
pub struct ParquetRangeCache {
    extents: Arc<MemoryPersistentExtentCache>,
    metrics: Arc<Mutex<ParquetRangeCacheMetrics>>,
    identities_by_resource: Arc<Mutex<BTreeMap<String, ParquetObjectIdentity>>>,
}

impl Default for ParquetRangeCache {
    fn default() -> Self {
        Self {
            extents: Arc::new(MemoryPersistentExtentCache::default()),
            metrics: Arc::new(Mutex::new(ParquetRangeCacheMetrics::default())),
            identities_by_resource: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl ParquetRangeCache {
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            extents: Arc::new(MemoryPersistentExtentCache::with_max_entries(max_entries)),
            metrics: Arc::new(Mutex::new(ParquetRangeCacheMetrics::default())),
            identities_by_resource: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn snapshot(&self) -> ParquetRangeCacheMetrics {
        self.metrics
            .lock()
            .expect("parquet range cache metrics should not be poisoned")
            .clone()
    }

    async fn load(
        &self,
        target: &ScanTarget,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, QueryError> {
        let Some((key, extent)) = self.cache_key_and_extent(target, &range)? else {
            self.record_degraded_identity_read()?;
            return Ok(None);
        };
        self.record_identity_seen(
            &key.resource,
            target
                .object_identity()
                .as_ref()
                .expect("cache key requires identity"),
        )?;

        match self.extents.load_extent(&key, extent)? {
            Some(entry) => {
                self.record_hit()?;
                Ok(Some(entry.slice(extent)?))
            }
            None => {
                self.record_miss()?;
                Ok(None)
            }
        }
    }

    async fn store(
        &self,
        target: &ScanTarget,
        range: Range<u64>,
        bytes: Bytes,
    ) -> Result<(), QueryError> {
        let Some((key, extent)) = self.cache_key_and_extent(target, &range)? else {
            return Ok(());
        };
        let entry = ExtentCacheEntry::new(key, extent, bytes)?;
        self.extents.store_extent(&entry)?;
        self.record_store()?;
        Ok(())
    }

    fn cache_key_and_extent(
        &self,
        target: &ScanTarget,
        range: &Range<u64>,
    ) -> Result<Option<(ExtentCacheKey, ByteExtent)>, QueryError> {
        let length = range_len(range)?;
        if length == 0 {
            return Ok(None);
        }
        let Some(identity) = target.object_identity() else {
            return Ok(None);
        };
        let identity = parquet_range_cache_identity(&identity);
        let key = ExtentCacheKey::new(canonical_parquet_cache_resource(target), Some(identity));
        let extent = ByteExtent::new(range.start, length)?;
        Ok(Some((key, extent)))
    }

    fn record_identity_seen(
        &self,
        resource: &str,
        identity: &ParquetObjectIdentity,
    ) -> Result<(), QueryError> {
        let mut identities = self
            .identities_by_resource
            .lock()
            .map_err(|_| execution_runtime_error("parquet range cache identities were poisoned"))?;
        if identities
            .get(resource)
            .is_some_and(|previous| previous != identity)
        {
            self.record_identity_drift_miss()?;
        }
        identities.insert(resource.to_string(), identity.clone());
        Ok(())
    }

    fn record_hit(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet range cache metrics were poisoned"))?;
        metrics.range_cache_hits = metrics.range_cache_hits.saturating_add(1);
        Ok(())
    }

    fn record_miss(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet range cache metrics were poisoned"))?;
        metrics.range_cache_misses = metrics.range_cache_misses.saturating_add(1);
        Ok(())
    }

    fn record_store(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet range cache metrics were poisoned"))?;
        metrics.range_cache_stores = metrics.range_cache_stores.saturating_add(1);
        Ok(())
    }

    fn record_degraded_identity_read(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet range cache metrics were poisoned"))?;
        metrics.range_cache_degraded_identity_reads = metrics
            .range_cache_degraded_identity_reads
            .saturating_add(1);
        Ok(())
    }

    fn record_identity_drift_miss(&self) -> Result<(), QueryError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| execution_runtime_error("parquet range cache metrics were poisoned"))?;
        metrics.range_cache_identity_drift_misses =
            metrics.range_cache_identity_drift_misses.saturating_add(1);
        Ok(())
    }
}

fn parquet_range_cache_identity(identity: &ParquetObjectIdentity) -> String {
    format!("etag={};size={}", identity.object_etag, identity.size_bytes)
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
    pub coalesced_range_reads: u64,
    pub coalesced_gap_bytes_fetched: u64,
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

    pub fn record_coalesced_range_read(&mut self, gap_bytes_fetched: u64) {
        self.coalesced_range_reads = self.coalesced_range_reads.saturating_add(1);
        self.coalesced_gap_bytes_fetched = self
            .coalesced_gap_bytes_fetched
            .saturating_add(gap_bytes_fetched);
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
        coalesced_range_reads: left
            .coalesced_range_reads
            .saturating_add(right.coalesced_range_reads),
        coalesced_gap_bytes_fetched: left
            .coalesced_gap_bytes_fetched
            .saturating_add(right.coalesced_gap_bytes_fetched),
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

    fn record_coalesced_range_read(&self, gap_bytes_fetched: u64) -> Result<(), QueryError> {
        let mut snapshot = self.snapshot.lock().map_err(|_| {
            execution_runtime_error("scan target metrics handle was poisoned during execution")
        })?;
        snapshot
            .range_read_metrics
            .record_coalesced_range_read(gap_bytes_fetched);
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

#[derive(Clone, Debug)]
struct PlannedLogicalRange {
    index: usize,
    range: Range<u64>,
}

impl PlannedLogicalRange {
    fn len(&self) -> Result<u64, QueryError> {
        range_len(&self.range)
    }
}

#[derive(Clone, Debug)]
struct PlannedUnionSpan {
    range: Range<u64>,
    logical_ranges: Vec<PlannedLogicalRange>,
}

impl PlannedUnionSpan {
    fn len(&self) -> Result<u64, QueryError> {
        range_len(&self.range)
    }
}

#[derive(Clone, Debug)]
struct PlannedRangeRead {
    physical_range: Range<u64>,
    logical_ranges: Vec<PlannedLogicalRange>,
    coalesced_gap_bytes: u64,
}

impl PlannedRangeRead {
    fn exact(logical: PlannedLogicalRange) -> Self {
        Self {
            physical_range: logical.range.clone(),
            logical_ranges: vec![logical],
            coalesced_gap_bytes: 0,
        }
    }

    fn is_coalesced(&self) -> bool {
        self.logical_ranges.len() > 1
    }
}

fn plan_range_reads(
    ranges: Vec<Range<u64>>,
    allow_coalescing: bool,
    object_size_bytes: u64,
) -> Result<Vec<PlannedRangeRead>, QueryError> {
    if ranges.len() < 2 || !allow_coalescing {
        return Ok(exact_range_read_plan(ranges));
    }

    let mut logical = Vec::with_capacity(ranges.len());
    for (index, range) in ranges.iter().cloned().enumerate() {
        let length = range_len(&range)?;
        if length == 0 || range.end > object_size_bytes {
            return Ok(exact_range_read_plan(ranges));
        }
        logical.push(PlannedLogicalRange { index, range });
    }

    logical.sort_by(|left, right| {
        left.range
            .start
            .cmp(&right.range.start)
            .then_with(|| left.range.end.cmp(&right.range.end))
            .then_with(|| left.index.cmp(&right.index))
    });

    let spans = merged_union_spans(logical)?;
    let grouped = coalesced_range_read_plan(spans)?;
    Ok(grouped)
}

fn exact_range_read_plan(ranges: Vec<Range<u64>>) -> Vec<PlannedRangeRead> {
    ranges
        .into_iter()
        .enumerate()
        .map(|(index, range)| PlannedRangeRead::exact(PlannedLogicalRange { index, range }))
        .collect()
}

fn merged_union_spans(
    logical_ranges: Vec<PlannedLogicalRange>,
) -> Result<Vec<PlannedUnionSpan>, QueryError> {
    let mut spans: Vec<PlannedUnionSpan> = Vec::new();
    for logical in logical_ranges {
        if let Some(current) = spans.last_mut() {
            if logical.range.start <= current.range.end {
                current.range.end = current.range.end.max(logical.range.end);
                current.logical_ranges.push(logical);
                continue;
            }
        }
        spans.push(PlannedUnionSpan {
            range: logical.range.clone(),
            logical_ranges: vec![logical],
        });
    }
    Ok(spans)
}

fn coalesced_range_read_plan(
    spans: Vec<PlannedUnionSpan>,
) -> Result<Vec<PlannedRangeRead>, QueryError> {
    let mut plan = Vec::new();
    let mut current = PlannedCoalescingGroup::new();
    for span in spans {
        if current.is_empty() {
            current.push_first(span)?;
            continue;
        }
        if current.can_push(&span)? {
            current.push_next(span)?;
        } else {
            plan.extend(current.finish()?);
            current = PlannedCoalescingGroup::new();
            current.push_first(span)?;
        }
    }
    if !current.is_empty() {
        plan.extend(current.finish()?);
    }
    Ok(plan)
}

#[derive(Clone, Debug, Default)]
struct PlannedCoalescingGroup {
    spans: Vec<PlannedUnionSpan>,
    requested_union_bytes: u64,
    gap_bytes: u64,
}

impl PlannedCoalescingGroup {
    fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    fn push_first(&mut self, span: PlannedUnionSpan) -> Result<(), QueryError> {
        self.requested_union_bytes = span.len()?;
        self.spans.push(span);
        Ok(())
    }

    fn can_push(&self, span: &PlannedUnionSpan) -> Result<bool, QueryError> {
        let candidate = self.candidate_policy(span)?;
        Ok(
            candidate.individual_gap_bytes <= MAX_COALESCED_INDIVIDUAL_GAP_BYTES
                && candidate.gap_bytes <= MAX_COALESCED_CUMULATIVE_GAP_BYTES
                && candidate.physical_length <= MAX_COALESCED_PHYSICAL_RANGE_BYTES
                && candidate
                    .gap_bytes
                    .saturating_mul(MAX_COALESCED_GAP_AMPLIFICATION_DENOMINATOR)
                    <= candidate
                        .requested_union_bytes
                        .saturating_mul(MAX_COALESCED_GAP_AMPLIFICATION_NUMERATOR),
        )
    }

    fn push_next(&mut self, span: PlannedUnionSpan) -> Result<(), QueryError> {
        let candidate = self.candidate_policy(&span)?;
        self.requested_union_bytes = candidate.requested_union_bytes;
        self.gap_bytes = candidate.gap_bytes;
        self.spans.push(span);
        Ok(())
    }

    fn candidate_policy(
        &self,
        span: &PlannedUnionSpan,
    ) -> Result<CandidateCoalescingPolicy, QueryError> {
        let current_end = self.physical_end()?;
        let individual_gap_bytes = span
            .range
            .start
            .checked_sub(current_end)
            .ok_or_else(|| execution_runtime_error("coalesced range spans were not sorted"))?;
        let gap_bytes = self
            .gap_bytes
            .checked_add(individual_gap_bytes)
            .ok_or_else(|| execution_runtime_error("coalesced range gap bytes overflowed u64"))?;
        let requested_union_bytes = self
            .requested_union_bytes
            .checked_add(span.len()?)
            .ok_or_else(|| execution_runtime_error("coalesced requested bytes overflowed u64"))?;
        let physical_start = self.physical_start()?;
        let physical_length = span
            .range
            .end
            .checked_sub(physical_start)
            .ok_or_else(|| execution_runtime_error("coalesced range length underflowed u64"))?;
        Ok(CandidateCoalescingPolicy {
            individual_gap_bytes,
            gap_bytes,
            requested_union_bytes,
            physical_length,
        })
    }

    fn physical_start(&self) -> Result<u64, QueryError> {
        self.spans
            .first()
            .map(|span| span.range.start)
            .ok_or_else(|| execution_runtime_error("coalesced range group was empty"))
    }

    fn physical_end(&self) -> Result<u64, QueryError> {
        self.spans
            .last()
            .map(|span| span.range.end)
            .ok_or_else(|| execution_runtime_error("coalesced range group was empty"))
    }

    fn finish(self) -> Result<Vec<PlannedRangeRead>, QueryError> {
        let physical_range = self.physical_start()?..self.physical_end()?;
        let logical_ranges = self
            .spans
            .into_iter()
            .flat_map(|span| span.logical_ranges)
            .collect::<Vec<_>>();
        if logical_ranges.len() == 1 {
            let logical = logical_ranges
                .into_iter()
                .next()
                .expect("single logical range should exist");
            return Ok(vec![PlannedRangeRead::exact(logical)]);
        }
        if range_len(&physical_range)? > MAX_COALESCED_PHYSICAL_RANGE_BYTES {
            return Ok(logical_ranges
                .into_iter()
                .map(PlannedRangeRead::exact)
                .collect());
        }
        Ok(vec![PlannedRangeRead {
            physical_range,
            logical_ranges,
            coalesced_gap_bytes: self.gap_bytes,
        }])
    }
}

#[derive(Clone, Copy, Debug)]
struct CandidateCoalescingPolicy {
    individual_gap_bytes: u64,
    gap_bytes: u64,
    requested_union_bytes: u64,
    physical_length: u64,
}

fn range_len(range: &Range<u64>) -> Result<u64, QueryError> {
    range
        .end
        .checked_sub(range.start)
        .ok_or_else(|| execution_runtime_error("parquet byte range underflowed u64"))
}

#[derive(Clone)]
struct HttpRangeAsyncFileReader {
    reader: HttpRangeReader,
    target: ScanTarget,
    request_timeout: Option<Duration>,
    metadata_cache: Option<ParquetMetadataCache>,
    range_cache: Option<ParquetRangeCache>,
    metrics: SharedScanTargetMetricsHandle,
    count_metadata_fetches: bool,
    metadata_probe_round_trips: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhysicalRangeReadSource {
    Cache,
    Network,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalRangeRead {
    bytes: Bytes,
    source: PhysicalRangeReadSource,
}

impl HttpRangeAsyncFileReader {
    fn new(
        reader: HttpRangeReader,
        target: ScanTarget,
        request_timeout: Option<Duration>,
        metadata_cache: Option<ParquetMetadataCache>,
        range_cache: Option<ParquetRangeCache>,
        metrics: SharedScanTargetMetricsHandle,
    ) -> Self {
        Self {
            reader,
            target,
            request_timeout,
            metadata_cache,
            range_cache,
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
        target: ScanTarget,
        validation: Option<HttpRangeValidation>,
        request_timeout: Option<Duration>,
        metrics: SharedScanTargetMetricsHandle,
        range_cache: Option<ParquetRangeCache>,
        phase: ParquetRangeReadPhase,
        range: Range<u64>,
    ) -> Result<PhysicalRangeRead, QueryError> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| execution_runtime_error("parquet byte range underflowed u64"))?;
        if length == 0 {
            return Ok(PhysicalRangeRead {
                bytes: Bytes::new(),
                source: PhysicalRangeReadSource::Network,
            });
        }

        if let Some(cache) = &range_cache {
            if let Some(bytes) = cache.load(&target, range.clone()).await? {
                metrics.record_range_read(
                    phase,
                    target.path.clone(),
                    target.object_identity(),
                    range.start,
                    length,
                )?;
                return Ok(PhysicalRangeRead {
                    bytes,
                    source: PhysicalRangeReadSource::Cache,
                });
            }
        }

        let read = reader
            .read_range_with_validation(
                &target.object_source.url,
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
        metrics.record_range_read(
            phase,
            target.path.clone(),
            target.object_identity(),
            range.start,
            length,
        )?;
        if let Some(cache) = &range_cache {
            cache.store(&target, range, read.bytes.clone()).await?;
        }
        Ok(PhysicalRangeRead {
            bytes: read.bytes,
            source: PhysicalRangeReadSource::Network,
        })
    }

    async fn fetch_coalesced_range_owned(
        reader: HttpRangeReader,
        target: ScanTarget,
        validation: HttpRangeValidation,
        request_timeout: Option<Duration>,
        metrics: SharedScanTargetMetricsHandle,
        range_cache: Option<ParquetRangeCache>,
        range: Range<u64>,
    ) -> Result<PhysicalRangeRead, QueryError> {
        let length = range_len(&range)?;
        if length == 0 {
            return Ok(PhysicalRangeRead {
                bytes: Bytes::new(),
                source: PhysicalRangeReadSource::Network,
            });
        }

        if let Some(cache) = &range_cache {
            if let Some(bytes) = cache.load(&target, range.clone()).await? {
                return Ok(PhysicalRangeRead {
                    bytes,
                    source: PhysicalRangeReadSource::Cache,
                });
            }
        }

        let read = reader
            .read_range_with_validation(
                &target.object_source.url,
                HttpByteRange::Bounded {
                    offset: range.start,
                    length,
                },
                Some(validation),
                request_timeout,
            )
            .await?;
        let observed_object_size = read.metadata.size_bytes.ok_or_else(|| {
            parquet_protocol_error("coalesced range response did not report object size metadata")
        })?;
        if observed_object_size != target.size_bytes {
            return Err(parquet_protocol_error(format!(
                "coalesced range response expected object size {} bytes, but the response reported {observed_object_size} bytes",
                target.size_bytes
            )));
        }
        metrics.record_bytes_fetched(
            u64::try_from(read.bytes.len())
                .map_err(|_| execution_runtime_error("scan byte totals overflowed u64"))?,
        )?;
        if let Some(cache) = &range_cache {
            cache.store(&target, range, read.bytes.clone()).await?;
        }
        Ok(PhysicalRangeRead {
            bytes: read.bytes,
            source: PhysicalRangeReadSource::Network,
        })
    }

    async fn fetch_byte_ranges_owned(
        reader: HttpRangeReader,
        target: ScanTarget,
        validation: Option<HttpRangeValidation>,
        request_timeout: Option<Duration>,
        metrics: SharedScanTargetMetricsHandle,
        range_cache: Option<ParquetRangeCache>,
        phase: ParquetRangeReadPhase,
        ranges: Vec<Range<u64>>,
    ) -> Result<Vec<Bytes>, QueryError> {
        let plan = plan_range_reads(ranges.clone(), validation.is_some(), target.size_bytes)?;
        let mut outputs = vec![None; ranges.len()];
        for planned in plan {
            if planned.is_coalesced() {
                let validation = validation.clone().ok_or_else(|| {
                    execution_runtime_error("coalesced parquet reads require an If-Range validator")
                })?;
                let physical_start = planned.physical_range.start;
                let read = Self::fetch_coalesced_range_owned(
                    reader.clone(),
                    target.clone(),
                    validation,
                    request_timeout,
                    metrics.clone(),
                    range_cache.clone(),
                    planned.physical_range.clone(),
                )
                .await?;
                let bytes = read.bytes;

                let mut slices = Vec::with_capacity(planned.logical_ranges.len());
                for logical in &planned.logical_ranges {
                    let slice_start =
                        logical
                            .range
                            .start
                            .checked_sub(physical_start)
                            .ok_or_else(|| {
                                execution_runtime_error(
                                    "coalesced logical range started before physical range",
                                )
                            })?;
                    let slice_end =
                        logical
                            .range
                            .end
                            .checked_sub(physical_start)
                            .ok_or_else(|| {
                                execution_runtime_error(
                                    "coalesced logical range ended before physical range",
                                )
                            })?;
                    let slice_start = usize::try_from(slice_start).map_err(|_| {
                        execution_runtime_error("coalesced slice start overflowed usize")
                    })?;
                    let slice_end = usize::try_from(slice_end).map_err(|_| {
                        execution_runtime_error("coalesced slice end overflowed usize")
                    })?;
                    if slice_end > bytes.len() || slice_start > slice_end {
                        return Err(execution_runtime_error(
                            "coalesced response bytes did not cover a logical range",
                        ));
                    }
                    slices.push((logical.index, bytes.slice(slice_start..slice_end)));
                }

                if read.source == PhysicalRangeReadSource::Network {
                    metrics.record_coalesced_range_read(planned.coalesced_gap_bytes)?;
                }
                for logical in &planned.logical_ranges {
                    metrics.record_range_read(
                        phase,
                        target.path.clone(),
                        target.object_identity(),
                        logical.range.start,
                        logical.len()?,
                    )?;
                }
                for (index, slice) in slices {
                    outputs[index] = Some(slice);
                }
            } else {
                let logical = planned
                    .logical_ranges
                    .into_iter()
                    .next()
                    .ok_or_else(|| execution_runtime_error("range read plan was empty"))?;
                let read = Self::fetch_range_owned(
                    reader.clone(),
                    target.clone(),
                    validation.clone(),
                    request_timeout,
                    metrics.clone(),
                    range_cache.clone(),
                    phase,
                    logical.range,
                )
                .await?;
                outputs[logical.index] = Some(read.bytes);
            }
        }

        outputs
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                bytes.ok_or_else(|| {
                    execution_runtime_error(format!(
                        "parquet byte range {index} did not produce output bytes"
                    ))
                })
            })
            .collect()
    }

    fn planned_physical_request_count(
        ranges: Vec<Range<u64>>,
        allow_coalescing: bool,
        object_size_bytes: u64,
    ) -> usize {
        plan_range_reads(ranges.clone(), allow_coalescing, object_size_bytes)
            .map(|plan| plan.len())
            .unwrap_or(ranges.len())
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
            let target = self.target.clone();
            let validation = self.validation();
            let request_timeout = self.request_timeout;
            let metrics = self.metrics.clone();
            let range_cache = self.range_cache.clone();
            let phase = if self.count_metadata_fetches {
                ParquetRangeReadPhase::ScanFooter
            } else {
                ParquetRangeReadPhase::ScanData
            };
            let (future, handle) = async move {
                Self::fetch_range_owned(
                    reader,
                    target,
                    validation,
                    request_timeout,
                    metrics,
                    range_cache,
                    phase,
                    range,
                )
                .await
                .map(|read| read.bytes)
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
                self.target.clone(),
                self.validation(),
                self.request_timeout,
                self.metrics.clone(),
                self.range_cache.clone(),
                phase,
                range,
            )
            .await
            .map(|read| read.bytes)
            .map_err(parquet_error_from_query_error)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let validation = self.validation();
        if self.count_metadata_fetches {
            let request_count = Self::planned_physical_request_count(
                ranges.clone(),
                validation.is_some(),
                self.target.size_bytes,
            );
            self.metadata_probe_round_trips = self
                .metadata_probe_round_trips
                .checked_add(
                    u64::try_from(request_count)
                        .expect("metadata request counts should fit in u64"),
                )
                .expect("metadata probe round trips should not overflow");
        }
        let phase = if self.count_metadata_fetches {
            ParquetRangeReadPhase::ScanFooter
        } else {
            ParquetRangeReadPhase::ScanData
        };

        #[cfg(target_arch = "wasm32")]
        {
            let reader = self.reader.clone();
            let target = self.target.clone();
            let request_timeout = self.request_timeout;
            let metrics = self.metrics.clone();
            let range_cache = self.range_cache.clone();
            let (future, handle) = async move {
                Self::fetch_byte_ranges_owned(
                    reader,
                    target,
                    validation,
                    request_timeout,
                    metrics,
                    range_cache,
                    phase,
                    ranges,
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
            Self::fetch_byte_ranges_owned(
                self.reader.clone(),
                self.target.clone(),
                validation,
                self.request_timeout,
                self.metrics.clone(),
                self.range_cache.clone(),
                phase,
                ranges,
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
    stream_scan_target_batches_with_row_group_pruning_and_caches(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        row_group_predicate,
        None,
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
    stream_scan_target_batches_with_row_group_pruning_and_caches(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        row_group_predicate,
        cache,
        None,
    )
    .await
}

pub async fn stream_scan_target_batches_with_row_group_pruning_and_caches(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    row_group_predicate: Option<&ParquetRowGroupPruningPredicate>,
    metadata_cache: Option<&ParquetMetadataCache>,
    range_cache: Option<&ParquetRangeCache>,
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
        metadata_cache.cloned(),
        range_cache.cloned(),
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
    metadata_cache: Option<ParquetMetadataCache>,
    range_cache: Option<ParquetRangeCache>,
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
    stream_scan_targets_with_caches(
        reader,
        targets,
        required_columns,
        partition_column_types,
        request_timeout,
        None,
        None,
    )
    .await
}

pub async fn stream_scan_targets_with_caches(
    reader: &HttpRangeReader,
    targets: &[ScanTarget],
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    metadata_cache: Option<&ParquetMetadataCache>,
    range_cache: Option<&ParquetRangeCache>,
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
        metadata_cache: metadata_cache.cloned(),
        range_cache: range_cache.cloned(),
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

            let stream = stream_scan_target_batches_with_row_group_pruning_and_caches(
                &state.reader,
                &target,
                &state.required_columns,
                &state.partition_column_types,
                state.request_timeout,
                None,
                state.metadata_cache.as_ref(),
                state.range_cache.as_ref(),
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
    scan_target_input_rows_with_caches(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        cache,
        None,
    )
    .await
}

pub async fn scan_target_input_rows_with_caches(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    metadata_cache: Option<&ParquetMetadataCache>,
    range_cache: Option<&ParquetRangeCache>,
) -> Result<Vec<ParquetInputRow>, QueryError> {
    let stream = stream_scan_target_batches_with_row_group_pruning_and_caches(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        None,
        metadata_cache,
        range_cache,
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
        merge_parquet_range_read_metrics, HttpRangeAsyncFileReader, ObjectSource,
        ParquetObjectIdentity, ParquetRangeCache, ParquetRangeReadKey, ParquetRangeReadMetrics,
        ParquetRangeReadPhase, ScanTarget, SharedScanTargetMetricsHandle,
    };
    use crate::{ScanTargetMetricsHandle, ScanTargetMetricsSnapshot};
    use parquet::arrow::async_reader::AsyncFileReader;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use wasm_http_object_store::HttpRangeReader;

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

    #[test]
    fn range_read_metric_merge_sums_coalesced_physical_counters() {
        let mut left = ParquetRangeReadMetrics::default();
        left.record_coalesced_range_read(32);
        let mut right = ParquetRangeReadMetrics::default();
        right.record_coalesced_range_read(48);

        let merged = merge_parquet_range_read_metrics(&left, &right);

        assert_eq!(merged.coalesced_range_reads, 2);
        assert_eq!(merged.coalesced_gap_bytes_fetched, 80);
    }

    #[tokio::test]
    async fn nearby_small_gap_byte_ranges_coalesce_into_one_if_range_request() {
        let body = test_body(128 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));

        let ranges = vec![1_024..33_792, 37_888..70_656];
        let chunks = reader
            .get_byte_ranges(ranges.clone())
            .await
            .expect("coalesced ranges should read");

        assert_eq!(chunks, expected_chunks(&body, &ranges));
        let requests = server.recorded_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].headers.get("range"),
            Some(&"bytes=1024-70655".to_string())
        );
        assert_eq!(
            requests[0].headers.get("if-range"),
            Some(&"\"object-v1\"".to_string())
        );
        let metrics = reader.metrics.snapshot();
        assert_eq!(metrics.range_read_metrics.scan_data_range_reads, 2);
        assert_eq!(metrics.range_read_metrics.coalesced_range_reads, 1);
        assert_eq!(
            metrics.range_read_metrics.coalesced_gap_bytes_fetched,
            4_096
        );
        assert_eq!(metrics.bytes_fetched, 69_632);
    }

    #[tokio::test]
    async fn coalesced_physical_read_satisfies_later_overlapping_get_bytes_from_range_cache() {
        let body = test_body(16 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let cache = ParquetRangeCache::default();
        let mut reader =
            test_async_reader_with_range_cache(&server, body.len(), Some("\"object-v1\""), cache);

        let first_ranges = vec![1_024..2_048, 2_560..3_584];
        let chunks = reader
            .get_byte_ranges(first_ranges.clone())
            .await
            .expect("coalesced ranges should read");
        assert_eq!(chunks, expected_chunks(&body, &first_ranges));

        let overlapping = reader
            .get_bytes(2_000..2_800)
            .await
            .expect("overlapping logical read should reuse cached physical bytes");

        assert_eq!(
            overlapping,
            bytes::Bytes::copy_from_slice(&body[2_000..2_800])
        );
        assert_eq!(server.recorded_requests().len(), 1);
        let metrics = reader.metrics.snapshot();
        assert_eq!(metrics.bytes_fetched, 2_560);
        assert_eq!(metrics.range_read_metrics.scan_data_range_reads, 3);
        let cache_metrics = reader
            .range_cache
            .as_ref()
            .expect("test reader should have a range cache")
            .snapshot();
        assert_eq!(cache_metrics.range_cache_hits, 1);
        assert_eq!(cache_metrics.range_cache_misses, 1);
        assert_eq!(cache_metrics.range_cache_stores, 1);
    }

    #[tokio::test]
    async fn changed_strong_identity_refetches_overlapping_range_without_stale_cache_reuse() {
        let body = test_body(16 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let cache = ParquetRangeCache::default();
        let mut first_reader = test_async_reader_with_range_cache(
            &server,
            body.len(),
            Some("\"object-v1\""),
            cache.clone(),
        );
        first_reader
            .get_bytes(1_024..2_048)
            .await
            .expect("first strong-identity range should read");

        server.set_etag(Some("\"object-v2\"".to_string()));
        let mut second_reader = test_async_reader_with_range_cache(
            &server,
            body.len(),
            Some("\"object-v2\""),
            cache.clone(),
        );
        let overlapping = second_reader
            .get_bytes(1_536..1_792)
            .await
            .expect("changed identity must refetch overlapping bytes");

        assert_eq!(
            overlapping,
            bytes::Bytes::copy_from_slice(&body[1_536..1_792])
        );
        assert_eq!(server.recorded_requests().len(), 2);
        let cache_metrics = cache.snapshot();
        assert_eq!(cache_metrics.range_cache_hits, 0);
        assert_eq!(cache_metrics.range_cache_misses, 2);
        assert_eq!(cache_metrics.range_cache_identity_drift_misses, 1);
    }

    #[tokio::test]
    async fn changed_size_identity_refetches_overlapping_range_without_stale_cache_reuse() {
        let body = test_body(16 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let cache = ParquetRangeCache::default();
        let mut first_reader = test_async_reader_with_range_cache(
            &server,
            body.len(),
            Some("\"object-v1\""),
            cache.clone(),
        );
        first_reader
            .get_bytes(1_024..2_048)
            .await
            .expect("first strong-identity range should read");

        let mut second_reader = test_async_reader_with_range_cache(
            &server,
            body.len() + 1,
            Some("\"object-v1\""),
            cache.clone(),
        );
        let overlapping = second_reader
            .get_bytes(1_536..1_792)
            .await
            .expect("changed size identity must refetch overlapping bytes");

        assert_eq!(
            overlapping,
            bytes::Bytes::copy_from_slice(&body[1_536..1_792])
        );
        assert_eq!(server.recorded_requests().len(), 2);
        let cache_metrics = cache.snapshot();
        assert_eq!(cache_metrics.range_cache_hits, 0);
        assert_eq!(cache_metrics.range_cache_misses, 2);
        assert_eq!(cache_metrics.range_cache_identity_drift_misses, 1);
    }

    #[tokio::test]
    async fn weak_identity_skips_range_cache_lookup_and_store() {
        let body = test_body(16 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("W/\"object-v1\"".to_string()));
        let cache = ParquetRangeCache::default();
        let mut reader = test_async_reader_with_range_cache(
            &server,
            body.len(),
            Some("W/\"object-v1\""),
            cache.clone(),
        );

        let first = reader
            .get_bytes(1_024..2_048)
            .await
            .expect("weak identity range should read");
        let second = reader
            .get_bytes(1_536..1_792)
            .await
            .expect("weak identity overlap should refetch");

        assert_eq!(first, bytes::Bytes::copy_from_slice(&body[1_024..2_048]));
        assert_eq!(second, bytes::Bytes::copy_from_slice(&body[1_536..1_792]));
        assert_eq!(server.recorded_requests().len(), 2);
        let cache_metrics = cache.snapshot();
        assert_eq!(cache_metrics.range_cache_hits, 0);
        assert_eq!(cache_metrics.range_cache_misses, 0);
        assert_eq!(cache_metrics.range_cache_stores, 0);
        assert_eq!(cache_metrics.range_cache_degraded_identity_reads, 2);
    }

    #[tokio::test]
    async fn missing_identity_skips_range_cache_lookup_and_store() {
        let body = test_body(16 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let cache = ParquetRangeCache::default();
        let mut reader =
            test_async_reader_with_range_cache(&server, body.len(), None, cache.clone());

        let first = reader
            .get_bytes(1_024..2_048)
            .await
            .expect("missing identity range should read");
        let second = reader
            .get_bytes(1_536..1_792)
            .await
            .expect("missing identity overlap should refetch");

        assert_eq!(first, bytes::Bytes::copy_from_slice(&body[1_024..2_048]));
        assert_eq!(second, bytes::Bytes::copy_from_slice(&body[1_536..1_792]));
        assert_eq!(server.recorded_requests().len(), 2);
        let cache_metrics = cache.snapshot();
        assert_eq!(cache_metrics.range_cache_hits, 0);
        assert_eq!(cache_metrics.range_cache_misses, 0);
        assert_eq!(cache_metrics.range_cache_stores, 0);
        assert_eq!(cache_metrics.range_cache_degraded_identity_reads, 2);
    }

    #[tokio::test]
    async fn large_gap_byte_ranges_do_not_coalesce() {
        let body = test_body(128 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));

        let ranges = vec![1_024..33_792, 50_177..82_945];
        let chunks = reader
            .get_byte_ranges(ranges.clone())
            .await
            .expect("exact ranges should read");

        assert_eq!(chunks, expected_chunks(&body, &ranges));
        let requests = server.recorded_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].headers.get("range"),
            Some(&"bytes=1024-33791".to_string())
        );
        assert_eq!(
            requests[1].headers.get("range"),
            Some(&"bytes=50177-82944".to_string())
        );
        let metrics = reader.metrics.snapshot();
        assert_eq!(metrics.range_read_metrics.scan_data_range_reads, 2);
        assert_eq!(metrics.range_read_metrics.coalesced_range_reads, 0);
        assert_eq!(metrics.range_read_metrics.coalesced_gap_bytes_fetched, 0);
    }

    #[tokio::test]
    async fn missing_and_weak_etags_decline_coalescing() {
        for etag in [None, Some("W/\"object-v1\"")] {
            let body = test_body(64 * 1024);
            let server = RangeCoalescingServer::new(body.clone(), etag.map(str::to_string));
            let mut reader = test_async_reader(&server, body.len(), etag);
            let ranges = vec![256..512, 768..1_024];

            let chunks = reader
                .get_byte_ranges(ranges.clone())
                .await
                .expect("exact ranges should read without strong identity");

            assert_eq!(chunks, expected_chunks(&body, &ranges));
            let requests = server.recorded_requests();
            assert_eq!(requests.len(), 2);
            assert!(requests
                .iter()
                .all(|request| !request.headers.contains_key("if-range")));
            assert_eq!(
                reader
                    .metrics
                    .snapshot()
                    .range_read_metrics
                    .coalesced_range_reads,
                0
            );
        }
    }

    #[tokio::test]
    async fn coalesced_reads_fail_closed_on_non_partial_or_malformed_responses() {
        for behavior in [
            RangeResponseBehavior::FullObject200,
            RangeResponseBehavior::WrongContentRange,
            RangeResponseBehavior::ShortBody,
        ] {
            let body = test_body(64 * 1024);
            let server = RangeCoalescingServer::new_with_behavior(
                body,
                Some("\"object-v1\"".to_string()),
                behavior,
            );
            let mut reader = test_async_reader(&server, 64 * 1024, Some("\"object-v1\""));

            let error = reader
                .get_byte_ranges(vec![1_024..2_048, 3_072..4_096])
                .await
                .expect_err("invalid coalesced responses must fail closed");

            assert!(
                error.to_string().contains("Partial Content")
                    || error.to_string().contains("Content-Range")
                    || error.to_string().contains("returned"),
                "unexpected error: {error}"
            );
            assert_eq!(server.recorded_requests().len(), 1);
            assert_eq!(
                reader
                    .metrics
                    .snapshot()
                    .range_read_metrics
                    .coalesced_range_reads,
                0
            );
            assert_eq!(
                reader
                    .metrics
                    .snapshot()
                    .range_read_metrics
                    .scan_data_range_reads,
                0
            );
        }
    }

    #[tokio::test]
    async fn unsorted_overlapping_and_duplicate_inputs_preserve_outputs_and_gap_accounting() {
        let body = test_body(128 * 1024);
        let server = RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
        let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));

        let ranges = vec![36_000..76_000, 1_000..21_000, 20_000..25_000, 1_000..21_000];
        let chunks = reader
            .get_byte_ranges(ranges.clone())
            .await
            .expect("overlapping ranges should slice out of coalesced data");

        assert_eq!(chunks, expected_chunks(&body, &ranges));
        let requests = server.recorded_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].headers.get("range"),
            Some(&"bytes=1000-75999".to_string())
        );
        let metrics = reader.metrics.snapshot();
        assert_eq!(metrics.range_read_metrics.scan_data_range_reads, 4);
        assert_eq!(metrics.range_read_metrics.duplicate_range_reads, 1);
        assert_eq!(metrics.range_read_metrics.coalesced_range_reads, 1);
        assert_eq!(
            metrics.range_read_metrics.coalesced_gap_bytes_fetched,
            11_000
        );
    }

    #[tokio::test]
    async fn coalescing_policy_boundaries_are_enforced() {
        let body = test_body(1024 * 1024);
        for (gap, expected_requests) in [(16 * 1024, 1), (16 * 1024 + 1, 2)] {
            let server =
                RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
            let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));
            reader
                .get_byte_ranges(vec![0..65_536, 65_536 + gap..65_536 + gap + 65_536])
                .await
                .expect("boundary ranges should read");
            assert_eq!(server.recorded_requests().len(), expected_requests);
        }

        let exact_cumulative_gap = vec![
            0..65_536,
            81_920..147_456,
            163_840..229_376,
            245_760..311_296,
            327_680..393_216,
        ];
        let just_over_cumulative_gap = vec![
            0..65_536,
            81_920..147_456,
            163_840..229_376,
            245_760..311_296,
            327_680..393_216,
            393_217..458_753,
        ];
        for (ranges, expected_requests) in
            [(exact_cumulative_gap, 1), (just_over_cumulative_gap, 2)]
        {
            let server =
                RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
            let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));
            reader
                .get_byte_ranges(ranges)
                .await
                .expect("cumulative gap boundary ranges should read");
            assert_eq!(server.recorded_requests().len(), expected_requests);
        }

        for (gap, expected_requests) in [(512, 1), (513, 2)] {
            let server =
                RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
            let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));
            reader
                .get_byte_ranges(vec![0..1_024, 1_024 + gap..2_048 + gap])
                .await
                .expect("amplification boundary ranges should read");
            assert_eq!(server.recorded_requests().len(), expected_requests);
        }

        for (second_end, expected_requests) in [(524_288, 1), (524_289, 2)] {
            let server =
                RangeCoalescingServer::new(body.clone(), Some("\"object-v1\"".to_string()));
            let mut reader = test_async_reader(&server, body.len(), Some("\"object-v1\""));
            reader
                .get_byte_ranges(vec![0..262_144, 262_144..second_end])
                .await
                .expect("physical size boundary ranges should read");
            assert_eq!(server.recorded_requests().len(), expected_requests);
        }
    }

    #[test]
    fn small_gap_coalescing_threshold_sweep_records_request_and_amplification_tradeoffs() {
        assert_eq!(super::MAX_COALESCED_INDIVIDUAL_GAP_BYTES, 16 * 1024);
        assert_eq!(super::MAX_COALESCED_CUMULATIVE_GAP_BYTES, 64 * 1024);
        assert_eq!(super::MAX_COALESCED_GAP_AMPLIFICATION_NUMERATOR, 1);
        assert_eq!(super::MAX_COALESCED_GAP_AMPLIFICATION_DENOMINATOR, 4);
        assert_eq!(super::MAX_COALESCED_PHYSICAL_RANGE_BYTES, 512 * 1024);

        let observations = range_coalescing_threshold_sweep();

        assert_eq!(
            observations,
            vec![
                ThresholdSweepObservation {
                    label: "nearby_page_ranges",
                    logical_range_count: 2,
                    exact_request_count: 2,
                    physical_request_count: 1,
                    logical_bytes: 65_536,
                    planned_physical_bytes: 69_632,
                    candidate_gap_bytes: 4_096,
                    fetched_gap_bytes: 4_096,
                    candidate_gap_amplification_basis_points: 625,
                },
                ThresholdSweepObservation {
                    label: "cumulative_gap_at_limit",
                    logical_range_count: 5,
                    exact_request_count: 5,
                    physical_request_count: 1,
                    logical_bytes: 327_680,
                    planned_physical_bytes: 393_216,
                    candidate_gap_bytes: 65_536,
                    fetched_gap_bytes: 65_536,
                    candidate_gap_amplification_basis_points: 2_000,
                },
                ThresholdSweepObservation {
                    label: "amplification_at_limit",
                    logical_range_count: 2,
                    exact_request_count: 2,
                    physical_request_count: 1,
                    logical_bytes: 2_048,
                    planned_physical_bytes: 2_560,
                    candidate_gap_bytes: 512,
                    fetched_gap_bytes: 512,
                    candidate_gap_amplification_basis_points: 2_500,
                },
                ThresholdSweepObservation {
                    label: "individual_gap_just_over_limit",
                    logical_range_count: 2,
                    exact_request_count: 2,
                    physical_request_count: 2,
                    logical_bytes: 131_072,
                    planned_physical_bytes: 131_072,
                    candidate_gap_bytes: 16_385,
                    fetched_gap_bytes: 0,
                    candidate_gap_amplification_basis_points: 1_250,
                },
                ThresholdSweepObservation {
                    label: "cumulative_gap_just_over_limit",
                    logical_range_count: 6,
                    exact_request_count: 6,
                    physical_request_count: 2,
                    logical_bytes: 393_216,
                    planned_physical_bytes: 458_752,
                    candidate_gap_bytes: 65_537,
                    fetched_gap_bytes: 65_536,
                    candidate_gap_amplification_basis_points: 1_666,
                },
                ThresholdSweepObservation {
                    label: "amplification_just_over_limit",
                    logical_range_count: 2,
                    exact_request_count: 2,
                    physical_request_count: 2,
                    logical_bytes: 2_048,
                    planned_physical_bytes: 2_048,
                    candidate_gap_bytes: 513,
                    fetched_gap_bytes: 0,
                    candidate_gap_amplification_basis_points: 2_504,
                },
                ThresholdSweepObservation {
                    label: "physical_range_just_over_limit",
                    logical_range_count: 2,
                    exact_request_count: 2,
                    physical_request_count: 2,
                    logical_bytes: 524_289,
                    planned_physical_bytes: 524_289,
                    candidate_gap_bytes: 0,
                    fetched_gap_bytes: 0,
                    candidate_gap_amplification_basis_points: 0,
                },
            ]
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    struct ThresholdSweepObservation {
        label: &'static str,
        logical_range_count: usize,
        exact_request_count: usize,
        physical_request_count: usize,
        logical_bytes: u64,
        planned_physical_bytes: u64,
        candidate_gap_bytes: u64,
        fetched_gap_bytes: u64,
        candidate_gap_amplification_basis_points: u64,
    }

    fn range_coalescing_threshold_sweep() -> Vec<ThresholdSweepObservation> {
        [
            ("nearby_page_ranges", vec![1_024..33_792, 37_888..70_656]),
            (
                "cumulative_gap_at_limit",
                vec![
                    0..65_536,
                    81_920..147_456,
                    163_840..229_376,
                    245_760..311_296,
                    327_680..393_216,
                ],
            ),
            ("amplification_at_limit", vec![0..1_024, 1_536..2_560]),
            (
                "individual_gap_just_over_limit",
                vec![0..65_536, 81_921..147_457],
            ),
            (
                "cumulative_gap_just_over_limit",
                vec![
                    0..65_536,
                    81_920..147_456,
                    163_840..229_376,
                    245_760..311_296,
                    327_680..393_216,
                    393_217..458_753,
                ],
            ),
            (
                "amplification_just_over_limit",
                vec![0..1_024, 1_537..2_561],
            ),
            (
                "physical_range_just_over_limit",
                vec![0..262_144, 262_144..524_289],
            ),
        ]
        .into_iter()
        .map(|(label, ranges)| observe_threshold_case(label, ranges))
        .collect()
    }

    fn observe_threshold_case(
        label: &'static str,
        ranges: Vec<Range<u64>>,
    ) -> ThresholdSweepObservation {
        let object_size_bytes = ranges
            .iter()
            .map(|range| range.end)
            .max()
            .expect("sweep cases should include ranges");
        let plan = super::plan_range_reads(ranges.clone(), true, object_size_bytes)
            .expect("sweep ranges should plan successfully");
        let logical_bytes = ranges.iter().map(test_range_len).sum::<u64>();
        let planned_physical_bytes = plan
            .iter()
            .map(|read| test_range_len(&read.physical_range))
            .sum::<u64>();
        let candidate_gap_bytes = candidate_gap_bytes(&ranges);

        ThresholdSweepObservation {
            label,
            logical_range_count: ranges.len(),
            exact_request_count: ranges.len(),
            physical_request_count: plan.len(),
            logical_bytes,
            planned_physical_bytes,
            candidate_gap_bytes,
            fetched_gap_bytes: plan.iter().map(|read| read.coalesced_gap_bytes).sum(),
            candidate_gap_amplification_basis_points: candidate_gap_bytes.saturating_mul(10_000)
                / logical_bytes,
        }
    }

    fn candidate_gap_bytes(ranges: &[Range<u64>]) -> u64 {
        let mut merged = Vec::<Range<u64>>::new();
        let mut sorted = ranges.to_vec();
        sorted.sort_by(|left, right| {
            left.start
                .cmp(&right.start)
                .then_with(|| left.end.cmp(&right.end))
        });

        for range in sorted {
            if let Some(current) = merged.last_mut() {
                if range.start <= current.end {
                    current.end = current.end.max(range.end);
                    continue;
                }
            }
            merged.push(range);
        }

        let physical_start = merged
            .first()
            .expect("sweep cases should include ranges")
            .start;
        let physical_end = merged
            .last()
            .expect("sweep cases should include ranges")
            .end;
        let requested_union_bytes = merged.iter().map(test_range_len).sum::<u64>();

        physical_end
            .saturating_sub(physical_start)
            .saturating_sub(requested_union_bytes)
    }

    fn test_range_len(range: &Range<u64>) -> u64 {
        range
            .end
            .checked_sub(range.start)
            .expect("test range should be ascending")
    }

    fn test_async_reader(
        server: &RangeCoalescingServer,
        object_len: usize,
        object_etag: Option<&str>,
    ) -> HttpRangeAsyncFileReader {
        test_async_reader_with_optional_range_cache(server, object_len, object_etag, None)
    }

    fn test_async_reader_with_range_cache(
        server: &RangeCoalescingServer,
        object_len: usize,
        object_etag: Option<&str>,
        range_cache: ParquetRangeCache,
    ) -> HttpRangeAsyncFileReader {
        test_async_reader_with_optional_range_cache(
            server,
            object_len,
            object_etag,
            Some(range_cache),
        )
    }

    fn test_async_reader_with_optional_range_cache(
        server: &RangeCoalescingServer,
        object_len: usize,
        object_etag: Option<&str>,
        range_cache: Option<ParquetRangeCache>,
    ) -> HttpRangeAsyncFileReader {
        HttpRangeAsyncFileReader::new(
            HttpRangeReader::new(),
            ScanTarget {
                object_source: ObjectSource::new(server.url()),
                object_etag: object_etag.map(str::to_string),
                path: "part-000.parquet".to_string(),
                size_bytes: u64::try_from(object_len).expect("test body length should fit in u64"),
                partition_values: BTreeMap::new(),
            },
            None,
            None,
            range_cache,
            SharedScanTargetMetricsHandle::new(ScanTargetMetricsSnapshot::default()),
        )
    }

    fn test_body(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| u8::try_from(index % 251).expect("modulo should fit in u8"))
            .collect()
    }

    fn expected_chunks(body: &[u8], ranges: &[std::ops::Range<u64>]) -> Vec<bytes::Bytes> {
        ranges
            .iter()
            .map(|range| {
                let start = usize::try_from(range.start).expect("test range start should fit");
                let end = usize::try_from(range.end).expect("test range end should fit");
                bytes::Bytes::copy_from_slice(&body[start..end])
            })
            .collect()
    }

    #[derive(Clone, Debug)]
    struct CapturedRequest {
        headers: BTreeMap<String, String>,
    }

    #[derive(Clone, Copy, Debug)]
    enum RangeResponseBehavior {
        Normal,
        FullObject200,
        WrongContentRange,
        ShortBody,
    }

    struct TestResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    struct RangeCoalescingServer {
        address: std::net::SocketAddr,
        stop: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        etag: Arc<Mutex<Option<String>>>,
        thread: Option<JoinHandle<()>>,
    }

    impl RangeCoalescingServer {
        fn new(body: Vec<u8>, etag: Option<String>) -> Self {
            Self::new_with_behavior(body, etag, RangeResponseBehavior::Normal)
        }

        fn new_with_behavior(
            body: Vec<u8>,
            etag: Option<String>,
            behavior: RangeResponseBehavior,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
            listener
                .set_nonblocking(true)
                .expect("listener should allow nonblocking accept");
            let address = listener.local_addr().expect("listener addr should resolve");
            let stop = Arc::new(AtomicBool::new(false));
            let stop_for_thread = Arc::clone(&stop);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_for_thread = Arc::clone(&requests);
            let etag = Arc::new(Mutex::new(etag));
            let etag_for_thread = Arc::clone(&etag);

            let thread = thread::spawn(move || {
                while !stop_for_thread.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if stop_for_thread.load(Ordering::SeqCst) {
                                break;
                            }
                            stream
                                .set_nonblocking(false)
                                .expect("accepted streams should allow blocking reads");
                            let request = read_request(&mut stream);
                            requests_for_thread
                                .lock()
                                .expect("recorded requests should be writable")
                                .push(request.clone());
                            let etag = etag_for_thread
                                .lock()
                                .expect("test server ETag should be readable")
                                .clone();
                            write_response(
                                &mut stream,
                                ranged_response(&request, &body, etag.as_deref(), behavior),
                            );
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(std::time::Duration::from_millis(5));
                        }
                        Err(error) => panic!("test server accept should succeed: {error}"),
                    }
                }
            });

            Self {
                address,
                stop,
                requests,
                etag,
                thread: Some(thread),
            }
        }

        fn url(&self) -> String {
            format!("http://{}/object", self.address)
        }

        fn recorded_requests(&self) -> Vec<CapturedRequest> {
            self.requests
                .lock()
                .expect("recorded requests should be readable")
                .clone()
        }

        fn set_etag(&self, etag: Option<String>) {
            *self
                .etag
                .lock()
                .expect("test server ETag should be writable") = etag;
        }
    }

    impl Drop for RangeCoalescingServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = std::net::TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 512];
        loop {
            let read = stream
                .read(&mut chunk)
                .expect("test server should read request bytes");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request = String::from_utf8_lossy(&buffer);
        let headers = request
            .lines()
            .skip(1)
            .take_while(|line| !line.is_empty())
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
            })
            .collect();

        CapturedRequest { headers }
    }

    fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
        write!(stream, "HTTP/1.1 {}\r\n", response.status_line).expect("status line should write");
        for (header, value) in response.headers {
            write!(stream, "{header}: {value}\r\n").expect("header should write");
        }
        write!(stream, "\r\n").expect("header terminator should write");
        stream
            .write_all(&response.body)
            .expect("response body should write");
        stream.flush().expect("response should flush");
    }

    fn ranged_response(
        request: &CapturedRequest,
        body: &[u8],
        etag: Option<&str>,
        behavior: RangeResponseBehavior,
    ) -> TestResponse {
        let mut response = match request.headers.get("range") {
            Some(range_header) => {
                let (start, end) = resolve_range(range_header, body.len());
                let bounded = body[start..=end].to_vec();
                match behavior {
                    RangeResponseBehavior::Normal => TestResponse {
                        status_line: "206 Partial Content",
                        headers: vec![
                            ("Content-Length".to_string(), bounded.len().to_string()),
                            (
                                "Content-Range".to_string(),
                                format!("bytes {start}-{end}/{}", body.len()),
                            ),
                        ],
                        body: bounded,
                    },
                    RangeResponseBehavior::FullObject200 => TestResponse {
                        status_line: "200 OK",
                        headers: vec![("Content-Length".to_string(), body.len().to_string())],
                        body: body.to_vec(),
                    },
                    RangeResponseBehavior::WrongContentRange => TestResponse {
                        status_line: "206 Partial Content",
                        headers: vec![
                            ("Content-Length".to_string(), bounded.len().to_string()),
                            (
                                "Content-Range".to_string(),
                                format!("bytes {}-{}/{}", start + 1, end + 1, body.len()),
                            ),
                        ],
                        body: bounded,
                    },
                    RangeResponseBehavior::ShortBody => TestResponse {
                        status_line: "206 Partial Content",
                        headers: vec![
                            (
                                "Content-Length".to_string(),
                                bounded.len().saturating_sub(1).to_string(),
                            ),
                            (
                                "Content-Range".to_string(),
                                format!("bytes {start}-{end}/{}", body.len()),
                            ),
                        ],
                        body: bounded[..bounded.len().saturating_sub(1)].to_vec(),
                    },
                }
            }
            None => TestResponse {
                status_line: "200 OK",
                headers: vec![("Content-Length".to_string(), body.len().to_string())],
                body: body.to_vec(),
            },
        };
        if let Some(etag) = etag {
            response
                .headers
                .push(("ETag".to_string(), etag.to_string()));
        }
        response
    }

    fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
        let range = range_header
            .strip_prefix("bytes=")
            .expect("test server expects byte ranges");
        let (start, end) = range
            .split_once('-')
            .expect("range should include '-' separator");
        let start = start.parse::<usize>().expect("range start should parse");
        let end = end.parse::<usize>().expect("range end should parse");
        assert!(start <= end, "test range should be ascending");
        assert!(end < object_len, "test range should fit in object");
        (start, end)
    }
}

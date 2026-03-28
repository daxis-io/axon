//! Browser-safe runtime envelope with Parquet footer bootstrap for future DataFusion WASM
//! integration.
//!
//! The current in-repo surface validates browser-safe object sources, preserves a tiny generic
//! probe path above HTTP range reads, can materialize browser HTTP snapshot descriptors into
//! runtime-owned object sources, and can bootstrap bounded raw Parquet footer bytes without
//! starting DataFusion registration or browser SQL execution.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use futures_timer::Delay;
use futures_util::{
    future::{select, Either},
    pin_mut,
    stream::{self, StreamExt, TryStreamExt},
};
use parquet::basic::{
    ConvertedType as ParquetConvertedType,
    EdgeInterpolationAlgorithm as ParquetEdgeInterpolationAlgorithm,
    LogicalType as ParquetLogicalType, Repetition as ParquetRepetition,
    TimeUnit as ParquetTimeUnit, Type as ParquetPhysicalType,
};
use parquet::file::{metadata::ParquetMetaDataReader, statistics::Statistics as ParquetStatistics};
use query_contract::{
    validate_browser_object_url, BrowserHttpSnapshotDescriptor, BrowserObjectUrlPolicy,
    CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason, QueryError, QueryErrorCode,
    QueryRequest,
};
use reqwest::Url;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, LimitClause, ObjectName, ObjectNamePart, OrderByKind,
    Query as SqlQuery, Select, SelectItem, SetExpr as SqlSetExpr, Statement as SqlStatement,
    TableAlias, TableFactor as SqlTableFactor, TableWithJoins as SqlTableWithJoins,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use wasm_http_object_store::{HttpByteRange, HttpRangeReadResult, HttpRangeReader};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe query execution for the supported SQL envelope.";
pub const DEFAULT_TABLE_NAME: &str = "axon_table";
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
pub enum BrowserParquetPhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

impl fmt::Display for BrowserParquetPhysicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Boolean => "BOOLEAN",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Int96 => "INT96",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::ByteArray => "BYTE_ARRAY",
            Self::FixedLenByteArray => "FIXED_LEN_BYTE_ARRAY",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserParquetConvertedType {
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

impl fmt::Display for BrowserParquetConvertedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Utf8 => "UTF8",
            Self::Map => "MAP",
            Self::MapKeyValue => "MAP_KEY_VALUE",
            Self::List => "LIST",
            Self::Enum => "ENUM",
            Self::Decimal => "DECIMAL",
            Self::Date => "DATE",
            Self::TimeMillis => "TIME_MILLIS",
            Self::TimeMicros => "TIME_MICROS",
            Self::TimestampMillis => "TIMESTAMP_MILLIS",
            Self::TimestampMicros => "TIMESTAMP_MICROS",
            Self::UInt8 => "UINT_8",
            Self::UInt16 => "UINT_16",
            Self::UInt32 => "UINT_32",
            Self::UInt64 => "UINT_64",
            Self::Int8 => "INT_8",
            Self::Int16 => "INT_16",
            Self::Int32 => "INT_32",
            Self::Int64 => "INT_64",
            Self::Json => "JSON",
            Self::Bson => "BSON",
            Self::Interval => "INTERVAL",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserParquetTimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl fmt::Display for BrowserParquetTimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Millis => "MILLIS",
            Self::Micros => "MICROS",
            Self::Nanos => "NANOS",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserParquetEdgeInterpolationAlgorithm {
    Spherical,
    Vincenty,
    Thomas,
    Andoyer,
    Karney,
    Unknown(i32),
}

impl fmt::Display for BrowserParquetEdgeInterpolationAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spherical => f.write_str("SPHERICAL"),
            Self::Vincenty => f.write_str("VINCENTY"),
            Self::Thomas => f.write_str("THOMAS"),
            Self::Andoyer => f.write_str("ANDOYER"),
            Self::Karney => f.write_str("KARNEY"),
            Self::Unknown(code) => write!(f, "UNKNOWN({code})"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserParquetLogicalType {
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
        unit: BrowserParquetTimeUnit,
    },
    Timestamp {
        is_adjusted_to_utc: bool,
        unit: BrowserParquetTimeUnit,
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
        algorithm: Option<BrowserParquetEdgeInterpolationAlgorithm>,
    },
    Unrecognized {
        field_id: i16,
    },
}

impl fmt::Display for BrowserParquetLogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => f.write_str("STRING"),
            Self::Map => f.write_str("MAP"),
            Self::List => f.write_str("LIST"),
            Self::Enum => f.write_str("ENUM"),
            Self::Decimal { scale, precision } => {
                write!(f, "DECIMAL(scale={scale}, precision={precision})")
            }
            Self::Date => f.write_str("DATE"),
            Self::Time {
                is_adjusted_to_utc,
                unit,
            } => write!(f, "TIME(unit={unit}, adjusted_to_utc={is_adjusted_to_utc})"),
            Self::Timestamp {
                is_adjusted_to_utc,
                unit,
            } => write!(
                f,
                "TIMESTAMP(unit={unit}, adjusted_to_utc={is_adjusted_to_utc})"
            ),
            Self::Integer {
                bit_width,
                is_signed,
            } => write!(f, "INTEGER(bit_width={bit_width}, signed={is_signed})"),
            Self::Unknown => f.write_str("UNKNOWN"),
            Self::Json => f.write_str("JSON"),
            Self::Bson => f.write_str("BSON"),
            Self::Uuid => f.write_str("UUID"),
            Self::Float16 => f.write_str("FLOAT16"),
            Self::Variant {
                specification_version,
            } => write!(
                f,
                "VARIANT(specification_version={})",
                specification_version
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            ),
            Self::Geometry { crs } => {
                write!(f, "GEOMETRY(crs={})", crs.as_deref().unwrap_or("<none>"))
            }
            Self::Geography { crs, algorithm } => write!(
                f,
                "GEOGRAPHY(crs={}, algorithm={})",
                crs.as_deref().unwrap_or("<none>"),
                algorithm
                    .as_ref()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            ),
            Self::Unrecognized { field_id } => write!(f, "UNRECOGNIZED(field_id={field_id})"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserParquetRepetition {
    Required,
    Optional,
    Repeated,
}

impl fmt::Display for BrowserParquetRepetition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Required => "REQUIRED",
            Self::Optional => "OPTIONAL",
            Self::Repeated => "REPEATED",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserParquetField {
    pub name: String,
    pub physical_type: BrowserParquetPhysicalType,
    pub logical_type: Option<BrowserParquetLogicalType>,
    pub converted_type: Option<BrowserParquetConvertedType>,
    pub repetition: BrowserParquetRepetition,
    pub nullable: bool,
    pub max_definition_level: i16,
    pub max_repetition_level: i16,
    pub type_length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrowserParquetFieldStats {
    pub min_i64: Option<i64>,
    pub max_i64: Option<i64>,
    pub null_count: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserParquetFileMetadata {
    pub object_size_bytes: u64,
    pub footer_length_bytes: u32,
    pub row_group_count: u64,
    pub row_count: u64,
    pub fields: Vec<BrowserParquetField>,
    pub field_stats: BTreeMap<String, BrowserParquetFieldStats>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrappedBrowserFile {
    path: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    metadata: BrowserParquetFileMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrappedBrowserSnapshot {
    table_uri: String,
    snapshot_version: i64,
    active_files: Vec<BootstrappedBrowserFile>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserQueryShape {
    pub referenced_columns: Vec<String>,
    pub filter_columns: Vec<String>,
    pub projection_columns: Vec<String>,
    pub has_aggregation: bool,
    pub has_order_by: bool,
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrowserPruningSummary {
    pub used_partition_pruning: bool,
    pub used_file_stats_pruning: bool,
    pub files_retained: u64,
    pub files_pruned: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserPlannedQuery {
    pub table_uri: String,
    pub snapshot_version: i64,
    pub query_shape: BrowserQueryShape,
    pub candidate_paths: Vec<String>,
    pub candidate_file_count: u64,
    pub candidate_bytes: u64,
    pub candidate_rows: u64,
    pub partition_columns: Vec<String>,
    pub pruning: BrowserPruningSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserScanPlan {
    candidate_paths: Vec<String>,
    candidate_file_count: u64,
    candidate_bytes: u64,
    candidate_rows: u64,
    partition_columns: Vec<String>,
}

impl BrowserScanPlan {
    pub fn candidate_paths(&self) -> &[String] {
        &self.candidate_paths
    }

    pub fn candidate_file_count(&self) -> u64 {
        self.candidate_file_count
    }

    pub fn candidate_bytes(&self) -> u64 {
        self.candidate_bytes
    }

    pub fn candidate_rows(&self) -> u64 {
        self.candidate_rows
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserAggregateFunction {
    Avg,
    ArrayAgg,
    BoolAnd,
    BoolOr,
    CountStar,
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserAggregateMeasure {
    output_name: String,
    function: BrowserAggregateFunction,
    source_column: Option<String>,
}

impl BrowserAggregateMeasure {
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    pub fn function(&self) -> &BrowserAggregateFunction {
        &self.function
    }

    pub fn source_column(&self) -> Option<&str> {
        self.source_column.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserAggregationPlan {
    group_by_columns: Vec<String>,
    measures: Vec<BrowserAggregateMeasure>,
}

impl BrowserAggregationPlan {
    pub fn group_by_columns(&self) -> &[String] {
        &self.group_by_columns
    }

    pub fn measures(&self) -> &[BrowserAggregateMeasure] {
        &self.measures
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserExecutionOutputKind {
    Passthrough {
        source_column: String,
    },
    Aggregate {
        function: BrowserAggregateFunction,
        source_column: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserExecutionOutput {
    output_name: String,
    kind: BrowserExecutionOutputKind,
}

impl BrowserExecutionOutput {
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    pub fn kind(&self) -> &BrowserExecutionOutputKind {
        &self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSortKey {
    output_name: String,
    descending: bool,
}

impl BrowserSortKey {
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    pub fn descending(&self) -> bool {
        self.descending
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserExecutionPlan {
    table_uri: String,
    snapshot_version: i64,
    scan: BrowserScanPlan,
    outputs: Vec<BrowserExecutionOutput>,
    aggregation: Option<BrowserAggregationPlan>,
    order_by: Vec<BrowserSortKey>,
    limit: Option<u64>,
    pruning: BrowserPruningSummary,
}

impl BrowserExecutionPlan {
    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    pub fn snapshot_version(&self) -> i64 {
        self.snapshot_version
    }

    pub fn scan(&self) -> &BrowserScanPlan {
        &self.scan
    }

    pub fn outputs(&self) -> &[BrowserExecutionOutput] {
        &self.outputs
    }

    pub fn aggregation(&self) -> Option<&BrowserAggregationPlan> {
        self.aggregation.as_ref()
    }

    pub fn order_by(&self) -> &[BrowserSortKey] {
        &self.order_by
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub fn pruning(&self) -> &BrowserPruningSummary {
        &self.pruning
    }
}

impl BootstrappedBrowserFile {
    pub fn new(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
        metadata: BrowserParquetFileMetadata,
    ) -> Result<Self, QueryError> {
        let path = path.into();
        if metadata.object_size_bytes != size_bytes {
            return Err(snapshot_validation_error(format!(
                "bootstrapped browser file '{}' declared {} bytes, but metadata reported {} bytes",
                path, size_bytes, metadata.object_size_bytes,
            )));
        }

        Ok(Self {
            path,
            size_bytes,
            partition_values,
            metadata,
        })
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

    pub fn metadata(&self) -> &BrowserParquetFileMetadata {
        &self.metadata
    }
}

impl BootstrappedBrowserSnapshot {
    pub fn new(
        table_uri: impl Into<String>,
        snapshot_version: i64,
        active_files: Vec<BootstrappedBrowserFile>,
    ) -> Result<Self, QueryError> {
        validate_unique_bootstrapped_paths(&active_files)?;

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

    pub fn active_files(&self) -> &[BootstrappedBrowserFile] {
        &self.active_files
    }

    pub fn validate_uniform_schema(&self) -> Result<BrowserSnapshotSchema, QueryError> {
        let expected_fields = self
            .active_files
            .first()
            .map(|file| file.metadata().fields.clone())
            .unwrap_or_default();
        let expected_partition_columns = self
            .active_files
            .first()
            .map(|file| file.partition_values().keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut total_row_groups = 0_u64;
        let mut total_rows = 0_u64;
        let mut total_bytes = 0_u64;

        for file in &self.active_files {
            if file.metadata().fields != expected_fields {
                return Err(snapshot_validation_error(format!(
                    "bootstrapped browser snapshot contained a schema mismatch in file '{}': expected [{}], found [{}]",
                    file.path(),
                    format_browser_fields(&expected_fields),
                    format_browser_fields(&file.metadata().fields),
                )));
            }
            let partition_columns = file.partition_values().keys().cloned().collect::<Vec<_>>();
            if partition_columns != expected_partition_columns {
                return Err(snapshot_validation_error(format!(
                    "bootstrapped browser snapshot contained partition-column mismatch in file '{}': expected [{}], found [{}]",
                    file.path(),
                    format_partition_columns(&expected_partition_columns),
                    format_partition_columns(&partition_columns),
                )));
            }

            total_row_groups = total_row_groups
                .checked_add(file.metadata().row_group_count)
                .ok_or_else(|| {
                    snapshot_validation_error(
                        "bootstrapped browser snapshot row-group totals overflowed u64",
                    )
                })?;
            total_rows = total_rows
                .checked_add(file.metadata().row_count)
                .ok_or_else(|| {
                    snapshot_validation_error(
                        "bootstrapped browser snapshot row-count totals overflowed u64",
                    )
                })?;
            total_bytes = total_bytes.checked_add(file.size_bytes()).ok_or_else(|| {
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
            table_uri: self.table_uri().to_string(),
            snapshot_version: self.snapshot_version(),
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

    pub fn analyze_query_shape(&self, sql: &str) -> Result<BrowserQueryShape, QueryError> {
        Ok(analyze_browser_query(sql)?.shape)
    }

    pub fn plan_query(
        &self,
        snapshot: &BootstrappedBrowserSnapshot,
        request: &QueryRequest,
    ) -> Result<BrowserPlannedQuery, QueryError> {
        validate_snapshot_request_match(snapshot, request)?;
        let analyzed = analyze_browser_query(&request.sql)?;
        self.plan_query_from_analyzed(snapshot, &analyzed)
    }

    pub fn build_execution_plan(
        &self,
        snapshot: &BootstrappedBrowserSnapshot,
        request: &QueryRequest,
    ) -> Result<BrowserExecutionPlan, QueryError> {
        validate_snapshot_request_match(snapshot, request)?;
        let analyzed = analyze_browser_query(&request.sql)?;
        let planned = self.plan_query_from_analyzed(snapshot, &analyzed)?;
        lower_browser_execution_plan(analyzed.query.as_ref(), &planned)
    }

    fn plan_query_from_analyzed(
        &self,
        snapshot: &BootstrappedBrowserSnapshot,
        analyzed: &AnalyzedBrowserQuery,
    ) -> Result<BrowserPlannedQuery, QueryError> {
        let summary = snapshot.summarize()?;
        let mut available_columns = snapshot_available_columns(&summary);
        available_columns.extend(browser_query_scope_columns(analyzed.query.as_ref()));
        for referenced_column in &analyzed.shape.referenced_columns {
            if !available_columns.contains(referenced_column) {
                return Err(invalid_query_request(format!(
                    "query referenced unknown column '{}' for bootstrapped snapshot '{}'",
                    referenced_column,
                    snapshot.table_uri(),
                )));
            }
        }

        let partition_columns = summary.schema.partition_columns.clone();
        let normalized_partition_columns = partition_columns
            .iter()
            .map(|column| normalize_name(column))
            .collect::<BTreeSet<_>>();
        let stats_columns = summary
            .schema
            .fields
            .iter()
            .filter(|field| browser_field_supports_i64_stats(field))
            .map(|field| normalize_name(&field.name))
            .filter(|column| !normalized_partition_columns.contains(column))
            .collect::<BTreeSet<_>>();
        let pruning_constraints = extract_browser_pruning_constraints(
            analyzed.query.as_ref(),
            &normalized_partition_columns,
            &stats_columns,
        );
        let mut candidate_files = snapshot.active_files().iter().collect::<Vec<_>>();
        let partition_constraints = pruning_constraints
            .as_ref()
            .map(|constraints| &constraints.partition)
            .filter(|constraints| !constraints.by_column.is_empty());
        if let Some(partition_constraints) = partition_constraints {
            candidate_files.retain(|file| partition_constraints_match(file, partition_constraints));
        }

        let file_stats_constraint = pruning_constraints
            .as_ref()
            .and_then(|constraints| constraints.file_stats.as_ref());
        let used_file_stats_pruning = file_stats_constraint
            .is_some_and(|constraint| can_apply_file_stats_pruning(&candidate_files, constraint));
        if let Some(file_stats_constraint) =
            file_stats_constraint.filter(|_| used_file_stats_pruning)
        {
            candidate_files
                .retain(|file| file_stats_constraint_matches(file, file_stats_constraint));
        }

        let (candidate_paths, candidate_bytes, candidate_rows) =
            summarize_candidate_files(candidate_files.iter().copied())?;
        let candidate_file_count = u64::try_from(candidate_paths.len()).map_err(|_| {
            invalid_query_request("candidate file counts overflowed u64 during browser planning")
        })?;
        let total_file_count = u64::try_from(snapshot.active_files().len()).map_err(|_| {
            invalid_query_request("snapshot file counts overflowed u64 during browser planning")
        })?;
        let files_pruned = total_file_count
            .checked_sub(candidate_file_count)
            .ok_or_else(|| {
                invalid_query_request(
                    "candidate file counts exceeded snapshot file counts during browser planning",
                )
            })?;

        Ok(BrowserPlannedQuery {
            table_uri: snapshot.table_uri().to_string(),
            snapshot_version: snapshot.snapshot_version(),
            query_shape: analyzed.shape.clone(),
            candidate_paths,
            candidate_file_count,
            candidate_bytes,
            candidate_rows,
            partition_columns,
            pruning: BrowserPruningSummary {
                used_partition_pruning: partition_constraints.is_some(),
                used_file_stats_pruning,
                files_retained: candidate_file_count,
                files_pruned,
                ..BrowserPruningSummary::default()
            },
        })
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

        BootstrappedBrowserSnapshot::new(
            snapshot.table_uri(),
            snapshot.snapshot_version(),
            active_files,
        )
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

        BootstrappedBrowserFile::new(
            file.path(),
            file.size_bytes(),
            file.partition_values().clone(),
            metadata,
        )
    }
}

fn validate_snapshot_request_match(
    snapshot: &BootstrappedBrowserSnapshot,
    request: &QueryRequest,
) -> Result<(), QueryError> {
    if request.table_uri != snapshot.table_uri() {
        return Err(invalid_query_request(format!(
            "request table_uri '{}' did not match the bootstrapped snapshot table_uri '{}'",
            request.table_uri,
            snapshot.table_uri(),
        )));
    }

    if let Some(snapshot_version) = request.snapshot_version {
        if snapshot_version != snapshot.snapshot_version() {
            return Err(invalid_query_request(format!(
                "request snapshot_version {} did not match the bootstrapped snapshot version {}",
                snapshot_version,
                snapshot.snapshot_version(),
            )));
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
enum BrowserSourceBindings {
    BaseTable,
    Named(BTreeMap<String, String>),
}

impl BrowserSourceBindings {
    fn resolve_passthrough_expr(&self, expr: &SqlExpr) -> Option<String> {
        match self {
            Self::BaseTable => base_table_passthrough_column(expr),
            Self::Named(bindings) => {
                passthrough_binding_name(expr).and_then(|name| bindings.get(&name).cloned())
            }
        }
    }
}

fn lower_browser_execution_plan(
    query: &SqlQuery,
    planned: &BrowserPlannedQuery,
) -> Result<BrowserExecutionPlan, QueryError> {
    let select = query
        .body
        .as_select()
        .ok_or_else(|| execution_plan_error("browser execution plans require a SELECT query"))?;
    let source_bindings = lower_query_source_bindings(query)?;
    let group_by_columns = lower_group_by_columns(select, &source_bindings)?;
    if select.having.is_some() {
        return Err(execution_plan_error(
            "HAVING is not supported in browser execution plans",
        ));
    }

    let (outputs, measures) = lower_select_outputs(select, &source_bindings)?;
    validate_unique_execution_output_names(&outputs)?;
    let grouped_query = !group_by_columns.is_empty();
    let aggregate_query = grouped_query || !measures.is_empty();
    if aggregate_query {
        let grouped_columns = group_by_columns.iter().cloned().collect::<BTreeSet<_>>();
        for output in &outputs {
            if let BrowserExecutionOutputKind::Passthrough { source_column } = output.kind() {
                if !grouped_columns.contains(source_column) {
                    return Err(execution_plan_error(
                        "non-aggregate outputs must be grouped columns in browser execution plans",
                    ));
                }
            }
        }
    }

    let aggregation = aggregate_query.then(|| BrowserAggregationPlan {
        group_by_columns,
        measures,
    });
    let order_by = lower_order_by_keys(query, &outputs)?;

    Ok(BrowserExecutionPlan {
        table_uri: planned.table_uri.clone(),
        snapshot_version: planned.snapshot_version,
        scan: browser_scan_plan(planned),
        outputs,
        aggregation,
        order_by,
        limit: planned.query_shape.limit,
        pruning: planned.pruning.clone(),
    })
}

fn lower_query_source_bindings(query: &SqlQuery) -> Result<BrowserSourceBindings, QueryError> {
    let mut cte_bindings = BTreeMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if !cte.alias.columns.is_empty() {
                return Err(execution_plan_error(
                    "CTE column lists are not supported in browser execution plans",
                ));
            }

            let bindings = lower_query_passthrough_bindings(cte.query.as_ref(), &cte_bindings)?;
            cte_bindings.insert(normalize_name(&cte.alias.name.value), bindings);
        }
    }

    let select = query
        .body
        .as_select()
        .ok_or_else(|| execution_plan_error("browser execution plans require a SELECT query"))?;
    let source = select.from.first().ok_or_else(|| {
        execution_plan_error("browser execution plans require a single source relation")
    })?;
    if !source.joins.is_empty() {
        return Err(execution_plan_error(
            "browser execution plans require a single source relation",
        ));
    }

    lower_table_factor_bindings(&source.relation, &cte_bindings)
}

fn lower_table_factor_bindings(
    table_factor: &SqlTableFactor,
    cte_bindings: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<BrowserSourceBindings, QueryError> {
    match table_factor {
        SqlTableFactor::Table { name, alias, .. } => {
            if alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(execution_plan_error(
                    "table alias column lists are not supported in browser execution plans",
                ));
            }

            let relation_name = object_name_to_relation_name(name)
                .ok_or_else(|| execution_plan_error("query must read only from axon_table"))?;
            if relation_name == DEFAULT_TABLE_NAME {
                Ok(BrowserSourceBindings::BaseTable)
            } else if let Some(bindings) = cte_bindings.get(&relation_name) {
                Ok(BrowserSourceBindings::Named(bindings.clone()))
            } else {
                Err(execution_plan_error("query must read only from axon_table"))
            }
        }
        SqlTableFactor::Derived {
            subquery, alias, ..
        } => {
            if alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(execution_plan_error(
                    "derived-table alias column lists are not supported in browser execution plans",
                ));
            }

            Ok(BrowserSourceBindings::Named(
                lower_query_passthrough_bindings(subquery.as_ref(), cte_bindings)?,
            ))
        }
        _ => Err(execution_plan_error("query must read only from axon_table")),
    }
}

fn lower_query_passthrough_bindings(
    query: &SqlQuery,
    cte_bindings: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, QueryError> {
    if query.order_by.is_some() || query.limit_clause.is_some() {
        return Err(execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        ));
    }

    let select = query.body.as_select().ok_or_else(|| {
        execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        )
    })?;
    if select.having.is_some()
        || matches!(select.group_by, GroupByExpr::All(_))
        || matches!(&select.group_by, GroupByExpr::Expressions(expressions, _) if !expressions.is_empty())
    {
        return Err(execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        ));
    }

    let source = select.from.first().ok_or_else(|| {
        execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        )
    })?;
    if !source.joins.is_empty() {
        return Err(execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        ));
    }

    let source_bindings = lower_table_factor_bindings(&source.relation, cte_bindings)?;
    let mut bindings = BTreeMap::new();
    for select_item in &select.projection {
        let output = lower_passthrough_output(select_item, &source_bindings)?;
        let BrowserExecutionOutputKind::Passthrough { source_column } = output.kind else {
            return Err(execution_plan_error(
                "CTE and derived queries must remain pure passthrough projections in browser execution plans",
            ));
        };
        if bindings
            .insert(output.output_name.clone(), source_column.clone())
            .is_some()
        {
            return Err(execution_plan_error(
                "duplicate output columns are not supported in browser execution plans",
            ));
        }
    }

    Ok(bindings)
}

fn lower_group_by_columns(
    select: &Select,
    source_bindings: &BrowserSourceBindings,
) -> Result<Vec<String>, QueryError> {
    match &select.group_by {
        GroupByExpr::All(_) => Err(execution_plan_error(
            "GROUP BY ALL is not supported in browser execution plans",
        )),
        GroupByExpr::Expressions(expressions, _) => expressions
            .iter()
            .map(|expression| {
                source_bindings
                    .resolve_passthrough_expr(expression)
                    .ok_or_else(|| {
                        execution_plan_error(
                            "GROUP BY expressions must be passthrough column references in browser execution plans",
                        )
                    })
            })
            .collect(),
    }
}

fn lower_select_outputs(
    select: &Select,
    source_bindings: &BrowserSourceBindings,
) -> Result<(Vec<BrowserExecutionOutput>, Vec<BrowserAggregateMeasure>), QueryError> {
    let mut outputs = Vec::new();
    let mut measures = Vec::new();

    for select_item in &select.projection {
        if let Some((output, measure)) = lower_aggregate_output(select_item, source_bindings)? {
            outputs.push(output);
            measures.push(measure);
        } else {
            outputs.push(lower_passthrough_output(select_item, source_bindings)?);
        }
    }

    Ok((outputs, measures))
}

fn lower_aggregate_output(
    select_item: &SelectItem,
    source_bindings: &BrowserSourceBindings,
) -> Result<Option<(BrowserExecutionOutput, BrowserAggregateMeasure)>, QueryError> {
    let (expr, output_name) = match select_item {
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(normalize_name(&alias.value))),
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(execution_plan_error(
                "wildcard projections are not supported in browser execution plans",
            ))
        }
    };

    let SqlExpr::Function(function) = expr else {
        return Ok(None);
    };
    let function_name = function
        .name
        .0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| normalize_name(&ident.value))
        .ok_or_else(|| {
            execution_plan_error(
                "aggregate projections are not supported in browser execution plans",
            )
        })?;

    let aggregate_function = match function_name.as_str() {
        "avg" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Avg)?
        }
        "array_agg" => lower_single_column_aggregate(
            function,
            source_bindings,
            BrowserAggregateFunction::ArrayAgg,
        )?,
        "bool_and" => lower_single_column_aggregate(
            function,
            source_bindings,
            BrowserAggregateFunction::BoolAnd,
        )?,
        "bool_or" => lower_single_column_aggregate(
            function,
            source_bindings,
            BrowserAggregateFunction::BoolOr,
        )?,
        "count" => lower_count_aggregate(function, source_bindings)?,
        "sum" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Sum)?
        }
        "min" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Min)?
        }
        "max" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Max)?
        }
        _ => return Ok(None),
    };
    let output_name = output_name.ok_or_else(|| {
        execution_plan_error(
            "aggregate projections must use explicit aliases in browser execution plans",
        )
    })?;

    let measure = BrowserAggregateMeasure {
        output_name: output_name.clone(),
        function: aggregate_function.0,
        source_column: aggregate_function.1.clone(),
    };
    let output = BrowserExecutionOutput {
        output_name,
        kind: BrowserExecutionOutputKind::Aggregate {
            function: measure.function.clone(),
            source_column: measure.source_column.clone(),
        },
    };

    Ok(Some((output, measure)))
}

fn lower_count_aggregate(
    function: &sqlparser::ast::Function,
    source_bindings: &BrowserSourceBindings,
) -> Result<(BrowserAggregateFunction, Option<String>), QueryError> {
    validate_supported_aggregate_function(function)?;
    let args = single_function_argument(function)?;
    match args {
        FunctionArgExpr::Wildcard => Ok((BrowserAggregateFunction::CountStar, None)),
        FunctionArgExpr::Expr(expr) => {
            let source_column = source_bindings.resolve_passthrough_expr(expr).ok_or_else(|| {
                execution_plan_error(
                    "aggregate arguments must be passthrough column references in browser execution plans",
                )
            })?;
            Ok((BrowserAggregateFunction::Count, Some(source_column)))
        }
        FunctionArgExpr::QualifiedWildcard(_) => Err(execution_plan_error(
            "qualified wildcard aggregates are not supported in browser execution plans",
        )),
    }
}

fn lower_single_column_aggregate(
    function: &sqlparser::ast::Function,
    source_bindings: &BrowserSourceBindings,
    aggregate_function: BrowserAggregateFunction,
) -> Result<(BrowserAggregateFunction, Option<String>), QueryError> {
    validate_supported_aggregate_function(function)?;
    let FunctionArgExpr::Expr(expr) = single_function_argument(function)? else {
        return Err(execution_plan_error(
            "aggregate arguments must be passthrough column references in browser execution plans",
        ));
    };
    let source_column = source_bindings
        .resolve_passthrough_expr(expr)
        .ok_or_else(|| {
            execution_plan_error(
            "aggregate arguments must be passthrough column references in browser execution plans",
        )
        })?;

    Ok((aggregate_function, Some(source_column)))
}

fn validate_supported_aggregate_function(
    function: &sqlparser::ast::Function,
) -> Result<(), QueryError> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    }

    let FunctionArguments::List(arguments) = &function.args else {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    }

    Ok(())
}

fn single_function_argument(
    function: &sqlparser::ast::Function,
) -> Result<&FunctionArgExpr, QueryError> {
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    };
    let [argument] = arguments.args.as_slice() else {
        return Err(execution_plan_error(
            "aggregate functions must take exactly one argument in browser execution plans",
        ));
    };

    match argument {
        FunctionArg::Named { arg, .. }
        | FunctionArg::ExprNamed { arg, .. }
        | FunctionArg::Unnamed(arg) => Ok(arg),
    }
}

fn lower_passthrough_output(
    select_item: &SelectItem,
    source_bindings: &BrowserSourceBindings,
) -> Result<BrowserExecutionOutput, QueryError> {
    match select_item {
        SelectItem::ExprWithAlias { expr, alias } => {
            let source_column = source_bindings.resolve_passthrough_expr(expr).ok_or_else(|| {
                execution_plan_error(
                    "projection expressions must be passthrough column references in browser execution plans",
                )
            })?;
            Ok(BrowserExecutionOutput {
                output_name: normalize_name(&alias.value),
                kind: BrowserExecutionOutputKind::Passthrough { source_column },
            })
        }
        SelectItem::UnnamedExpr(expr) => {
            let source_column = source_bindings.resolve_passthrough_expr(expr).ok_or_else(|| {
                execution_plan_error(
                    "projection expressions must be passthrough column references in browser execution plans",
                )
            })?;
            let output_name = passthrough_output_name(expr).ok_or_else(|| {
                execution_plan_error(
                    "projection expressions must be passthrough column references in browser execution plans",
                )
            })?;
            Ok(BrowserExecutionOutput {
                output_name,
                kind: BrowserExecutionOutputKind::Passthrough { source_column },
            })
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => Err(execution_plan_error(
            "wildcard projections are not supported in browser execution plans",
        )),
    }
}

fn passthrough_output_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Nested(expr) => passthrough_output_name(expr),
        SqlExpr::Identifier(ident) => Some(normalize_name(&ident.value)),
        SqlExpr::CompoundIdentifier(parts) => Some(normalize_name(&parts.last()?.value)),
        _ => None,
    }
}

fn passthrough_binding_name(expr: &SqlExpr) -> Option<String> {
    passthrough_output_name(expr)
}

fn base_table_passthrough_column(expr: &SqlExpr) -> Option<String> {
    passthrough_output_name(expr)
}

fn lower_order_by_keys(
    query: &SqlQuery,
    outputs: &[BrowserExecutionOutput],
) -> Result<Vec<BrowserSortKey>, QueryError> {
    let output_names = outputs
        .iter()
        .map(|output| output.output_name.clone())
        .collect::<BTreeSet<_>>();
    let Some(order_by) = &query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(execution_plan_error(
            "ORDER BY expressions are not supported in browser execution plans",
        ));
    };

    expressions
        .iter()
        .map(|expression| {
            if expression.with_fill.is_some() || expression.options.nulls_first.is_some() {
                return Err(execution_plan_error(
                    "ORDER BY options are not supported in browser execution plans",
                ));
            }

            let output_name =
                expr_named_column(&expression.expr, &output_names).ok_or_else(|| {
                    execution_plan_error(
                        "ORDER BY expressions must reference projected output columns in browser execution plans",
                    )
                })?;
            Ok(BrowserSortKey {
                output_name,
                descending: expression.options.asc == Some(false),
            })
        })
        .collect()
}

fn validate_unique_execution_output_names(
    outputs: &[BrowserExecutionOutput],
) -> Result<(), QueryError> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();

    for output in outputs {
        if !seen.insert(output.output_name.clone()) {
            duplicates.insert(output.output_name.clone());
        }
    }

    if duplicates.is_empty() {
        return Ok(());
    }

    Err(execution_plan_error(format!(
        "duplicate output columns are not supported in browser execution plans [{}]",
        duplicates
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

fn execution_plan_error(message: impl Into<String>) -> QueryError {
    invalid_query_request(message)
}

fn browser_scan_plan(planned: &BrowserPlannedQuery) -> BrowserScanPlan {
    BrowserScanPlan {
        candidate_paths: planned.candidate_paths.clone(),
        candidate_file_count: planned.candidate_file_count,
        candidate_bytes: planned.candidate_bytes,
        candidate_rows: planned.candidate_rows,
        partition_columns: planned.partition_columns.clone(),
    }
}

#[derive(Clone)]
struct AnalyzedBrowserQuery {
    shape: BrowserQueryShape,
    query: Box<SqlQuery>,
}

#[derive(Default)]
struct BrowserQueryShapeAccumulator {
    referenced_columns: BTreeSet<String>,
    filter_columns: BTreeSet<String>,
    has_aggregation: bool,
}

#[derive(Clone, Debug, Default)]
struct PartitionPruningConstraints {
    by_column: BTreeMap<String, PartitionValueConstraint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PartitionValueConstraint {
    AllowedValues(BTreeSet<Option<String>>),
    NotNull,
}

#[derive(Clone, Debug, Default)]
struct BrowserPruningConstraints {
    partition: PartitionPruningConstraints,
    file_stats: Option<FileStatsPruningConstraint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileStatsPruningConstraint {
    column: String,
    comparison: IntegerLiteralComparison,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntegerLiteralComparison {
    Eq(i64),
    Gt(i64),
    Gte(i64),
    Lt(i64),
    Lte(i64),
}

fn analyze_browser_query(sql: &str) -> Result<AnalyzedBrowserQuery, QueryError> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| invalid_query_request(error.to_string()))?;
    if statements.len() != 1 {
        return Err(invalid_query_request(format!(
            "sql must contain exactly one read-only query over {DEFAULT_TABLE_NAME}"
        )));
    }

    let statement = statements
        .into_iter()
        .next()
        .expect("statement length already checked");
    let SqlStatement::Query(query) = statement else {
        return Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        )));
    };

    validate_browser_query(query.as_ref(), &BTreeSet::new())?;
    let shape = collect_browser_query_shape(query.as_ref())?;

    Ok(AnalyzedBrowserQuery { shape, query })
}

fn validate_browser_query(
    query: &SqlQuery,
    outer_scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    if query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        )));
    }

    let mut scope = outer_scope.clone();
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(invalid_query_request(format!(
                "recursive queries are not supported; only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
            )));
        }

        for cte in &with.cte_tables {
            scope.insert(normalize_name(&cte.alias.name.value));
        }

        for cte in &with.cte_tables {
            validate_browser_query(cte.query.as_ref(), &scope)?;
        }
    }

    validate_set_expr_sources(query.body.as_ref(), &scope)?;

    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } if offset.is_none() && limit_by.is_empty() => {
                if let Some(limit) = limit {
                    parse_limit_expr(limit)?;
                }
            }
            _ => {
                return Err(invalid_query_request(format!(
                    "only simple LIMIT clauses over {DEFAULT_TABLE_NAME} are supported"
                )))
            }
        }
    }

    if let Some(order_by) = &query.order_by {
        if !matches!(order_by.kind, OrderByKind::Expressions(_)) || order_by.interpolate.is_some() {
            return Err(invalid_query_request(format!(
                "only standard ORDER BY clauses over {DEFAULT_TABLE_NAME} are supported"
            )));
        }
    }

    Ok(())
}

fn validate_set_expr_sources(
    set_expr: &SqlSetExpr,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match set_expr {
        SqlSetExpr::Select(select) => validate_select_sources(select.as_ref(), scope),
        SqlSetExpr::Query(query) => validate_browser_query(query.as_ref(), scope),
        _ => Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        ))),
    }
}

fn validate_select_sources(select: &Select, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    if select.distinct.is_some() {
        return Err(invalid_query_request(format!(
            "DISTINCT is not supported; query must read only from {DEFAULT_TABLE_NAME}"
        )));
    }

    if select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.connect_by.is_some()
        || !matches!(select.flavor, sqlparser::ast::SelectFlavor::Standard)
        || select.from.len() != 1
    {
        return Err(invalid_query_request(format!(
            "query must read only from {DEFAULT_TABLE_NAME}"
        )));
    }

    validate_table_with_joins(
        select.from.first().expect("select.from length checked"),
        scope,
    )
}

fn validate_table_with_joins(
    table_with_joins: &SqlTableWithJoins,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    if !table_with_joins.joins.is_empty() {
        return Err(invalid_query_request(format!(
            "query must read only from {DEFAULT_TABLE_NAME}"
        )));
    }

    validate_table_factor(&table_with_joins.relation, scope)
}

fn validate_table_factor(
    table_factor: &SqlTableFactor,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match table_factor {
        SqlTableFactor::Table {
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            let relation_name = object_name_to_relation_name(name).ok_or_else(|| {
                invalid_query_request(format!("query must read only from {DEFAULT_TABLE_NAME}"))
            })?;

            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err(invalid_query_request(format!(
                    "query must read only from {DEFAULT_TABLE_NAME}"
                )));
            }

            if relation_name == DEFAULT_TABLE_NAME || scope.contains(&relation_name) {
                Ok(())
            } else {
                Err(invalid_query_request(format!(
                    "query must read only from {DEFAULT_TABLE_NAME}"
                )))
            }
        }
        SqlTableFactor::Derived { subquery, .. } => {
            validate_browser_query(subquery.as_ref(), scope)
        }
        _ => Err(invalid_query_request(format!(
            "query must read only from {DEFAULT_TABLE_NAME}"
        ))),
    }
}

fn collect_browser_query_shape(query: &SqlQuery) -> Result<BrowserQueryShape, QueryError> {
    let mut accumulator = BrowserQueryShapeAccumulator::default();
    collect_query_columns(query, &mut accumulator)?;

    let projection_columns = query
        .body
        .as_select()
        .map(collect_projection_columns)
        .transpose()?
        .unwrap_or_default();

    let limit = query
        .limit_clause
        .as_ref()
        .map(parse_limit_clause)
        .transpose()?
        .flatten();

    Ok(BrowserQueryShape {
        referenced_columns: accumulator.referenced_columns.into_iter().collect(),
        filter_columns: accumulator.filter_columns.into_iter().collect(),
        projection_columns,
        has_aggregation: accumulator.has_aggregation || query_uses_grouping(query),
        has_order_by: query.order_by.is_some(),
        limit,
    })
}

fn collect_query_columns(
    query: &SqlQuery,
    accumulator: &mut BrowserQueryShapeAccumulator,
) -> Result<(), QueryError> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_columns(cte.query.as_ref(), accumulator)?;
        }
    }

    collect_set_expr_columns(query.body.as_ref(), accumulator)?;

    if let Some(order_by) = &query.order_by {
        collect_order_by_columns(
            order_by,
            &mut accumulator.referenced_columns,
            &mut accumulator.has_aggregation,
        )?;
    }

    Ok(())
}

fn collect_set_expr_columns(
    set_expr: &SqlSetExpr,
    accumulator: &mut BrowserQueryShapeAccumulator,
) -> Result<(), QueryError> {
    match set_expr {
        SqlSetExpr::Select(select) => collect_select_columns(select.as_ref(), accumulator),
        SqlSetExpr::Query(query) => collect_query_columns(query.as_ref(), accumulator),
        _ => Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        ))),
    }
}

fn collect_select_columns(
    select: &Select,
    accumulator: &mut BrowserQueryShapeAccumulator,
) -> Result<(), QueryError> {
    for table_with_joins in &select.from {
        if let SqlTableFactor::Derived { subquery, .. } = &table_with_joins.relation {
            collect_query_columns(subquery.as_ref(), accumulator)?;
        }
    }

    for select_item in &select.projection {
        collect_select_item_columns(
            select_item,
            &mut accumulator.referenced_columns,
            &mut accumulator.has_aggregation,
        )?;
    }

    if let Some(selection) = &select.selection {
        collect_expr_columns(
            selection,
            &mut accumulator.referenced_columns,
            &mut accumulator.has_aggregation,
        )?;
        collect_expr_columns(
            selection,
            &mut accumulator.filter_columns,
            &mut accumulator.has_aggregation,
        )?;
    }

    match &select.group_by {
        GroupByExpr::All(_) => {
            accumulator.has_aggregation = true;
        }
        GroupByExpr::Expressions(expressions, _) => {
            if !expressions.is_empty() {
                accumulator.has_aggregation = true;
            }
            for expression in expressions {
                collect_expr_columns(
                    expression,
                    &mut accumulator.referenced_columns,
                    &mut accumulator.has_aggregation,
                )?;
            }
        }
    }

    if let Some(having) = &select.having {
        accumulator.has_aggregation = true;
        collect_expr_columns(
            having,
            &mut accumulator.referenced_columns,
            &mut accumulator.has_aggregation,
        )?;
    }

    Ok(())
}

fn collect_projection_columns(select: &Select) -> Result<Vec<String>, QueryError> {
    let mut projection_columns = BTreeSet::new();
    let mut has_aggregation = false;
    for select_item in &select.projection {
        collect_select_item_columns(select_item, &mut projection_columns, &mut has_aggregation)?;
    }
    Ok(projection_columns.into_iter().collect())
}

fn collect_select_item_columns(
    select_item: &SelectItem,
    columns: &mut BTreeSet<String>,
    has_aggregation: &mut bool,
) -> Result<(), QueryError> {
    match select_item {
        SelectItem::UnnamedExpr(expr) => collect_expr_columns(expr, columns, has_aggregation),
        SelectItem::ExprWithAlias { expr, .. } => {
            collect_expr_columns(expr, columns, has_aggregation)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => Err(invalid_query_request(
            format!("wildcard projections are not supported; query must read only from {DEFAULT_TABLE_NAME}"),
        )),
    }
}

fn collect_order_by_columns(
    order_by: &sqlparser::ast::OrderBy,
    columns: &mut BTreeSet<String>,
    has_aggregation: &mut bool,
) -> Result<(), QueryError> {
    match &order_by.kind {
        OrderByKind::Expressions(expressions) => {
            for expression in expressions {
                collect_expr_columns(&expression.expr, columns, has_aggregation)?;
            }
            Ok(())
        }
        _ => Err(invalid_query_request(format!(
            "only standard ORDER BY clauses over {DEFAULT_TABLE_NAME} are supported"
        ))),
    }
}

fn collect_expr_columns(
    expr: &SqlExpr,
    columns: &mut BTreeSet<String>,
    has_aggregation: &mut bool,
) -> Result<(), QueryError> {
    match expr {
        SqlExpr::Identifier(ident) => {
            columns.insert(normalize_name(&ident.value));
            Ok(())
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let ident = parts.last().map(|part| &part.value).ok_or_else(|| {
                invalid_query_request("compound identifiers must end in a column identifier")
            })?;
            columns.insert(normalize_name(ident));
            Ok(())
        }
        SqlExpr::CompoundFieldAccess { root, .. } => {
            collect_expr_columns(root, columns, has_aggregation)
        }
        SqlExpr::JsonAccess { value, .. } => collect_expr_columns(value, columns, has_aggregation),
        SqlExpr::IsFalse(expr)
        | SqlExpr::IsNotFalse(expr)
        | SqlExpr::IsTrue(expr)
        | SqlExpr::IsNotTrue(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::IsUnknown(expr)
        | SqlExpr::IsNotUnknown(expr)
        | SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Nested(expr)
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::AtTimeZone {
            timestamp: expr, ..
        }
        | SqlExpr::Extract { expr, .. }
        | SqlExpr::Ceil { expr, .. }
        | SqlExpr::Floor { expr, .. }
        | SqlExpr::Trim { expr, .. } => collect_expr_columns(expr, columns, has_aggregation),
        SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, columns, has_aggregation)?;
            collect_expr_columns(right, columns, has_aggregation)
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            for item in list {
                collect_expr_columns(item, columns, has_aggregation)?;
            }
            Ok(())
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            collect_expr_columns(low, columns, has_aggregation)?;
            collect_expr_columns(high, columns, has_aggregation)
        }
        SqlExpr::Like { expr, pattern, .. }
        | SqlExpr::ILike { expr, pattern, .. }
        | SqlExpr::SimilarTo { expr, pattern, .. }
        | SqlExpr::RLike { expr, pattern, .. } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            collect_expr_columns(pattern, columns, has_aggregation)
        }
        SqlExpr::AnyOp {
            left,
            right,
            compare_op: _,
            is_some: _,
        }
        | SqlExpr::AllOp {
            left,
            right,
            compare_op: _,
        } => {
            collect_expr_columns(left, columns, has_aggregation)?;
            collect_expr_columns(right, columns, has_aggregation)
        }
        SqlExpr::Convert { expr, styles, .. } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            for style in styles {
                collect_expr_columns(style, columns, has_aggregation)?;
            }
            Ok(())
        }
        SqlExpr::Position { expr, r#in } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            collect_expr_columns(r#in, columns, has_aggregation)
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            if let Some(substring_from) = substring_from {
                collect_expr_columns(substring_from, columns, has_aggregation)?;
            }
            if let Some(substring_for) = substring_for {
                collect_expr_columns(substring_for, columns, has_aggregation)?;
            }
            Ok(())
        }
        SqlExpr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_expr_columns(expr, columns, has_aggregation)?;
            collect_expr_columns(overlay_what, columns, has_aggregation)?;
            collect_expr_columns(overlay_from, columns, has_aggregation)?;
            if let Some(overlay_for) = overlay_for {
                collect_expr_columns(overlay_for, columns, has_aggregation)?;
            }
            Ok(())
        }
        SqlExpr::Collate { expr, .. } => collect_expr_columns(expr, columns, has_aggregation),
        SqlExpr::Value(_) | SqlExpr::TypedString(_) => Ok(()),
        SqlExpr::Function(function) => {
            if function_uses_aggregation(function) {
                *has_aggregation = true;
            }
            collect_function_arguments(&function.args, columns, has_aggregation)?;
            if let Some(filter) = &function.filter {
                collect_expr_columns(filter, columns, has_aggregation)?;
            }
            for expression in &function.within_group {
                collect_expr_columns(&expression.expr, columns, has_aggregation)?;
            }
            Ok(())
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_columns(operand, columns, has_aggregation)?;
            }
            for condition in conditions {
                collect_expr_columns(&condition.condition, columns, has_aggregation)?;
                collect_expr_columns(&condition.result, columns, has_aggregation)?;
            }
            if let Some(else_result) = else_result {
                collect_expr_columns(else_result, columns, has_aggregation)?;
            }
            Ok(())
        }
        SqlExpr::Exists { .. }
        | SqlExpr::Subquery(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::InUnnest { .. }
        | SqlExpr::Tuple(_)
        | SqlExpr::Struct { .. }
        | SqlExpr::Named { .. }
        | SqlExpr::Dictionary(_)
        | SqlExpr::Map(_)
        | SqlExpr::Array(_)
        | SqlExpr::Interval(_)
        | SqlExpr::MatchAgainst { .. }
        | SqlExpr::Wildcard(_)
        | SqlExpr::QualifiedWildcard(_, _) => Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        ))),
        _ => Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        ))),
    }
}

fn collect_function_arguments(
    arguments: &FunctionArguments,
    columns: &mut BTreeSet<String>,
    has_aggregation: &mut bool,
) -> Result<(), QueryError> {
    match arguments {
        FunctionArguments::None => Ok(()),
        FunctionArguments::Subquery(_) => Err(invalid_query_request(format!(
            "only read-only SELECT queries over {DEFAULT_TABLE_NAME} are supported"
        ))),
        FunctionArguments::List(argument_list) => {
            for argument in &argument_list.args {
                match argument {
                    FunctionArg::Named { arg, .. }
                    | FunctionArg::ExprNamed { arg, .. }
                    | FunctionArg::Unnamed(arg) => {
                        collect_function_arg_expr(arg, columns, has_aggregation)?;
                    }
                }
            }
            Ok(())
        }
    }
}

fn collect_function_arg_expr(
    arg: &FunctionArgExpr,
    columns: &mut BTreeSet<String>,
    has_aggregation: &mut bool,
) -> Result<(), QueryError> {
    match arg {
        FunctionArgExpr::Expr(expr) => collect_expr_columns(expr, columns, has_aggregation),
        FunctionArgExpr::Wildcard => Ok(()),
        FunctionArgExpr::QualifiedWildcard(_) => Err(invalid_query_request(format!(
            "wildcard function arguments are not supported; query must read only from {DEFAULT_TABLE_NAME}"
        ))),
    }
}

fn function_uses_aggregation(function: &sqlparser::ast::Function) -> bool {
    let function_name = function
        .name
        .0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| normalize_name(&ident.value));
    matches!(
        function_name.as_deref(),
        Some("avg" | "count" | "max" | "min" | "sum" | "array_agg" | "bool_and" | "bool_or")
    )
}

fn query_uses_grouping(query: &SqlQuery) -> bool {
    query
        .body
        .as_select()
        .is_some_and(|select| match &select.group_by {
            GroupByExpr::All(_) => true,
            GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
        })
        || query
            .body
            .as_select()
            .is_some_and(|select| select.having.is_some())
}

fn browser_query_scope_columns(query: &SqlQuery) -> BTreeSet<String> {
    let cte_outputs = query_cte_output_columns(query);
    let mut scope_columns = query_source_scope_columns(query, &cte_outputs);
    scope_columns.extend(query_order_by_alias_columns(query));
    scope_columns
}

fn query_cte_output_columns(query: &SqlQuery) -> BTreeMap<String, BTreeSet<String>> {
    let mut cte_outputs = BTreeMap::new();
    let Some(with) = &query.with else {
        return cte_outputs;
    };

    for cte in &with.cte_tables {
        let alias_columns = table_alias_columns(&cte.alias);
        let output_columns = if alias_columns.is_empty() {
            query_output_columns(cte.query.as_ref())
        } else {
            alias_columns
        };
        cte_outputs.insert(normalize_name(&cte.alias.name.value), output_columns);
    }

    cte_outputs
}

fn query_source_scope_columns(
    query: &SqlQuery,
    cte_outputs: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    let Some(select) = query.body.as_select() else {
        return BTreeSet::new();
    };
    let Some(source) = select.from.first() else {
        return BTreeSet::new();
    };

    table_factor_output_columns(&source.relation, cte_outputs)
}

fn query_order_by_alias_columns(query: &SqlQuery) -> BTreeSet<String> {
    let Some(select) = query.body.as_select() else {
        return BTreeSet::new();
    };
    let projection_aliases = select_projection_aliases(select);
    if projection_aliases.is_empty() {
        return BTreeSet::new();
    }

    let mut order_by_aliases = BTreeSet::new();
    let Some(order_by) = &query.order_by else {
        return order_by_aliases;
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return order_by_aliases;
    };

    for expression in expressions {
        if let Some(alias) = expr_named_column(&expression.expr, &projection_aliases) {
            order_by_aliases.insert(alias);
        }
    }

    order_by_aliases
}

fn query_output_columns(query: &SqlQuery) -> BTreeSet<String> {
    match query.body.as_ref() {
        SqlSetExpr::Select(select) => select_output_columns(select.as_ref()),
        SqlSetExpr::Query(query) => query_output_columns(query.as_ref()),
        _ => BTreeSet::new(),
    }
}

fn select_output_columns(select: &Select) -> BTreeSet<String> {
    let mut output_columns = BTreeSet::new();

    for select_item in &select.projection {
        match select_item {
            SelectItem::ExprWithAlias { alias, .. } => {
                output_columns.insert(normalize_name(&alias.value));
            }
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                output_columns.insert(normalize_name(&ident.value));
            }
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                if let Some(part) = parts.last() {
                    output_columns.insert(normalize_name(&part.value));
                }
            }
            _ => {}
        }
    }

    output_columns
}

fn select_projection_aliases(select: &Select) -> BTreeSet<String> {
    select
        .projection
        .iter()
        .filter_map(|select_item| match select_item {
            SelectItem::ExprWithAlias { alias, .. } => Some(normalize_name(&alias.value)),
            _ => None,
        })
        .collect()
}

fn table_factor_output_columns(
    table_factor: &SqlTableFactor,
    cte_outputs: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    match table_factor {
        SqlTableFactor::Table { name, alias, .. } => {
            let alias_columns = alias.as_ref().map(table_alias_columns).unwrap_or_default();
            if !alias_columns.is_empty() {
                return alias_columns;
            }

            object_name_to_relation_name(name)
                .and_then(|relation_name| cte_outputs.get(&relation_name).cloned())
                .unwrap_or_default()
        }
        SqlTableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias_columns = alias.as_ref().map(table_alias_columns).unwrap_or_default();
            if !alias_columns.is_empty() {
                alias_columns
            } else {
                query_output_columns(subquery.as_ref())
            }
        }
        _ => BTreeSet::new(),
    }
}

fn table_alias_columns(alias: &TableAlias) -> BTreeSet<String> {
    alias
        .columns
        .iter()
        .map(|ident| normalize_name(&ident.name.value))
        .collect()
}

fn extract_browser_pruning_constraints(
    query: &SqlQuery,
    partition_columns: &BTreeSet<String>,
    file_stats_columns: &BTreeSet<String>,
) -> Option<BrowserPruningConstraints> {
    if partition_columns.is_empty() && file_stats_columns.is_empty() {
        return None;
    }

    let select = query.body.as_select()?;
    let source = select.from.first()?;
    if !source.joins.is_empty() {
        return None;
    }

    match &source.relation {
        SqlTableFactor::Table { name, .. }
            if object_name_to_relation_name(name).as_deref() == Some(DEFAULT_TABLE_NAME) => {}
        _ => return None,
    }

    let selection = select.selection.as_ref()?;
    lower_browser_pruning_constraints(selection, partition_columns, file_stats_columns)
}

fn lower_browser_pruning_constraints(
    expr: &SqlExpr,
    partition_columns: &BTreeSet<String>,
    file_stats_columns: &BTreeSet<String>,
) -> Option<BrowserPruningConstraints> {
    match expr {
        SqlExpr::Nested(expr) => {
            lower_browser_pruning_constraints(expr, partition_columns, file_stats_columns)
        }
        SqlExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::And => {
            merge_optional_browser_pruning_constraints(
                lower_browser_pruning_constraints(left, partition_columns, file_stats_columns),
                lower_browser_pruning_constraints(right, partition_columns, file_stats_columns),
            )
        }
        _ => {
            if let Some(partition) = lower_partition_pruning_constraint(expr, partition_columns) {
                return Some(BrowserPruningConstraints {
                    partition,
                    file_stats: None,
                });
            }

            let file_stats = lower_file_stats_pruning_constraint(expr, file_stats_columns)?;
            Some(BrowserPruningConstraints {
                partition: PartitionPruningConstraints::default(),
                file_stats: Some(file_stats),
            })
        }
    }
}

fn merge_optional_browser_pruning_constraints(
    left: Option<BrowserPruningConstraints>,
    right: Option<BrowserPruningConstraints>,
) -> Option<BrowserPruningConstraints> {
    match (left, right) {
        (None, None) => None,
        (Some(constraints), None) | (None, Some(constraints)) => Some(constraints),
        (Some(left), Some(right)) => merge_browser_pruning_constraints(left, right),
    }
}

fn merge_browser_pruning_constraints(
    left: BrowserPruningConstraints,
    right: BrowserPruningConstraints,
) -> Option<BrowserPruningConstraints> {
    let file_stats = match (left.file_stats, right.file_stats) {
        (None, None) => None,
        (Some(file_stats), None) | (None, Some(file_stats)) => Some(file_stats),
        (Some(left_file_stats), Some(right_file_stats)) if left_file_stats == right_file_stats => {
            Some(left_file_stats)
        }
        _ => return None,
    };

    Some(BrowserPruningConstraints {
        partition: merge_partition_constraints(left.partition, right.partition)?,
        file_stats,
    })
}

fn lower_partition_pruning_constraint(
    expr: &SqlExpr,
    partition_columns: &BTreeSet<String>,
) -> Option<PartitionPruningConstraints> {
    match expr {
        SqlExpr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let column = expr_partition_column_name(expr, partition_columns)?;
            let mut values = BTreeSet::new();
            for item in list {
                values.insert(expr_literal_value(item)?);
            }
            Some(single_partition_values_constraint(column, values))
        }
        SqlExpr::IsNull(expr) => Some(single_partition_value_constraint(
            expr_partition_column_name(expr, partition_columns)?,
            None,
        )),
        SqlExpr::IsNotNull(expr) => Some(single_partition_not_null_constraint(
            expr_partition_column_name(expr, partition_columns)?,
        )),
        SqlExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::Eq => {
            let (column, value) = partition_column_literal_pair(left, right, partition_columns)?;
            Some(single_partition_value_constraint(column, value))
        }
        _ => None,
    }
}

fn merge_partition_constraints(
    left: PartitionPruningConstraints,
    right: PartitionPruningConstraints,
) -> Option<PartitionPruningConstraints> {
    let mut by_column = left.by_column;

    for (column, right_constraint) in right.by_column {
        match by_column.remove(&column) {
            Some(left_constraint) => {
                by_column.insert(
                    column,
                    merge_partition_value_constraint(left_constraint, right_constraint)?,
                );
            }
            None => {
                by_column.insert(column, right_constraint);
            }
        }
    }

    Some(PartitionPruningConstraints { by_column })
}

fn merge_partition_value_constraint(
    left: PartitionValueConstraint,
    right: PartitionValueConstraint,
) -> Option<PartitionValueConstraint> {
    match (left, right) {
        (
            PartitionValueConstraint::AllowedValues(left_values),
            PartitionValueConstraint::AllowedValues(right_values),
        ) => Some(PartitionValueConstraint::AllowedValues(
            left_values
                .intersection(&right_values)
                .cloned()
                .collect::<BTreeSet<_>>(),
        )),
        (PartitionValueConstraint::AllowedValues(values), PartitionValueConstraint::NotNull)
        | (PartitionValueConstraint::NotNull, PartitionValueConstraint::AllowedValues(values)) => {
            Some(PartitionValueConstraint::AllowedValues(
                values
                    .into_iter()
                    .filter(|value| value.is_some())
                    .collect::<BTreeSet<_>>(),
            ))
        }
        (PartitionValueConstraint::NotNull, PartitionValueConstraint::NotNull) => {
            Some(PartitionValueConstraint::NotNull)
        }
    }
}

fn lower_file_stats_pruning_constraint(
    expr: &SqlExpr,
    file_stats_columns: &BTreeSet<String>,
) -> Option<FileStatsPruningConstraint> {
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return None;
    };

    stats_column_integer_predicate(left, op, right, file_stats_columns).or_else(|| {
        let reversed = reverse_binary_operator(op)?;
        stats_column_integer_predicate(right, &reversed, left, file_stats_columns)
    })
}

fn stats_column_integer_predicate(
    column_expr: &SqlExpr,
    op: &SqlBinaryOperator,
    literal_expr: &SqlExpr,
    file_stats_columns: &BTreeSet<String>,
) -> Option<FileStatsPruningConstraint> {
    let column = expr_file_stats_column_name(column_expr, file_stats_columns)?;
    let literal = expr_i64_literal(literal_expr)?;
    let comparison = match op {
        SqlBinaryOperator::Eq => IntegerLiteralComparison::Eq(literal),
        SqlBinaryOperator::Gt => IntegerLiteralComparison::Gt(literal),
        SqlBinaryOperator::GtEq => IntegerLiteralComparison::Gte(literal),
        SqlBinaryOperator::Lt => IntegerLiteralComparison::Lt(literal),
        SqlBinaryOperator::LtEq => IntegerLiteralComparison::Lte(literal),
        _ => return None,
    };

    Some(FileStatsPruningConstraint { column, comparison })
}

fn reverse_binary_operator(op: &SqlBinaryOperator) -> Option<SqlBinaryOperator> {
    Some(match op {
        SqlBinaryOperator::Eq => SqlBinaryOperator::Eq,
        SqlBinaryOperator::Gt => SqlBinaryOperator::Lt,
        SqlBinaryOperator::GtEq => SqlBinaryOperator::LtEq,
        SqlBinaryOperator::Lt => SqlBinaryOperator::Gt,
        SqlBinaryOperator::LtEq => SqlBinaryOperator::GtEq,
        _ => return None,
    })
}

fn partition_column_literal_pair(
    left: &SqlExpr,
    right: &SqlExpr,
    partition_columns: &BTreeSet<String>,
) -> Option<(String, Option<String>)> {
    if let Some(column) = expr_partition_column_name(left, partition_columns) {
        return Some((column, expr_literal_value(right)?));
    }

    Some((
        expr_partition_column_name(right, partition_columns)?,
        expr_literal_value(left)?,
    ))
}

fn expr_partition_column_name(
    expr: &SqlExpr,
    partition_columns: &BTreeSet<String>,
) -> Option<String> {
    expr_named_column(expr, partition_columns)
}

fn expr_file_stats_column_name(
    expr: &SqlExpr,
    file_stats_columns: &BTreeSet<String>,
) -> Option<String> {
    expr_named_column(expr, file_stats_columns)
}

fn expr_named_column(expr: &SqlExpr, supported_columns: &BTreeSet<String>) -> Option<String> {
    let column = match expr {
        SqlExpr::Nested(expr) => return expr_named_column(expr, supported_columns),
        SqlExpr::Identifier(ident) => normalize_name(&ident.value),
        SqlExpr::CompoundIdentifier(parts) => normalize_name(&parts.last()?.value),
        _ => return None,
    };

    supported_columns.contains(&column).then_some(column)
}

fn expr_literal_value(expr: &SqlExpr) -> Option<Option<String>> {
    match expr {
        SqlExpr::Nested(expr) => expr_literal_value(expr),
        SqlExpr::Value(value) => {
            if let Some(string) = value.value.clone().into_string() {
                return Some(Some(string));
            }

            match &value.value {
                SqlValue::Number(number, _) => Some(Some(number.clone())),
                SqlValue::Boolean(value) => Some(Some(value.to_string())),
                SqlValue::Null => Some(None),
                _ => None,
            }
        }
        _ => None,
    }
}

fn expr_i64_literal(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Nested(expr) => expr_i64_literal(expr),
        SqlExpr::UnaryOp { op, expr } => match op {
            SqlUnaryOperator::Plus => expr_i64_literal(expr),
            SqlUnaryOperator::Minus => expr_i64_literal(expr)?.checked_neg(),
            _ => None,
        },
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(number, _) => number.parse::<i64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn single_partition_value_constraint(
    column: String,
    value: Option<String>,
) -> PartitionPruningConstraints {
    single_partition_values_constraint(column, BTreeSet::from([value]))
}

fn single_partition_values_constraint(
    column: String,
    values: BTreeSet<Option<String>>,
) -> PartitionPruningConstraints {
    PartitionPruningConstraints {
        by_column: BTreeMap::from([(column, PartitionValueConstraint::AllowedValues(values))]),
    }
}

fn single_partition_not_null_constraint(column: String) -> PartitionPruningConstraints {
    PartitionPruningConstraints {
        by_column: BTreeMap::from([(column, PartitionValueConstraint::NotNull)]),
    }
}

fn partition_constraints_match(
    file: &BootstrappedBrowserFile,
    constraints: &PartitionPruningConstraints,
) -> bool {
    constraints.by_column.iter().all(|(column, constraint)| {
        let value = file
            .partition_values()
            .iter()
            .find_map(|(key, value)| (normalize_name(key) == *column).then_some(value));

        match constraint {
            PartitionValueConstraint::AllowedValues(values) => {
                value.is_some_and(|value| values.contains(value))
            }
            PartitionValueConstraint::NotNull => value.is_some_and(|value| value.is_some()),
        }
    })
}

fn can_apply_file_stats_pruning(
    candidate_files: &[&BootstrappedBrowserFile],
    constraint: &FileStatsPruningConstraint,
) -> bool {
    !candidate_files.is_empty()
        && candidate_files.iter().all(|file| {
            file_field_stats(file, &constraint.column)
                .is_some_and(|stats| stats.min_i64.is_some() && stats.max_i64.is_some())
        })
}

fn file_stats_constraint_matches(
    file: &BootstrappedBrowserFile,
    constraint: &FileStatsPruningConstraint,
) -> bool {
    let Some(stats) = file_field_stats(file, &constraint.column) else {
        return false;
    };
    let (Some(min_i64), Some(max_i64)) = (stats.min_i64, stats.max_i64) else {
        return false;
    };

    match constraint.comparison {
        IntegerLiteralComparison::Eq(value) => min_i64 <= value && max_i64 >= value,
        IntegerLiteralComparison::Gt(value) => max_i64 > value,
        IntegerLiteralComparison::Gte(value) => max_i64 >= value,
        IntegerLiteralComparison::Lt(value) => min_i64 < value,
        IntegerLiteralComparison::Lte(value) => min_i64 <= value,
    }
}

fn file_field_stats<'a>(
    file: &'a BootstrappedBrowserFile,
    column: &str,
) -> Option<&'a BrowserParquetFieldStats> {
    file.metadata()
        .field_stats
        .iter()
        .find_map(|(key, value)| (normalize_name(key) == column).then_some(value))
}

fn parse_limit_clause(limit_clause: &LimitClause) -> Result<Option<u64>, QueryError> {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } if offset.is_none() && limit_by.is_empty() => {
            limit.as_ref().map(parse_limit_expr).transpose()
        }
        _ => Err(invalid_query_request(format!(
            "only simple LIMIT clauses over {DEFAULT_TABLE_NAME} are supported"
        ))),
    }
}

fn parse_limit_expr(limit: &SqlExpr) -> Result<u64, QueryError> {
    let SqlExpr::Value(value) = limit else {
        return Err(invalid_query_request(format!(
            "LIMIT must be a positive integer literal over {DEFAULT_TABLE_NAME}"
        )));
    };

    match &value.value {
        SqlValue::Number(number, false) => number.parse::<u64>().map_err(|_| {
            invalid_query_request(format!(
                "LIMIT must be a positive integer literal over {DEFAULT_TABLE_NAME}"
            ))
        }),
        _ => Err(invalid_query_request(format!(
            "LIMIT must be a positive integer literal over {DEFAULT_TABLE_NAME}"
        ))),
    }
}

fn object_name_to_relation_name(name: &ObjectName) -> Option<String> {
    let [part] = name.0.as_slice() else {
        return None;
    };

    Some(normalize_name(&part.as_ident()?.value))
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn snapshot_available_columns(summary: &BrowserSnapshotPreflightSummary) -> BTreeSet<String> {
    let mut columns = summary
        .schema
        .fields
        .iter()
        .map(|field| normalize_name(&field.name))
        .collect::<BTreeSet<_>>();
    columns.extend(
        summary
            .schema
            .partition_columns
            .iter()
            .map(|column| normalize_name(column)),
    );
    columns
}

fn browser_field_supports_i64_stats(field: &BrowserParquetField) -> bool {
    matches!(
        field.physical_type,
        BrowserParquetPhysicalType::Int32 | BrowserParquetPhysicalType::Int64
    ) && field.converted_type != Some(BrowserParquetConvertedType::Decimal)
        && !matches!(
            field.logical_type,
            Some(BrowserParquetLogicalType::Decimal { .. })
        )
}

fn summarize_candidate_files<'a>(
    active_files: impl IntoIterator<Item = &'a BootstrappedBrowserFile>,
) -> Result<(Vec<String>, u64, u64), QueryError> {
    let mut candidate_paths = Vec::new();
    let mut candidate_bytes = 0_u64;
    let mut candidate_rows = 0_u64;

    for file in active_files {
        candidate_paths.push(file.path().to_string());
        candidate_bytes = candidate_bytes
            .checked_add(file.size_bytes())
            .ok_or_else(|| invalid_query_request("candidate byte totals overflowed u64"))?;
        candidate_rows = candidate_rows
            .checked_add(file.metadata().row_count)
            .ok_or_else(|| invalid_query_request("candidate row totals overflowed u64"))?;
    }

    Ok((candidate_paths, candidate_bytes, candidate_rows))
}

fn invalid_query_request(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::InvalidRequest, message, runtime_target())
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

fn validate_unique_bootstrapped_paths(
    active_files: &[BootstrappedBrowserFile],
) -> Result<(), QueryError> {
    validate_unique_paths(
        active_files.iter().map(|file| file.path()),
        "bootstrapped browser snapshot contained duplicate paths",
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
    let field_stats = browser_parquet_field_stats_from_metadata(&metadata);

    Ok(BrowserParquetFileMetadata {
        object_size_bytes: footer.object_size_bytes(),
        footer_length_bytes: footer.footer_length_bytes(),
        row_group_count,
        row_count,
        fields,
        field_stats,
    })
}

fn browser_parquet_field_stats_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> BTreeMap<String, BrowserParquetFieldStats> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(column_index, column)| {
            let field_stats = browser_parquet_integer_field_stats(
                column.as_ref(),
                metadata.row_groups(),
                column_index,
            )?;
            Some((column.path().string(), field_stats))
        })
        .collect()
}

fn browser_parquet_integer_field_stats(
    column: &parquet::schema::types::ColumnDescriptor,
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    column_index: usize,
) -> Option<BrowserParquetFieldStats> {
    if !matches!(
        column.physical_type(),
        ParquetPhysicalType::INT32 | ParquetPhysicalType::INT64
    ) {
        return None;
    }
    if column.converted_type() == ParquetConvertedType::DECIMAL
        || matches!(
            column.logical_type_ref(),
            Some(ParquetLogicalType::Decimal { .. })
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

    let field_stats = BrowserParquetFieldStats {
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

fn browser_parquet_field_from_descriptor(
    column: &parquet::schema::types::ColumnDescriptor,
) -> BrowserParquetField {
    let repetition = column.self_type().get_basic_info().repetition();
    let converted_type = browser_parquet_converted_type(column.converted_type());
    let logical_type = column.logical_type_ref().map(browser_parquet_logical_type);
    let type_length = matches!(
        column.physical_type(),
        ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY
    )
    .then(|| column.type_length());
    let has_decimal_annotation = column.converted_type() == ParquetConvertedType::DECIMAL
        || matches!(
            column.logical_type_ref(),
            Some(ParquetLogicalType::Decimal { .. })
        );
    let precision = has_decimal_annotation.then(|| column.type_precision());
    let scale = has_decimal_annotation.then(|| column.type_scale());

    BrowserParquetField {
        name: column.path().string(),
        physical_type: browser_parquet_physical_type(column.physical_type()),
        logical_type,
        converted_type,
        repetition: browser_parquet_repetition(repetition),
        nullable: column.self_type().is_optional(),
        max_definition_level: column.max_def_level(),
        max_repetition_level: column.max_rep_level(),
        type_length,
        precision,
        scale,
    }
}

fn browser_parquet_physical_type(physical_type: ParquetPhysicalType) -> BrowserParquetPhysicalType {
    match physical_type {
        ParquetPhysicalType::BOOLEAN => BrowserParquetPhysicalType::Boolean,
        ParquetPhysicalType::INT32 => BrowserParquetPhysicalType::Int32,
        ParquetPhysicalType::INT64 => BrowserParquetPhysicalType::Int64,
        ParquetPhysicalType::INT96 => BrowserParquetPhysicalType::Int96,
        ParquetPhysicalType::FLOAT => BrowserParquetPhysicalType::Float,
        ParquetPhysicalType::DOUBLE => BrowserParquetPhysicalType::Double,
        ParquetPhysicalType::BYTE_ARRAY => BrowserParquetPhysicalType::ByteArray,
        ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY => BrowserParquetPhysicalType::FixedLenByteArray,
    }
}

fn browser_parquet_repetition(repetition: ParquetRepetition) -> BrowserParquetRepetition {
    match repetition {
        ParquetRepetition::REQUIRED => BrowserParquetRepetition::Required,
        ParquetRepetition::OPTIONAL => BrowserParquetRepetition::Optional,
        ParquetRepetition::REPEATED => BrowserParquetRepetition::Repeated,
    }
}

fn browser_parquet_converted_type(
    converted_type: ParquetConvertedType,
) -> Option<BrowserParquetConvertedType> {
    Some(match converted_type {
        ParquetConvertedType::NONE => return None,
        ParquetConvertedType::UTF8 => BrowserParquetConvertedType::Utf8,
        ParquetConvertedType::MAP => BrowserParquetConvertedType::Map,
        ParquetConvertedType::MAP_KEY_VALUE => BrowserParquetConvertedType::MapKeyValue,
        ParquetConvertedType::LIST => BrowserParquetConvertedType::List,
        ParquetConvertedType::ENUM => BrowserParquetConvertedType::Enum,
        ParquetConvertedType::DECIMAL => BrowserParquetConvertedType::Decimal,
        ParquetConvertedType::DATE => BrowserParquetConvertedType::Date,
        ParquetConvertedType::TIME_MILLIS => BrowserParquetConvertedType::TimeMillis,
        ParquetConvertedType::TIME_MICROS => BrowserParquetConvertedType::TimeMicros,
        ParquetConvertedType::TIMESTAMP_MILLIS => BrowserParquetConvertedType::TimestampMillis,
        ParquetConvertedType::TIMESTAMP_MICROS => BrowserParquetConvertedType::TimestampMicros,
        ParquetConvertedType::UINT_8 => BrowserParquetConvertedType::UInt8,
        ParquetConvertedType::UINT_16 => BrowserParquetConvertedType::UInt16,
        ParquetConvertedType::UINT_32 => BrowserParquetConvertedType::UInt32,
        ParquetConvertedType::UINT_64 => BrowserParquetConvertedType::UInt64,
        ParquetConvertedType::INT_8 => BrowserParquetConvertedType::Int8,
        ParquetConvertedType::INT_16 => BrowserParquetConvertedType::Int16,
        ParquetConvertedType::INT_32 => BrowserParquetConvertedType::Int32,
        ParquetConvertedType::INT_64 => BrowserParquetConvertedType::Int64,
        ParquetConvertedType::JSON => BrowserParquetConvertedType::Json,
        ParquetConvertedType::BSON => BrowserParquetConvertedType::Bson,
        ParquetConvertedType::INTERVAL => BrowserParquetConvertedType::Interval,
    })
}

fn browser_parquet_logical_type(logical_type: &ParquetLogicalType) -> BrowserParquetLogicalType {
    match logical_type {
        ParquetLogicalType::String => BrowserParquetLogicalType::String,
        ParquetLogicalType::Map => BrowserParquetLogicalType::Map,
        ParquetLogicalType::List => BrowserParquetLogicalType::List,
        ParquetLogicalType::Enum => BrowserParquetLogicalType::Enum,
        ParquetLogicalType::Decimal { scale, precision } => BrowserParquetLogicalType::Decimal {
            scale: *scale,
            precision: *precision,
        },
        ParquetLogicalType::Date => BrowserParquetLogicalType::Date,
        ParquetLogicalType::Time {
            is_adjusted_to_u_t_c,
            unit,
        } => BrowserParquetLogicalType::Time {
            is_adjusted_to_utc: *is_adjusted_to_u_t_c,
            unit: browser_parquet_time_unit(unit),
        },
        ParquetLogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            unit,
        } => BrowserParquetLogicalType::Timestamp {
            is_adjusted_to_utc: *is_adjusted_to_u_t_c,
            unit: browser_parquet_time_unit(unit),
        },
        ParquetLogicalType::Integer {
            bit_width,
            is_signed,
        } => BrowserParquetLogicalType::Integer {
            bit_width: *bit_width,
            is_signed: *is_signed,
        },
        ParquetLogicalType::Unknown => BrowserParquetLogicalType::Unknown,
        ParquetLogicalType::Json => BrowserParquetLogicalType::Json,
        ParquetLogicalType::Bson => BrowserParquetLogicalType::Bson,
        ParquetLogicalType::Uuid => BrowserParquetLogicalType::Uuid,
        ParquetLogicalType::Float16 => BrowserParquetLogicalType::Float16,
        ParquetLogicalType::Variant {
            specification_version,
        } => BrowserParquetLogicalType::Variant {
            specification_version: *specification_version,
        },
        ParquetLogicalType::Geometry { crs } => {
            BrowserParquetLogicalType::Geometry { crs: crs.clone() }
        }
        ParquetLogicalType::Geography { crs, algorithm } => BrowserParquetLogicalType::Geography {
            crs: crs.clone(),
            algorithm: algorithm
                .as_ref()
                .map(browser_parquet_edge_interpolation_algorithm),
        },
        ParquetLogicalType::_Unknown { field_id } => BrowserParquetLogicalType::Unrecognized {
            field_id: *field_id,
        },
    }
}

fn browser_parquet_time_unit(unit: &ParquetTimeUnit) -> BrowserParquetTimeUnit {
    match unit {
        ParquetTimeUnit::MILLIS => BrowserParquetTimeUnit::Millis,
        ParquetTimeUnit::MICROS => BrowserParquetTimeUnit::Micros,
        ParquetTimeUnit::NANOS => BrowserParquetTimeUnit::Nanos,
    }
}

fn browser_parquet_edge_interpolation_algorithm(
    algorithm: &ParquetEdgeInterpolationAlgorithm,
) -> BrowserParquetEdgeInterpolationAlgorithm {
    match algorithm {
        ParquetEdgeInterpolationAlgorithm::SPHERICAL => {
            BrowserParquetEdgeInterpolationAlgorithm::Spherical
        }
        ParquetEdgeInterpolationAlgorithm::VINCENTY => {
            BrowserParquetEdgeInterpolationAlgorithm::Vincenty
        }
        ParquetEdgeInterpolationAlgorithm::THOMAS => {
            BrowserParquetEdgeInterpolationAlgorithm::Thomas
        }
        ParquetEdgeInterpolationAlgorithm::ANDOYER => {
            BrowserParquetEdgeInterpolationAlgorithm::Andoyer
        }
        ParquetEdgeInterpolationAlgorithm::KARNEY => {
            BrowserParquetEdgeInterpolationAlgorithm::Karney
        }
        ParquetEdgeInterpolationAlgorithm::_Unknown(code) => {
            BrowserParquetEdgeInterpolationAlgorithm::Unknown(*code)
        }
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
                .as_ref()
                .map(|logical_type| logical_type.to_string())
                .unwrap_or_else(|| "<none>".to_string());
            let converted_type = field
                .converted_type
                .as_ref()
                .map(|converted_type| converted_type.to_string())
                .unwrap_or_else(|| "<none>".to_string());
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

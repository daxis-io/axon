//! Browser-safe runtime envelope with Parquet footer bootstrap for future DataFusion WASM
//! integration.
//!
//! The current in-repo surface validates browser-safe object sources, preserves a tiny generic
//! probe path above HTTP range reads, can materialize browser HTTP snapshot descriptors into
//! runtime-owned object sources, and can bootstrap bounded raw Parquet footer bytes without
//! starting DataFusion registration or browser SQL execution.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, Instant};

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
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::file::{metadata::ParquetMetaDataReader, statistics::Statistics as ParquetStatistics};
use parquet::record::Field as ParquetField;
use query_contract::{
    validate_browser_object_url, BrowserHttpSnapshotDescriptor, BrowserObjectUrlPolicy,
    CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason, PartitionColumnType,
    QueryError, QueryErrorCode, QueryMetricsSummary, QueryRequest,
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

mod execution;
mod lowering;
mod parquet_support;

use execution::{execute_aggregate_plan_rows, execute_non_aggregate_plan_rows};
use lowering::lower_browser_execution_plan;
#[cfg(test)]
use parquet_support::parquet_row_to_input_row;
use parquet_support::{
    decode_parquet_input_rows, format_browser_fields, format_partition_columns,
    parse_parquet_footer_trailer, parse_parquet_metadata, validate_parquet_footer_read_consistency,
};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe query execution for the supported SQL envelope.";
pub const DEFAULT_TABLE_NAME: &str = "axon_table";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 120_000;
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
    /// Maximum wall-clock time allowed for end-to-end browser query execution.
    pub execution_timeout_ms: u64,
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
            execution_timeout_ms: DEFAULT_EXECUTION_TIMEOUT_MS,
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

        if self.execution_timeout_ms == 0 {
            return Err(QueryError::new(
                QueryErrorCode::InvalidRequest,
                "browser runtime execution timeout must be at least 1 ms",
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
    partition_column_types: BTreeMap<String, PartitionColumnType>,
}

impl MaterializedBrowserSnapshot {
    pub fn new(
        table_uri: impl Into<String>,
        snapshot_version: i64,
        active_files: Vec<MaterializedBrowserFile>,
    ) -> Result<Self, QueryError> {
        Self::new_with_partition_column_types(
            table_uri,
            snapshot_version,
            active_files,
            BTreeMap::new(),
        )
    }

    pub fn new_with_partition_column_types(
        table_uri: impl Into<String>,
        snapshot_version: i64,
        active_files: Vec<MaterializedBrowserFile>,
        partition_column_types: BTreeMap<String, PartitionColumnType>,
    ) -> Result<Self, QueryError> {
        validate_unique_materialized_paths(&active_files)?;

        Ok(Self {
            table_uri: table_uri.into(),
            snapshot_version,
            active_files,
            partition_column_types,
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

    pub fn partition_column_types(&self) -> &BTreeMap<String, PartitionColumnType> {
        &self.partition_column_types
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
    object_etag: Option<String>,
    footer_length_bytes: u32,
    footer_bytes: Bytes,
}

impl BrowserParquetFooter {
    /// Total size of the source object in bytes, as derived from the Parquet trailer read.
    pub fn object_size_bytes(&self) -> u64 {
        self.object_size_bytes
    }

    /// Stable object identity observed during footer bootstrap when the server returned an ETag.
    pub fn object_etag(&self) -> Option<&str> {
        self.object_etag.as_deref()
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
    object_etag: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrappedBrowserSnapshot {
    table_uri: String,
    snapshot_version: i64,
    active_files: Vec<BootstrappedBrowserFile>,
    partition_column_types: BTreeMap<String, PartitionColumnType>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSnapshotSchema {
    pub fields: Vec<BrowserParquetField>,
    pub partition_columns: Vec<String>,
    pub partition_column_types: BTreeMap<String, PartitionColumnType>,
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
    candidate_object_etags: BTreeMap<String, String>,
    required_columns: Vec<String>,
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

    pub fn candidate_object_etag(&self, path: &str) -> Option<&str> {
        self.candidate_object_etags.get(path).map(String::as_str)
    }

    pub fn required_columns(&self) -> &[String] {
        &self.required_columns
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BrowserScalarValue {
    Int64(i64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserComparisonOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserFilterExpr {
    And {
        children: Vec<BrowserFilterExpr>,
    },
    Compare {
        source_column: String,
        comparison: BrowserComparisonOp,
        literal: BrowserScalarValue,
    },
    InList {
        source_column: String,
        literals: Vec<BrowserScalarValue>,
    },
    IsNull {
        source_column: String,
    },
    IsNotNull {
        source_column: String,
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
pub struct BrowserExecutionRow {
    values: Vec<BrowserScalarValue>,
}

impl BrowserExecutionRow {
    pub fn values(&self) -> &[BrowserScalarValue] {
        &self.values
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserExecutionResult {
    output_names: Vec<String>,
    rows: Vec<BrowserExecutionRow>,
    metrics: QueryMetricsSummary,
}

impl BrowserExecutionResult {
    pub fn output_names(&self) -> &[String] {
        &self.output_names
    }

    pub fn rows(&self) -> &[BrowserExecutionRow] {
        &self.rows
    }

    pub fn metrics(&self) -> &QueryMetricsSummary {
        &self.metrics
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserExecutionPlan {
    table_uri: String,
    snapshot_version: i64,
    scan: BrowserScanPlan,
    filter: Option<BrowserFilterExpr>,
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

    pub fn filter(&self) -> Option<&BrowserFilterExpr> {
        self.filter.as_ref()
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

type BrowserInputRow = BTreeMap<String, BrowserScalarValue>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct BrowserProjectedRow {
    values_by_name: BTreeMap<String, BrowserScalarValue>,
    row: BrowserExecutionRow,
}

impl BootstrappedBrowserFile {
    pub fn new(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
        metadata: BrowserParquetFileMetadata,
    ) -> Result<Self, QueryError> {
        Self::new_with_object_etag(path, size_bytes, partition_values, metadata, None)
    }

    pub fn new_with_object_etag(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
        metadata: BrowserParquetFileMetadata,
        object_etag: Option<String>,
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
            object_etag,
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

    pub fn object_etag(&self) -> Option<&str> {
        self.object_etag.as_deref()
    }
}

impl BootstrappedBrowserSnapshot {
    pub fn new(
        table_uri: impl Into<String>,
        snapshot_version: i64,
        active_files: Vec<BootstrappedBrowserFile>,
    ) -> Result<Self, QueryError> {
        Self::new_with_partition_column_types(
            table_uri,
            snapshot_version,
            active_files,
            BTreeMap::new(),
        )
    }

    pub fn new_with_partition_column_types(
        table_uri: impl Into<String>,
        snapshot_version: i64,
        active_files: Vec<BootstrappedBrowserFile>,
        partition_column_types: BTreeMap<String, PartitionColumnType>,
    ) -> Result<Self, QueryError> {
        validate_unique_bootstrapped_paths(&active_files)?;

        Ok(Self {
            table_uri: table_uri.into(),
            snapshot_version,
            active_files,
            partition_column_types,
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

    pub fn partition_column_types(&self) -> &BTreeMap<String, PartitionColumnType> {
        &self.partition_column_types
    }

    pub fn validate_uniform_schema(&self) -> Result<BrowserSnapshotSchema, QueryError> {
        let expected_fields = self
            .active_files
            .first()
            .map(|file| file.metadata().fields.clone())
            .unwrap_or_default();
        let expected_partition_columns = if self.partition_column_types.is_empty() {
            self.active_files
                .first()
                .map(|file| file.partition_values().keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        } else {
            self.partition_column_types
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
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
            partition_column_types: self.partition_column_types.clone(),
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
        lower_browser_execution_plan(analyzed.query.as_ref(), snapshot, &planned)
    }

    pub async fn execute_plan(
        &self,
        snapshot: &MaterializedBrowserSnapshot,
        plan: &BrowserExecutionPlan,
    ) -> Result<BrowserExecutionResult, QueryError> {
        validate_materialized_snapshot_plan_match(snapshot, plan)?;
        let candidate_files =
            candidate_materialized_files(snapshot, plan.scan().candidate_paths())?;
        let operation_started_at = Instant::now();
        let execution =
            self.execute_plan_inner(snapshot, plan, &candidate_files, operation_started_at);
        let timeout = Delay::new(Duration::from_millis(self.config.execution_timeout_ms));
        pin_mut!(execution);
        pin_mut!(timeout);

        match select(execution, timeout).await {
            Either::Left((result, _)) => result,
            Either::Right((_, _)) => Err(execution_timeout_error(
                snapshot.table_uri(),
                snapshot.snapshot_version(),
                candidate_files.len(),
                self.config.execution_timeout_ms,
            )),
        }
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

        MaterializedBrowserSnapshot::new_with_partition_column_types(
            descriptor.table_uri.clone(),
            descriptor.snapshot_version,
            active_files,
            descriptor.partition_column_types.clone(),
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

        BootstrappedBrowserSnapshot::new_with_partition_column_types(
            snapshot.table_uri(),
            snapshot.snapshot_version(),
            active_files,
            snapshot.partition_column_types().clone(),
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
            object_etag: trailer.metadata.etag.clone(),
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

    async fn read_candidate_input_rows(
        &self,
        snapshot: &MaterializedBrowserSnapshot,
        candidate_files: &[&MaterializedBrowserFile],
        scan: &BrowserScanPlan,
    ) -> Result<(Vec<BrowserInputRow>, u64), QueryError> {
        let mut rows = Vec::new();
        let mut bytes_fetched = 0_u64;

        for file in candidate_files {
            let read = self
                .read_range(file.object_source(), HttpByteRange::Full)
                .await?;
            validate_full_object_read(file, &read, scan.candidate_object_etag(file.path()))?;
            bytes_fetched = bytes_fetched
                .checked_add(file.size_bytes())
                .ok_or_else(|| {
                    execution_runtime_error("browser execution byte totals overflowed u64")
                })?;
            rows.extend(decode_parquet_input_rows(
                file,
                read.bytes,
                scan.required_columns(),
                snapshot.partition_column_types(),
            )?);
        }

        Ok((rows, bytes_fetched))
    }

    async fn bootstrap_file_metadata(
        &self,
        file: &MaterializedBrowserFile,
    ) -> Result<BootstrappedBrowserFile, QueryError> {
        let footer = self.read_parquet_footer_for_file(file).await?;
        let metadata = parse_parquet_metadata(file, &footer)?;

        BootstrappedBrowserFile::new_with_object_etag(
            file.path(),
            file.size_bytes(),
            file.partition_values().clone(),
            metadata,
            footer.object_etag().map(str::to_string),
        )
    }

    async fn execute_plan_inner(
        &self,
        snapshot: &MaterializedBrowserSnapshot,
        plan: &BrowserExecutionPlan,
        candidate_files: &[&MaterializedBrowserFile],
        operation_started_at: Instant,
    ) -> Result<BrowserExecutionResult, QueryError> {
        let (rows, bytes_fetched) = self
            .read_candidate_input_rows(snapshot, candidate_files, plan.scan())
            .await?;
        let mut result = if plan.aggregation().is_some() {
            execute_aggregate_plan_rows(plan, rows)?
        } else {
            execute_non_aggregate_plan_rows(plan, rows)?
        };
        result.metrics = execution_metrics(snapshot, plan, bytes_fetched, operation_started_at)?;
        Ok(result)
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

fn validate_materialized_snapshot_plan_match(
    snapshot: &MaterializedBrowserSnapshot,
    plan: &BrowserExecutionPlan,
) -> Result<(), QueryError> {
    if snapshot.table_uri() != plan.table_uri() {
        return Err(invalid_query_request(format!(
            "materialized snapshot table uri '{}' did not match browser execution plan table uri '{}'",
            snapshot.table_uri(),
            plan.table_uri(),
        )));
    }

    if snapshot.snapshot_version() != plan.snapshot_version() {
        return Err(invalid_query_request(format!(
            "materialized snapshot version {} did not match browser execution plan version {}",
            snapshot.snapshot_version(),
            plan.snapshot_version(),
        )));
    }

    Ok(())
}

fn candidate_materialized_files<'a>(
    snapshot: &'a MaterializedBrowserSnapshot,
    candidate_paths: &[String],
) -> Result<Vec<&'a MaterializedBrowserFile>, QueryError> {
    let files_by_path = snapshot
        .active_files()
        .iter()
        .map(|file| (file.path(), file))
        .collect::<BTreeMap<_, _>>();

    candidate_paths
        .iter()
        .map(|path| {
            files_by_path.get(path.as_str()).copied().ok_or_else(|| {
                invalid_query_request(format!(
                    "browser execution plan referenced candidate path '{}' that was absent from the materialized snapshot",
                    path
                ))
            })
        })
        .collect()
}

fn validate_full_object_read(
    file: &MaterializedBrowserFile,
    read: &HttpRangeReadResult,
    expected_object_etag: Option<&str>,
) -> Result<(), QueryError> {
    if let Some(object_size_bytes) = read.metadata.size_bytes {
        if object_size_bytes != file.size_bytes() {
            return Err(parquet_protocol_error(format!(
                "browser execution for file '{}' observed object size {} bytes, but the descriptor declared {} bytes",
                file.path(),
                object_size_bytes,
                file.size_bytes(),
            )));
        }
    }

    let read_len = u64::try_from(read.bytes.len())
        .map_err(|_| parquet_protocol_error("full-object read length overflowed u64"))?;
    if read_len != file.size_bytes() {
        return Err(parquet_protocol_error(format!(
            "browser execution for file '{}' read {} bytes, but the descriptor declared {} bytes",
            file.path(),
            read_len,
            file.size_bytes(),
        )));
    }

    if let Some(expected_object_etag) = expected_object_etag {
        match read.metadata.etag.as_deref() {
            Some(actual_object_etag) if actual_object_etag == expected_object_etag => {}
            Some(actual_object_etag) => {
                return Err(parquet_protocol_error(format!(
                    "browser execution for file '{}' observed entity tag {} during bootstrap but {} during execution",
                    file.path(),
                    expected_object_etag,
                    actual_object_etag,
                )))
            }
            None => {
                return Err(parquet_protocol_error(format!(
                    "browser execution for file '{}' expected entity tag metadata {} from bootstrap, but the full-object read returned none",
                    file.path(),
                    expected_object_etag,
                )))
            }
        }
    }

    Ok(())
}

fn execution_plan_error(message: impl Into<String>) -> QueryError {
    invalid_query_request(message)
}

fn execution_runtime_error(message: impl Into<String>) -> QueryError {
    QueryError::new(QueryErrorCode::ExecutionFailed, message, runtime_target())
}

fn execution_metrics(
    _snapshot: &MaterializedBrowserSnapshot,
    plan: &BrowserExecutionPlan,
    bytes_fetched: u64,
    operation_started_at: Instant,
) -> Result<QueryMetricsSummary, QueryError> {
    Ok(QueryMetricsSummary {
        bytes_fetched,
        duration_ms: wall_clock_duration_ms(operation_started_at),
        files_touched: plan.scan().candidate_file_count(),
        files_skipped: plan.pruning().files_pruned,
    })
}

fn wall_clock_duration_ms(operation_started_at: Instant) -> u64 {
    u64::try_from(operation_started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn browser_scan_plan(
    snapshot: &BootstrappedBrowserSnapshot,
    planned: &BrowserPlannedQuery,
    required_columns: Vec<String>,
) -> Result<BrowserScanPlan, QueryError> {
    validate_required_partition_execution_types(
        snapshot.partition_column_types(),
        &planned.partition_columns,
        &required_columns,
    )?;

    Ok(BrowserScanPlan {
        candidate_paths: planned.candidate_paths.clone(),
        candidate_file_count: planned.candidate_file_count,
        candidate_bytes: planned.candidate_bytes,
        candidate_rows: planned.candidate_rows,
        partition_columns: planned.partition_columns.clone(),
        candidate_object_etags: candidate_object_etags_from_snapshot(
            snapshot,
            &planned.candidate_paths,
        )?,
        required_columns,
    })
}

fn candidate_object_etags_from_snapshot(
    snapshot: &BootstrappedBrowserSnapshot,
    candidate_paths: &[String],
) -> Result<BTreeMap<String, String>, QueryError> {
    let files_by_path = snapshot
        .active_files()
        .iter()
        .map(|file| (file.path(), file))
        .collect::<BTreeMap<_, _>>();
    let mut candidate_object_etags = BTreeMap::new();

    for path in candidate_paths {
        let file = files_by_path.get(path.as_str()).copied().ok_or_else(|| {
            invalid_query_request(format!(
                "browser execution plan candidate path '{}' was absent from the bootstrapped snapshot",
                path
            ))
        })?;
        let object_etag = file.object_etag().ok_or_else(|| {
            QueryError::new(
                QueryErrorCode::FallbackRequired,
                format!(
                    "browser execution for file '{}' requires bootstrapped entity tag metadata to enforce object identity",
                    path
                ),
                runtime_target(),
            )
            .with_fallback_reason(FallbackReason::NativeRequired)
        })?;
        candidate_object_etags.insert(path.clone(), object_etag.to_string());
    }

    Ok(candidate_object_etags)
}

fn merge_partition_values_into_row(
    mut row: BrowserInputRow,
    partition_values: &BTreeMap<String, Option<String>>,
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    required_columns: &[String],
) -> Result<BrowserInputRow, QueryError> {
    let required_columns = required_columns
        .iter()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();

    for (column, value) in partition_values {
        let normalized_column = normalize_name(column);
        if !required_columns.is_empty() && !required_columns.contains(&normalized_column) {
            continue;
        }

        let partition_type = partition_column_type_for_name(partition_column_types, column)
            .ok_or_else(|| missing_partition_column_type_error(column))?;
        row.insert(
            normalized_column,
            partition_value_to_scalar(column, value, partition_type)?,
        );
    }

    Ok(row)
}

fn partition_value_to_scalar(
    column: &str,
    value: &Option<String>,
    partition_type: PartitionColumnType,
) -> Result<BrowserScalarValue, QueryError> {
    match value {
        None => Ok(BrowserScalarValue::Null),
        Some(value) => typed_partition_scalar(column, value, partition_type),
    }
}

fn typed_partition_scalar(
    column: &str,
    value: &str,
    partition_type: PartitionColumnType,
) -> Result<BrowserScalarValue, QueryError> {
    match partition_type {
        PartitionColumnType::String => Ok(BrowserScalarValue::String(value.to_string())),
        PartitionColumnType::Boolean => parse_canonical_partition_bool(value)
            .map(BrowserScalarValue::Boolean)
            .ok_or_else(|| invalid_partition_value_error(column, value, partition_type)),
        PartitionColumnType::Int64 => parse_canonical_partition_i64(value)
            .map(BrowserScalarValue::Int64)
            .ok_or_else(|| invalid_partition_value_error(column, value, partition_type)),
        PartitionColumnType::Unsupported => Err(unsupported_partition_type_error(column)),
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

fn validate_required_partition_execution_types(
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    partition_columns: &[String],
    required_columns: &[String],
) -> Result<(), QueryError> {
    let required_columns = required_columns
        .iter()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();

    for partition_column in partition_columns {
        let normalized_partition_column = normalize_name(partition_column);
        if !required_columns.contains(&normalized_partition_column) {
            continue;
        }

        match partition_column_type_for_name(partition_column_types, partition_column) {
            Some(PartitionColumnType::Unsupported) => {
                return Err(unsupported_partition_type_error(partition_column))
            }
            Some(_) => {}
            None => return Err(missing_partition_column_type_error(partition_column)),
        }
    }

    Ok(())
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

fn execution_timeout_error(
    table_uri: &str,
    snapshot_version: i64,
    file_count: usize,
    timeout_ms: u64,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        format!(
            "browser execution for '{}' at version {} did not finish within {} ms across {} file(s)",
            table_uri,
            snapshot_version,
            timeout_ms,
            file_count,
        ),
        runtime_target(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn browser_object_source_stores_a_validated_parsed_url() {
        let source = BrowserObjectSource::from_url("https://example.com/object")
            .expect("https object sources should be supported");

        let parsed_url: &Url = &source.url;

        assert_eq!(parsed_url.as_str(), "https://example.com/object");
        assert_eq!(source.url(), parsed_url.as_str());
    }

    #[test]
    fn merged_input_rows_expose_partition_values_for_execution() {
        let merged = merge_partition_values_into_row(
            input_row([
                ("id", BrowserScalarValue::Int64(6)),
                ("value", BrowserScalarValue::Int64(60)),
            ]),
            &BTreeMap::from([("category".to_string(), Some("C".to_string()))]),
            &BTreeMap::from([("category".to_string(), PartitionColumnType::String)]),
            &["category".to_string(), "id".to_string()],
        )
        .expect("partition values should merge");
        let plan = non_aggregate_test_plan(
            Some(BrowserFilterExpr::Compare {
                source_column: "category".to_string(),
                comparison: BrowserComparisonOp::Eq,
                literal: BrowserScalarValue::String("C".to_string()),
            }),
            vec![
                BrowserExecutionOutput {
                    output_name: "category".to_string(),
                    kind: BrowserExecutionOutputKind::Passthrough {
                        source_column: "category".to_string(),
                    },
                },
                BrowserExecutionOutput {
                    output_name: "id".to_string(),
                    kind: BrowserExecutionOutputKind::Passthrough {
                        source_column: "id".to_string(),
                    },
                },
            ],
            vec![BrowserSortKey {
                output_name: "id".to_string(),
                descending: false,
            }],
            None,
        );

        let result =
            execute_non_aggregate_plan_rows(&plan, vec![merged]).expect("plan rows should execute");

        assert_eq!(result.output_names(), ["category", "id"]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            [
                BrowserScalarValue::String("C".to_string()),
                BrowserScalarValue::Int64(6),
            ]
        );
    }

    #[test]
    fn merged_input_rows_normalize_and_coerce_integer_partition_values() {
        let merged = merge_partition_values_into_row(
            input_row([]),
            &BTreeMap::from([("ID".to_string(), Some("2".to_string()))]),
            &BTreeMap::from([("id".to_string(), PartitionColumnType::Int64)]),
            &["id".to_string()],
        )
        .expect("integer partition values should merge");

        assert_eq!(merged.get("id"), Some(&BrowserScalarValue::Int64(2)));
    }

    #[test]
    fn merged_input_rows_preserve_numeric_string_partition_values_when_typed_as_strings() {
        let merged = merge_partition_values_into_row(
            input_row([]),
            &BTreeMap::from([("year_code".to_string(), Some("2024".to_string()))]),
            &BTreeMap::from([("year_code".to_string(), PartitionColumnType::String)]),
            &["year_code".to_string()],
        )
        .expect("string partition values should merge");

        assert_eq!(
            merged.get("year_code"),
            Some(&BrowserScalarValue::String("2024".to_string()))
        );
    }

    #[test]
    fn parquet_row_to_input_row_skips_fields_when_no_columns_are_required() {
        let row =
            parquet::record::Row::new(vec![("payload".to_string(), ParquetField::Float(1.5))]);

        let values =
            parquet_row_to_input_row("part-000.parquet", &row, &[]).expect("row should decode");

        assert!(values.is_empty());
    }

    #[test]
    fn execute_non_aggregate_plan_applies_filter_alias_projection_order_and_limit() {
        let plan = non_aggregate_test_plan(
            Some(BrowserFilterExpr::Compare {
                source_column: "value".to_string(),
                comparison: BrowserComparisonOp::Gte,
                literal: BrowserScalarValue::Int64(20),
            }),
            vec![BrowserExecutionOutput {
                output_name: "total".to_string(),
                kind: BrowserExecutionOutputKind::Passthrough {
                    source_column: "value".to_string(),
                },
            }],
            vec![BrowserSortKey {
                output_name: "total".to_string(),
                descending: true,
            }],
            Some(2),
        );

        let result = execute_non_aggregate_plan_rows(
            &plan,
            vec![
                input_row([("value", BrowserScalarValue::Int64(10))]),
                input_row([("value", BrowserScalarValue::Int64(30))]),
                input_row([("value", BrowserScalarValue::Int64(20))]),
                input_row([("value", BrowserScalarValue::Int64(40))]),
            ],
        )
        .expect("plan rows should execute");

        assert_eq!(result.output_names(), ["total"]);
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), [BrowserScalarValue::Int64(40)]);
        assert_eq!(result.rows()[1].values(), [BrowserScalarValue::Int64(30)]);
    }

    #[test]
    fn execute_non_aggregate_plan_sorts_nulls_last_for_ascending_order() {
        let plan = non_aggregate_test_plan(
            None,
            vec![BrowserExecutionOutput {
                output_name: "value".to_string(),
                kind: BrowserExecutionOutputKind::Passthrough {
                    source_column: "value".to_string(),
                },
            }],
            vec![BrowserSortKey {
                output_name: "value".to_string(),
                descending: false,
            }],
            None,
        );

        let result = execute_non_aggregate_plan_rows(
            &plan,
            vec![
                input_row([("value", BrowserScalarValue::Null)]),
                input_row([("value", BrowserScalarValue::Int64(10))]),
                input_row([("value", BrowserScalarValue::Int64(5))]),
            ],
        )
        .expect("plan rows should execute");

        assert_eq!(result.output_names(), ["value"]);
        assert_eq!(result.rows().len(), 3);
        assert_eq!(result.rows()[0].values(), [BrowserScalarValue::Int64(5)]);
        assert_eq!(result.rows()[1].values(), [BrowserScalarValue::Int64(10)]);
        assert_eq!(result.rows()[2].values(), [BrowserScalarValue::Null]);
    }

    #[test]
    fn execute_non_aggregate_plan_sorts_nulls_first_for_descending_order() {
        let plan = non_aggregate_test_plan(
            None,
            vec![BrowserExecutionOutput {
                output_name: "value".to_string(),
                kind: BrowserExecutionOutputKind::Passthrough {
                    source_column: "value".to_string(),
                },
            }],
            vec![BrowserSortKey {
                output_name: "value".to_string(),
                descending: true,
            }],
            None,
        );

        let result = execute_non_aggregate_plan_rows(
            &plan,
            vec![
                input_row([("value", BrowserScalarValue::Int64(5))]),
                input_row([("value", BrowserScalarValue::Null)]),
                input_row([("value", BrowserScalarValue::Int64(10))]),
            ],
        )
        .expect("plan rows should execute");

        assert_eq!(result.output_names(), ["value"]);
        assert_eq!(result.rows().len(), 3);
        assert_eq!(result.rows()[0].values(), [BrowserScalarValue::Null]);
        assert_eq!(result.rows()[1].values(), [BrowserScalarValue::Int64(10)]);
        assert_eq!(result.rows()[2].values(), [BrowserScalarValue::Int64(5)]);
    }

    #[test]
    fn execute_non_aggregate_plan_returns_empty_rows_for_pruned_scan() {
        let plan = non_aggregate_test_plan(
            Some(BrowserFilterExpr::Compare {
                source_column: "category".to_string(),
                comparison: BrowserComparisonOp::Eq,
                literal: BrowserScalarValue::String("Z".to_string()),
            }),
            vec![BrowserExecutionOutput {
                output_name: "id".to_string(),
                kind: BrowserExecutionOutputKind::Passthrough {
                    source_column: "id".to_string(),
                },
            }],
            vec![BrowserSortKey {
                output_name: "id".to_string(),
                descending: false,
            }],
            None,
        );

        let result = execute_non_aggregate_plan_rows(&plan, Vec::new())
            .expect("empty pruned scans should still produce a result");

        assert_eq!(result.output_names(), ["id"]);
        assert!(result.rows().is_empty());
    }

    #[test]
    fn execute_aggregate_plan_supports_count_column_min_and_max() {
        let plan = aggregate_test_plan(
            vec![],
            vec![
                BrowserExecutionOutput {
                    output_name: "counted".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Count,
                        source_column: Some("value".to_string()),
                    },
                },
                BrowserExecutionOutput {
                    output_name: "min_value".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Min,
                        source_column: Some("value".to_string()),
                    },
                },
                BrowserExecutionOutput {
                    output_name: "max_value".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Max,
                        source_column: Some("value".to_string()),
                    },
                },
            ],
            None,
        );

        let result = execute_aggregate_plan_rows(
            &plan,
            vec![
                input_row([("value", BrowserScalarValue::Int64(10))]),
                input_row([("value", BrowserScalarValue::Null)]),
                input_row([("value", BrowserScalarValue::Int64(30))]),
            ],
        )
        .expect("aggregate plans should execute");

        assert_eq!(result.output_names(), ["counted", "min_value", "max_value"]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            [
                BrowserScalarValue::Int64(2),
                BrowserScalarValue::Int64(10),
                BrowserScalarValue::Int64(30),
            ]
        );
    }

    #[test]
    fn execute_aggregate_plan_returns_zero_and_nulls_for_empty_inputs() {
        let plan = aggregate_test_plan(
            vec![],
            vec![
                BrowserExecutionOutput {
                    output_name: "row_count".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::CountStar,
                        source_column: None,
                    },
                },
                BrowserExecutionOutput {
                    output_name: "sum_value".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Sum,
                        source_column: Some("value".to_string()),
                    },
                },
                BrowserExecutionOutput {
                    output_name: "avg_value".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Avg,
                        source_column: Some("value".to_string()),
                    },
                },
                BrowserExecutionOutput {
                    output_name: "min_value".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Min,
                        source_column: Some("value".to_string()),
                    },
                },
                BrowserExecutionOutput {
                    output_name: "max_value".to_string(),
                    kind: BrowserExecutionOutputKind::Aggregate {
                        function: BrowserAggregateFunction::Max,
                        source_column: Some("value".to_string()),
                    },
                },
            ],
            None,
        );

        let result = execute_aggregate_plan_rows(&plan, Vec::new())
            .expect("empty aggregates should execute");

        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            [
                BrowserScalarValue::Int64(0),
                BrowserScalarValue::Null,
                BrowserScalarValue::Null,
                BrowserScalarValue::Null,
                BrowserScalarValue::Null,
            ]
        );
    }

    #[test]
    fn execute_aggregate_plan_requires_native_for_deferred_functions() {
        let plan = aggregate_test_plan(
            vec![],
            vec![BrowserExecutionOutput {
                output_name: "values".to_string(),
                kind: BrowserExecutionOutputKind::Aggregate {
                    function: BrowserAggregateFunction::ArrayAgg,
                    source_column: Some("value".to_string()),
                },
            }],
            None,
        );

        let error = execute_aggregate_plan_rows(
            &plan,
            vec![input_row([("value", BrowserScalarValue::Int64(10))])],
        )
        .expect_err("deferred aggregate functions should require native fallback");

        assert_eq!(error.code, QueryErrorCode::FallbackRequired);
        assert_eq!(error.fallback_reason, Some(FallbackReason::NativeRequired));
    }

    fn non_aggregate_test_plan(
        filter: Option<BrowserFilterExpr>,
        outputs: Vec<BrowserExecutionOutput>,
        order_by: Vec<BrowserSortKey>,
        limit: Option<u64>,
    ) -> BrowserExecutionPlan {
        BrowserExecutionPlan {
            table_uri: "gs://axon-fixtures/sample_table".to_string(),
            snapshot_version: 4,
            scan: BrowserScanPlan {
                candidate_paths: vec!["part-000.parquet".to_string()],
                candidate_file_count: 1,
                candidate_bytes: 128,
                candidate_rows: 4,
                partition_columns: vec!["category".to_string()],
                candidate_object_etags: BTreeMap::new(),
                required_columns: vec![
                    "category".to_string(),
                    "id".to_string(),
                    "value".to_string(),
                ],
            },
            filter,
            outputs,
            aggregation: None,
            order_by,
            limit,
            pruning: BrowserPruningSummary::default(),
        }
    }

    fn aggregate_test_plan(
        group_by_columns: Vec<&str>,
        outputs: Vec<BrowserExecutionOutput>,
        order_by: Option<Vec<BrowserSortKey>>,
    ) -> BrowserExecutionPlan {
        let measures = outputs
            .iter()
            .filter_map(|output| match output.kind() {
                BrowserExecutionOutputKind::Aggregate {
                    function,
                    source_column,
                } => Some(BrowserAggregateMeasure {
                    output_name: output.output_name().to_string(),
                    function: function.clone(),
                    source_column: source_column.clone(),
                }),
                BrowserExecutionOutputKind::Passthrough { .. } => None,
            })
            .collect();

        BrowserExecutionPlan {
            table_uri: "gs://axon-fixtures/sample_table".to_string(),
            snapshot_version: 4,
            scan: BrowserScanPlan {
                candidate_paths: vec!["part-000.parquet".to_string()],
                candidate_file_count: 1,
                candidate_bytes: 128,
                candidate_rows: 4,
                partition_columns: vec!["category".to_string()],
                candidate_object_etags: BTreeMap::new(),
                required_columns: vec![
                    "category".to_string(),
                    "id".to_string(),
                    "value".to_string(),
                ],
            },
            filter: None,
            outputs,
            aggregation: Some(BrowserAggregationPlan {
                group_by_columns: group_by_columns
                    .into_iter()
                    .map(ToString::to_string)
                    .collect(),
                measures,
            }),
            order_by: order_by.unwrap_or_default(),
            limit: None,
            pruning: BrowserPruningSummary::default(),
        }
    }

    fn input_row<const N: usize>(entries: [(&str, BrowserScalarValue); N]) -> BrowserInputRow {
        entries
            .into_iter()
            .map(|(column, value)| (column.to_string(), value))
            .collect()
    }
}

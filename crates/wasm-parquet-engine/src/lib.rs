//! Browser-side Parquet planning and scan primitives.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use parquet::basic::{
    ConvertedType as RawParquetConvertedType,
    EdgeInterpolationAlgorithm as RawParquetEdgeInterpolationAlgorithm,
    LogicalType as RawParquetLogicalType, Repetition as RawParquetRepetition,
    TimeUnit as RawParquetTimeUnit, Type as RawParquetPhysicalType,
};
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::file::{metadata::ParquetMetaDataReader, statistics::Statistics as ParquetStatistics};
use parquet::record::Field as ParquetField;
use query_contract::{
    ExecutionTarget, FallbackReason, PartitionColumnType, QueryError, QueryErrorCode,
};
use wasm_http_object_store::{HttpByteRange, HttpRangeReadResult, HttpRangeReader};

pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-side Parquet planning and scan primitives.";
pub const MAX_PARQUET_FOOTER_SIZE_BYTES: u64 = 16 * 1024 * 1024;
pub const PARQUET_TRAILER_SIZE_BYTES: u64 = 8;
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

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

pub async fn read_parquet_metadata_for_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
) -> Result<ParquetFileMetadata, QueryError> {
    let footer = read_parquet_footer_for_target(reader, target, request_timeout).await?;
    parse_parquet_metadata(target, &footer)
}

pub async fn scan_target_input_rows(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
) -> Result<Vec<ParquetInputRow>, QueryError> {
    let read = reader
        .read_range_with_timeout(&target.object_source.url, HttpByteRange::Full, request_timeout)
        .await?;
    validate_full_object_read(target, &read)?;
    decode_parquet_input_rows(target, read.bytes, required_columns)?
        .into_iter()
        .map(|row| {
            merge_partition_values_into_row(
                row,
                target,
                partition_column_types,
                required_columns,
            )
        })
        .collect()
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

pub fn parse_parquet_metadata(
    target: &ScanTarget,
    footer: &ParquetFooter,
) -> Result<ParquetFileMetadata, QueryError> {
    let metadata =
        ParquetMetaDataReader::decode_metadata(footer.footer_bytes()).map_err(|error| {
            parquet_protocol_error(format!(
                "parquet metadata decode for file '{}' failed: {error}",
                target.path
            ))
        })?;
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
    let field_stats = parquet_field_stats_from_metadata(&metadata);

    Ok(ParquetFileMetadata {
        object_size_bytes: footer.object_size_bytes(),
        footer_length_bytes: footer.footer_length_bytes(),
        row_group_count,
        row_count,
        fields,
        field_stats,
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

fn validate_full_object_read(
    target: &ScanTarget,
    read: &HttpRangeReadResult,
) -> Result<(), QueryError> {
    if let Some(object_size_bytes) = read.metadata.size_bytes {
        if object_size_bytes != target.size_bytes {
            return Err(parquet_protocol_error(format!(
                "browser execution for file '{}' observed object size {} bytes, but the descriptor declared {} bytes",
                target.path, object_size_bytes, target.size_bytes,
            )));
        }
    }

    let read_len = u64::try_from(read.bytes.len())
        .map_err(|_| parquet_protocol_error("full-object read length overflowed u64"))?;
    if read_len != target.size_bytes {
        return Err(parquet_protocol_error(format!(
            "browser execution for file '{}' read {} bytes, but the descriptor declared {} bytes",
            target.path, read_len, target.size_bytes,
        )));
    }

    if let Some(expected_object_etag) = target.object_etag.as_deref() {
        match read.metadata.etag.as_deref() {
            Some(actual_object_etag) if actual_object_etag == expected_object_etag => {}
            Some(actual_object_etag) => {
                return Err(parquet_protocol_error(format!(
                    "browser execution for file '{}' observed entity tag {} during bootstrap but {} during execution",
                    target.path, expected_object_etag, actual_object_etag,
                )))
            }
            None => {
                return Err(parquet_protocol_error(format!(
                    "browser execution for file '{}' expected entity tag metadata {} from bootstrap, but the full-object read returned none",
                    target.path, expected_object_etag,
                )))
            }
        }
    }

    Ok(())
}

fn merge_partition_values_into_row(
    mut row: ParquetInputRow,
    target: &ScanTarget,
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    required_columns: &[String],
) -> Result<ParquetInputRow, QueryError> {
    let required_columns = required_columns
        .iter()
        .map(|column| normalize_name(column))
        .collect::<BTreeSet<_>>();

    for (column, value) in &target.partition_values {
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
) -> Result<ParquetScalarValue, QueryError> {
    match value {
        None => Ok(ParquetScalarValue::Null),
        Some(value) => typed_partition_scalar(column, value, partition_type),
    }
}

fn typed_partition_scalar(
    column: &str,
    value: &str,
    partition_type: PartitionColumnType,
) -> Result<ParquetScalarValue, QueryError> {
    match partition_type {
        PartitionColumnType::String => Ok(ParquetScalarValue::String(value.to_string())),
        PartitionColumnType::Boolean => parse_canonical_partition_bool(value)
            .map(ParquetScalarValue::Boolean)
            .ok_or_else(|| invalid_partition_value_error(column, value, partition_type)),
        PartitionColumnType::Int64 => parse_canonical_partition_i64(value)
            .map(ParquetScalarValue::Int64)
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

fn parquet_converted_type(
    converted_type: RawParquetConvertedType,
) -> Option<ParquetConvertedType> {
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
        RawParquetLogicalType::Geometry { crs } => ParquetLogicalType::Geometry { crs: crs.clone() },
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
        RawParquetEdgeInterpolationAlgorithm::SPHERICAL => ParquetEdgeInterpolationAlgorithm::Spherical,
        RawParquetEdgeInterpolationAlgorithm::VINCENTY => ParquetEdgeInterpolationAlgorithm::Vincenty,
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

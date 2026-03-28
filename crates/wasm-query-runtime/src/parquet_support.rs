use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use parquet::basic::{
    ConvertedType as ParquetConvertedType,
    EdgeInterpolationAlgorithm as ParquetEdgeInterpolationAlgorithm,
    LogicalType as ParquetLogicalType, Repetition as ParquetRepetition,
    TimeUnit as ParquetTimeUnit, Type as ParquetPhysicalType,
};
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::file::{metadata::ParquetMetaDataReader, statistics::Statistics as ParquetStatistics};
use parquet::record::Field as ParquetField;
use query_contract::{FallbackReason, PartitionColumnType, QueryError, QueryErrorCode};
use wasm_http_object_store::HttpRangeReadResult;

use crate::{
    execution_runtime_error, merge_partition_values_into_row, normalize_name,
    parquet_protocol_error, runtime_target, BrowserInputRow, BrowserParquetConvertedType,
    BrowserParquetEdgeInterpolationAlgorithm, BrowserParquetField, BrowserParquetFieldStats,
    BrowserParquetFileMetadata, BrowserParquetFooter, BrowserParquetLogicalType,
    BrowserParquetPhysicalType, BrowserParquetRepetition, BrowserParquetTimeUnit,
    BrowserScalarValue, MaterializedBrowserFile, PARQUET_MAGIC, PARQUET_TRAILER_SIZE_BYTES,
};

pub(super) fn decode_parquet_input_rows(
    file: &MaterializedBrowserFile,
    bytes: Bytes,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
) -> Result<Vec<BrowserInputRow>, QueryError> {
    let reader = SerializedFileReader::new(bytes).map_err(|error| {
        execution_runtime_error(format!(
            "browser runtime could not open parquet file '{}': {error}",
            file.path()
        ))
    })?;
    let row_iter = reader.get_row_iter(None).map_err(|error| {
        execution_runtime_error(format!(
            "browser runtime could not iterate parquet rows for file '{}': {error}",
            file.path()
        ))
    })?;
    let mut rows = Vec::new();

    for row in row_iter {
        let row = row.map_err(|error| {
            execution_runtime_error(format!(
                "browser runtime could not decode a parquet row for file '{}': {error}",
                file.path()
            ))
        })?;
        rows.push(merge_partition_values_into_row(
            parquet_row_to_input_row(file.path(), &row, required_columns)?,
            file.partition_values(),
            partition_column_types,
            required_columns,
        )?);
    }

    Ok(rows)
}

pub(super) fn parquet_row_to_input_row(
    file_path: &str,
    row: &parquet::record::Row,
    required_columns: &[String],
) -> Result<BrowserInputRow, QueryError> {
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
        if !required_columns.is_empty() && !required_columns.contains(&normalized_name) {
            continue;
        }

        values.insert(
            normalized_name,
            parquet_field_to_scalar(file_path, column_name, field)?,
        );
    }

    Ok(values)
}

fn parquet_field_to_scalar(
    file_path: &str,
    column_name: &str,
    field: &ParquetField,
) -> Result<BrowserScalarValue, QueryError> {
    match field {
        ParquetField::Null => Ok(BrowserScalarValue::Null),
        ParquetField::Bool(value) => Ok(BrowserScalarValue::Boolean(*value)),
        ParquetField::Byte(value) => Ok(BrowserScalarValue::Int64(i64::from(*value))),
        ParquetField::Short(value) => Ok(BrowserScalarValue::Int64(i64::from(*value))),
        ParquetField::Int(value) => Ok(BrowserScalarValue::Int64(i64::from(*value))),
        ParquetField::Long(value) => Ok(BrowserScalarValue::Int64(*value)),
        ParquetField::UByte(value) => Ok(BrowserScalarValue::Int64(i64::from(*value))),
        ParquetField::UShort(value) => Ok(BrowserScalarValue::Int64(i64::from(*value))),
        ParquetField::UInt(value) => Ok(BrowserScalarValue::Int64(i64::from(*value))),
        ParquetField::ULong(value) => i64::try_from(*value)
            .map(BrowserScalarValue::Int64)
            .map_err(|_| unsupported_execution_scalar(file_path, column_name, field)),
        ParquetField::Str(value) => Ok(BrowserScalarValue::String(value.clone())),
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

pub(super) fn parse_parquet_footer_trailer(
    trailer: &HttpRangeReadResult,
) -> Result<(u64, u32), QueryError> {
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

pub(super) fn validate_parquet_footer_read_consistency(
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

pub(super) fn parse_parquet_metadata(
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

pub(super) fn format_browser_fields(fields: &[BrowserParquetField]) -> String {
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

pub(super) fn format_partition_columns(partition_columns: &[String]) -> String {
    if partition_columns.is_empty() {
        return "<empty>".to_string();
    }

    partition_columns.join(", ")
}

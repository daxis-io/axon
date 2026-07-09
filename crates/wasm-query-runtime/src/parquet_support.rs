use std::collections::BTreeMap;
use std::time::Duration;

use arrow_array::{
    Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use bytes::Bytes;
use futures_util::Stream;
use query_contract::{FallbackReason, PartitionColumnType, QueryError, QueryErrorCode};
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine as parquet_engine;

use crate::{
    runtime_target, BrowserInputRow, BrowserObjectSource, BrowserParquetConvertedType,
    BrowserParquetEdgeInterpolationAlgorithm, BrowserParquetField, BrowserParquetFieldStats,
    BrowserParquetFileMetadata, BrowserParquetFooter, BrowserParquetLogicalType,
    BrowserParquetPhysicalType, BrowserParquetRepetition, BrowserParquetTimeUnit,
    BrowserScalarValue, MaterializedBrowserFile,
};

pub(super) fn engine_object_source(source: &BrowserObjectSource) -> parquet_engine::ObjectSource {
    parquet_engine::ObjectSource::new(source.url().to_string())
}

pub(super) fn engine_scan_target(file: &MaterializedBrowserFile) -> parquet_engine::ScanTarget {
    engine_scan_target_with_object_etag(file, None)
}

pub(super) fn engine_scan_target_with_object_etag(
    file: &MaterializedBrowserFile,
    object_etag: Option<&str>,
) -> parquet_engine::ScanTarget {
    parquet_engine::ScanTarget {
        object_source: engine_object_source(file.object_source()),
        object_etag: object_etag.map(str::to_string),
        path: file.path().to_string(),
        size_bytes: file.size_bytes(),
        partition_values: file.partition_values().clone(),
    }
}

pub(super) fn browser_footer_from_engine(
    footer: parquet_engine::ParquetFooter,
) -> BrowserParquetFooter {
    BrowserParquetFooter {
        object_size_bytes: footer.object_size_bytes(),
        object_etag: footer.object_etag().map(str::to_string),
        footer_length_bytes: footer.footer_length_bytes(),
        footer_bytes: Bytes::copy_from_slice(footer.footer_bytes()),
    }
}

pub(super) fn browser_metadata_from_engine(
    metadata: parquet_engine::ParquetFileMetadata,
) -> BrowserParquetFileMetadata {
    BrowserParquetFileMetadata {
        object_size_bytes: metadata.object_size_bytes,
        footer_length_bytes: metadata.footer_length_bytes,
        row_group_count: metadata.row_group_count,
        row_count: metadata.row_count,
        fields: metadata
            .fields
            .into_iter()
            .map(browser_field_from_engine)
            .collect(),
        field_stats: metadata
            .field_stats
            .into_iter()
            .map(|(name, stats)| (name, browser_field_stats_from_engine(stats)))
            .collect(),
    }
}

pub(super) async fn stream_scan_target_batches(
    reader: &HttpRangeReader,
    target: &parquet_engine::ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
    row_group_predicate: Option<&parquet_engine::ParquetRowGroupPruningPredicate>,
    metadata_cache: Option<&parquet_engine::ParquetMetadataCache>,
    range_cache: Option<&parquet_engine::ParquetRangeCache>,
) -> Result<
    parquet_engine::ScanTargetBatchStream<impl Stream<Item = Result<RecordBatch, QueryError>>>,
    QueryError,
> {
    parquet_engine::stream_scan_target_batches_with_row_group_pruning_and_caches(
        reader,
        target,
        required_columns,
        partition_column_types,
        request_timeout,
        row_group_predicate,
        metadata_cache,
        range_cache,
    )
    .await
}

pub(super) fn browser_rows_from_record_batch(
    batch: &RecordBatch,
) -> Result<Vec<BrowserInputRow>, QueryError> {
    let mut rows = (0..batch.num_rows())
        .map(|_| BTreeMap::new())
        .collect::<Vec<BrowserInputRow>>();

    for (field, column) in batch.schema().fields().iter().zip(batch.columns().iter()) {
        for (row_index, row) in rows.iter_mut().enumerate() {
            row.insert(
                field.name().to_string(),
                browser_scalar_from_arrow_array(field.name(), column.as_ref(), row_index)?,
            );
        }
    }

    Ok(rows)
}

#[cfg(test)]
pub(super) fn parquet_row_to_input_row(
    file_path: &str,
    row: &parquet::record::Row,
    required_columns: &[String],
) -> Result<BrowserInputRow, QueryError> {
    let row = parquet_engine::parquet_row_to_input_row(
        &parquet_engine::ScanTarget {
            object_source: parquet_engine::ObjectSource::new("https://example.com/object"),
            object_etag: None,
            path: file_path.to_string(),
            size_bytes: 0,
            partition_values: BTreeMap::new(),
        },
        row,
        required_columns,
    )?;
    Ok(browser_row_from_engine(row))
}

#[cfg(test)]
fn browser_row_from_engine(row: parquet_engine::ParquetInputRow) -> BrowserInputRow {
    row.into_iter()
        .map(|(column, value)| (column, browser_scalar_from_engine(value)))
        .collect()
}

#[cfg(test)]
fn browser_scalar_from_engine(value: parquet_engine::ParquetScalarValue) -> BrowserScalarValue {
    match value {
        parquet_engine::ParquetScalarValue::Null => BrowserScalarValue::Null,
        parquet_engine::ParquetScalarValue::Boolean(value) => BrowserScalarValue::Boolean(value),
        parquet_engine::ParquetScalarValue::Int64(value) => BrowserScalarValue::Int64(value),
        parquet_engine::ParquetScalarValue::String(value) => BrowserScalarValue::String(value),
    }
}

fn browser_scalar_from_arrow_array(
    column_name: &str,
    column: &dyn Array,
    row_index: usize,
) -> Result<BrowserScalarValue, QueryError> {
    if column.is_null(row_index) {
        return Ok(BrowserScalarValue::Null);
    }

    match column.data_type() {
        arrow_schema::DataType::Boolean => column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|column| BrowserScalarValue::Boolean(column.value(row_index)))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::Int8 => column
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|column| BrowserScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::Int16 => column
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|column| BrowserScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::Int32 => column
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|column| BrowserScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|column| BrowserScalarValue::Int64(column.value(row_index)))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::UInt8 => column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|column| BrowserScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::UInt16 => column
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|column| BrowserScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::UInt32 => column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|column| BrowserScalarValue::Int64(i64::from(column.value(row_index))))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::UInt64 => column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .and_then(|column| i64::try_from(column.value(row_index)).ok())
            .map(BrowserScalarValue::Int64)
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        arrow_schema::DataType::Utf8 => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|column| BrowserScalarValue::String(column.value(row_index).to_string()))
            .ok_or_else(|| unsupported_execution_array(column_name, column.data_type())),
        _ => Err(unsupported_execution_array(column_name, column.data_type())),
    }
}

fn unsupported_execution_array(
    column_name: &str,
    data_type: &arrow_schema::DataType,
) -> QueryError {
    QueryError::new(
        QueryErrorCode::FallbackRequired,
        format!(
            "browser runtime cannot execute Arrow field '{}' with data type '{data_type:?}'",
            column_name
        ),
        runtime_target(),
    )
    .with_fallback_reason(FallbackReason::NativeRequired)
}

fn browser_field_from_engine(field: parquet_engine::ParquetColumnField) -> BrowserParquetField {
    BrowserParquetField {
        name: field.name,
        physical_type: browser_physical_type_from_engine(field.physical_type),
        logical_type: field.logical_type.map(browser_logical_type_from_engine),
        converted_type: field.converted_type.map(browser_converted_type_from_engine),
        repetition: browser_repetition_from_engine(field.repetition),
        nullable: field.nullable,
        max_definition_level: field.max_definition_level,
        max_repetition_level: field.max_repetition_level,
        type_length: field.type_length,
        precision: field.precision,
        scale: field.scale,
    }
}

fn browser_field_stats_from_engine(
    stats: parquet_engine::ParquetFieldStats,
) -> BrowserParquetFieldStats {
    BrowserParquetFieldStats {
        min_i64: stats.min_i64,
        max_i64: stats.max_i64,
        null_count: stats.null_count,
    }
}

fn browser_physical_type_from_engine(
    physical_type: parquet_engine::ParquetPhysicalType,
) -> BrowserParquetPhysicalType {
    match physical_type {
        parquet_engine::ParquetPhysicalType::Boolean => BrowserParquetPhysicalType::Boolean,
        parquet_engine::ParquetPhysicalType::Int32 => BrowserParquetPhysicalType::Int32,
        parquet_engine::ParquetPhysicalType::Int64 => BrowserParquetPhysicalType::Int64,
        parquet_engine::ParquetPhysicalType::Int96 => BrowserParquetPhysicalType::Int96,
        parquet_engine::ParquetPhysicalType::Float => BrowserParquetPhysicalType::Float,
        parquet_engine::ParquetPhysicalType::Double => BrowserParquetPhysicalType::Double,
        parquet_engine::ParquetPhysicalType::ByteArray => BrowserParquetPhysicalType::ByteArray,
        parquet_engine::ParquetPhysicalType::FixedLenByteArray => {
            BrowserParquetPhysicalType::FixedLenByteArray
        }
    }
}

fn browser_repetition_from_engine(
    repetition: parquet_engine::ParquetRepetition,
) -> BrowserParquetRepetition {
    match repetition {
        parquet_engine::ParquetRepetition::Required => BrowserParquetRepetition::Required,
        parquet_engine::ParquetRepetition::Optional => BrowserParquetRepetition::Optional,
        parquet_engine::ParquetRepetition::Repeated => BrowserParquetRepetition::Repeated,
    }
}

fn browser_converted_type_from_engine(
    converted_type: parquet_engine::ParquetConvertedType,
) -> BrowserParquetConvertedType {
    match converted_type {
        parquet_engine::ParquetConvertedType::Utf8 => BrowserParquetConvertedType::Utf8,
        parquet_engine::ParquetConvertedType::Map => BrowserParquetConvertedType::Map,
        parquet_engine::ParquetConvertedType::MapKeyValue => {
            BrowserParquetConvertedType::MapKeyValue
        }
        parquet_engine::ParquetConvertedType::List => BrowserParquetConvertedType::List,
        parquet_engine::ParquetConvertedType::Enum => BrowserParquetConvertedType::Enum,
        parquet_engine::ParquetConvertedType::Decimal => BrowserParquetConvertedType::Decimal,
        parquet_engine::ParquetConvertedType::Date => BrowserParquetConvertedType::Date,
        parquet_engine::ParquetConvertedType::TimeMillis => BrowserParquetConvertedType::TimeMillis,
        parquet_engine::ParquetConvertedType::TimeMicros => BrowserParquetConvertedType::TimeMicros,
        parquet_engine::ParquetConvertedType::TimestampMillis => {
            BrowserParquetConvertedType::TimestampMillis
        }
        parquet_engine::ParquetConvertedType::TimestampMicros => {
            BrowserParquetConvertedType::TimestampMicros
        }
        parquet_engine::ParquetConvertedType::UInt8 => BrowserParquetConvertedType::UInt8,
        parquet_engine::ParquetConvertedType::UInt16 => BrowserParquetConvertedType::UInt16,
        parquet_engine::ParquetConvertedType::UInt32 => BrowserParquetConvertedType::UInt32,
        parquet_engine::ParquetConvertedType::UInt64 => BrowserParquetConvertedType::UInt64,
        parquet_engine::ParquetConvertedType::Int8 => BrowserParquetConvertedType::Int8,
        parquet_engine::ParquetConvertedType::Int16 => BrowserParquetConvertedType::Int16,
        parquet_engine::ParquetConvertedType::Int32 => BrowserParquetConvertedType::Int32,
        parquet_engine::ParquetConvertedType::Int64 => BrowserParquetConvertedType::Int64,
        parquet_engine::ParquetConvertedType::Json => BrowserParquetConvertedType::Json,
        parquet_engine::ParquetConvertedType::Bson => BrowserParquetConvertedType::Bson,
        parquet_engine::ParquetConvertedType::Interval => BrowserParquetConvertedType::Interval,
    }
}

fn browser_logical_type_from_engine(
    logical_type: parquet_engine::ParquetLogicalType,
) -> BrowserParquetLogicalType {
    match logical_type {
        parquet_engine::ParquetLogicalType::String => BrowserParquetLogicalType::String,
        parquet_engine::ParquetLogicalType::Map => BrowserParquetLogicalType::Map,
        parquet_engine::ParquetLogicalType::List => BrowserParquetLogicalType::List,
        parquet_engine::ParquetLogicalType::Enum => BrowserParquetLogicalType::Enum,
        parquet_engine::ParquetLogicalType::Decimal { scale, precision } => {
            BrowserParquetLogicalType::Decimal { scale, precision }
        }
        parquet_engine::ParquetLogicalType::Date => BrowserParquetLogicalType::Date,
        parquet_engine::ParquetLogicalType::Time {
            is_adjusted_to_utc,
            unit,
        } => BrowserParquetLogicalType::Time {
            is_adjusted_to_utc,
            unit: browser_time_unit_from_engine(unit),
        },
        parquet_engine::ParquetLogicalType::Timestamp {
            is_adjusted_to_utc,
            unit,
        } => BrowserParquetLogicalType::Timestamp {
            is_adjusted_to_utc,
            unit: browser_time_unit_from_engine(unit),
        },
        parquet_engine::ParquetLogicalType::Integer {
            bit_width,
            is_signed,
        } => BrowserParquetLogicalType::Integer {
            bit_width,
            is_signed,
        },
        parquet_engine::ParquetLogicalType::Unknown => BrowserParquetLogicalType::Unknown,
        parquet_engine::ParquetLogicalType::Json => BrowserParquetLogicalType::Json,
        parquet_engine::ParquetLogicalType::Bson => BrowserParquetLogicalType::Bson,
        parquet_engine::ParquetLogicalType::Uuid => BrowserParquetLogicalType::Uuid,
        parquet_engine::ParquetLogicalType::Float16 => BrowserParquetLogicalType::Float16,
        parquet_engine::ParquetLogicalType::Variant {
            specification_version,
        } => BrowserParquetLogicalType::Variant {
            specification_version,
        },
        parquet_engine::ParquetLogicalType::Geometry { crs } => {
            BrowserParquetLogicalType::Geometry { crs }
        }
        parquet_engine::ParquetLogicalType::Geography { crs, algorithm } => {
            BrowserParquetLogicalType::Geography {
                crs,
                algorithm: algorithm.map(browser_edge_interpolation_algorithm_from_engine),
            }
        }
        parquet_engine::ParquetLogicalType::Unrecognized { field_id } => {
            BrowserParquetLogicalType::Unrecognized { field_id }
        }
    }
}

fn browser_time_unit_from_engine(unit: parquet_engine::ParquetTimeUnit) -> BrowserParquetTimeUnit {
    match unit {
        parquet_engine::ParquetTimeUnit::Millis => BrowserParquetTimeUnit::Millis,
        parquet_engine::ParquetTimeUnit::Micros => BrowserParquetTimeUnit::Micros,
        parquet_engine::ParquetTimeUnit::Nanos => BrowserParquetTimeUnit::Nanos,
    }
}

fn browser_edge_interpolation_algorithm_from_engine(
    algorithm: parquet_engine::ParquetEdgeInterpolationAlgorithm,
) -> BrowserParquetEdgeInterpolationAlgorithm {
    match algorithm {
        parquet_engine::ParquetEdgeInterpolationAlgorithm::Spherical => {
            BrowserParquetEdgeInterpolationAlgorithm::Spherical
        }
        parquet_engine::ParquetEdgeInterpolationAlgorithm::Vincenty => {
            BrowserParquetEdgeInterpolationAlgorithm::Vincenty
        }
        parquet_engine::ParquetEdgeInterpolationAlgorithm::Thomas => {
            BrowserParquetEdgeInterpolationAlgorithm::Thomas
        }
        parquet_engine::ParquetEdgeInterpolationAlgorithm::Andoyer => {
            BrowserParquetEdgeInterpolationAlgorithm::Andoyer
        }
        parquet_engine::ParquetEdgeInterpolationAlgorithm::Karney => {
            BrowserParquetEdgeInterpolationAlgorithm::Karney
        }
        parquet_engine::ParquetEdgeInterpolationAlgorithm::Unknown(code) => {
            BrowserParquetEdgeInterpolationAlgorithm::Unknown(code)
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

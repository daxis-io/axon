use std::collections::BTreeMap;
use std::io::Cursor;

use arrow_array::cast::{
    as_boolean_array, as_largestring_array, as_primitive_array, as_string_array,
};
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::Array;
use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType as ArrowDataType;
use js_sys::{Object, Reflect, Uint8Array};
use query_contract::{
    BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryError, QueryErrorCode, QueryRequest,
    QueryResponse, SnapshotResolutionRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;
use wasm_datafusion_session::{BrowserDataFusionCancellation, BrowserDataFusionSession};
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
    DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{ObjectSource, ScanTarget};
use wasm_query_runtime::{BrowserObjectAccessMode, BrowserRuntimeConfig};

const DEFAULT_QUERY_SESSION_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_QUERY_PREVIEW_LIMIT: usize = 100;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    objects: Vec<ManifestObjectInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestObjectInput {
    relative_path: String,
    url: String,
    #[serde(default)]
    size_bytes: Option<u64>,
    #[serde(default)]
    etag: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParquetPreflightTargetInput {
    path: String,
    url: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    #[serde(default)]
    stats: Option<String>,
}

#[derive(Debug, Serialize)]
struct ParquetPreflightOutput {
    path: String,
    url: String,
    #[serde(serialize_with = "serialize_decimal_string")]
    size_bytes: u64,
    partition_values: BTreeMap<String, Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta_stats: Option<String>,
    #[serde(serialize_with = "serialize_decimal_string")]
    footer_length_bytes: u32,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_group_count: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    fields: Vec<ParquetPreflightField>,
    field_stats: BTreeMap<String, ParquetPreflightFieldStats>,
}

#[derive(Debug, Serialize)]
struct ParquetPreflightField {
    name: String,
    physical_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    logical_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    converted_type: Option<String>,
    repetition: String,
    nullable: bool,
}

#[derive(Debug, Serialize)]
struct ParquetPreflightFieldStats {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal_string"
    )]
    min_i64: Option<i64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal_string"
    )]
    max_i64: Option<i64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal_string"
    )]
    null_count: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SandboxOpenTableOutput {
    cache_metrics: SandboxCacheMetrics,
}

#[derive(Debug, Serialize)]
struct SandboxSqlMetadata {
    response: QueryResponse,
    preview: QueryPreviewOutput,
    #[serde(serialize_with = "serialize_decimal_string")]
    arrow_ipc_byte_length: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    cache_metrics: SandboxCacheMetrics,
}

#[derive(Debug, Serialize)]
struct QueryPreviewOutput {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    #[serde(serialize_with = "serialize_decimal_string")]
    row_count: u64,
    preview_row_limit: usize,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct SandboxCacheMetrics {
    #[serde(serialize_with = "serialize_decimal_string")]
    session_cached_bytes: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    session_table_count: u64,
    #[serde(serialize_with = "serialize_decimal_string")]
    max_session_cached_bytes: u64,
}

fn serialize_decimal_string<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_optional_decimal_string<T, S>(
    value: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

#[wasm_bindgen]
pub struct SandboxQueryCancellation {
    cancellation: BrowserDataFusionCancellation,
}

#[wasm_bindgen]
impl SandboxQueryCancellation {
    pub fn cancel(&self) {
        self.cancellation.cancel_running_queries();
    }
}

#[wasm_bindgen]
pub struct SandboxQuerySession {
    session: BrowserDataFusionSession,
    cancellation: BrowserDataFusionCancellation,
}

#[wasm_bindgen]
impl SandboxQuerySession {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<SandboxQuerySession, JsValue> {
        let runtime_config = BrowserRuntimeConfig {
            object_access_mode: BrowserObjectAccessMode::BrowserSafeHttp,
            allow_cloud_credentials: false,
            ..BrowserRuntimeConfig::default()
        };
        let session =
            BrowserDataFusionSession::new(runtime_config, DEFAULT_QUERY_SESSION_CACHE_BYTES)
                .map_err(query_error_to_js_value)?;
        let cancellation = session.cancellation_handle();

        Ok(Self {
            session,
            cancellation,
        })
    }

    pub fn cancellation(&self) -> SandboxQueryCancellation {
        SandboxQueryCancellation {
            cancellation: self.cancellation.clone(),
        }
    }

    pub async fn open_delta_table(
        &mut self,
        name: String,
        snapshot_json: String,
    ) -> Result<String, JsValue> {
        let snapshot = serde_json::from_str::<BrowserHttpSnapshotDescriptor>(&snapshot_json)
            .map_err(|error| JsValue::from_str(&format!("invalid snapshot descriptor: {error}")))?;
        self.session
            .open_delta_table(name.clone(), snapshot)
            .await
            .map_err(query_error_to_js_value)?;

        serde_json::to_string(&SandboxOpenTableOutput {
            cache_metrics: cache_metrics(&self.session),
        })
        .map_err(|error| {
            JsValue::from_str(&format!(
                "open table metadata serialization failed: {error}"
            ))
        })
    }

    pub async fn inspect_parquet(&mut self, name: String, path: String) -> Result<String, JsValue> {
        let summary = self
            .session
            .inspect_parquet(&name, &path)
            .await
            .map_err(query_error_to_js_value)?;
        serde_json::to_string(&summary).map_err(|error| {
            JsValue::from_str(&format!("Parquet inspection serialization failed: {error}"))
        })
    }

    pub async fn sql(
        &mut self,
        name: String,
        request_json: String,
        preview_limit: u32,
    ) -> Result<JsValue, JsValue> {
        let request = serde_json::from_str::<QueryRequest>(&request_json)
            .map_err(|error| JsValue::from_str(&format!("invalid query request: {error}")))?;
        let result = self
            .session
            .sql(&name, &request)
            .await
            .map_err(query_error_to_js_value)?;
        let arrow_ipc_bytes = result.runtime_result.ipc_bytes.to_vec();
        let preview = preview_from_arrow_ipc(&arrow_ipc_bytes, preview_limit)
            .map_err(query_error_to_js_value)?;
        let metadata = SandboxSqlMetadata {
            response: result.response,
            row_count: result.runtime_result.row_count,
            arrow_ipc_byte_length: result.runtime_result.encoded_bytes,
            preview,
            cache_metrics: cache_metrics(&self.session),
        };

        sql_bridge_value(&metadata, &arrow_ipc_bytes)
    }
}

#[wasm_bindgen]
pub async fn resolve_delta_snapshot_from_manifest(
    manifest_json: String,
    table_uri: String,
) -> Result<String, JsValue> {
    let manifest_input = serde_json::from_str::<ManifestInput>(&manifest_json)
        .map_err(|error| JsValue::from_str(&format!("invalid Delta log manifest: {error}")))?;
    let objects = manifest_input
        .objects
        .into_iter()
        .map(|object| {
            BrowserDeltaLogObject::new(object.relative_path, object.url)
                .with_metadata(object.size_bytes, object.etag)
        })
        .collect::<Vec<_>>();
    let manifest = BrowserDeltaLogManifest::new(table_uri.clone(), objects)
        .map_err(query_error_to_js_value)?;
    let resolver = SnapshotResolver::new(
        BrowserHttpDeltaLogStorageHandler::new(manifest),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri,
            snapshot_version: None,
        })
        .await
        .map_err(query_error_to_js_value)?;

    serde_json::to_string(&snapshot)
        .map_err(|error| JsValue::from_str(&format!("snapshot serialization failed: {error}")))
}

#[wasm_bindgen]
pub async fn preflight_parquet_metadata_for_targets(
    targets_json: String,
) -> Result<String, JsValue> {
    let targets = serde_json::from_str::<Vec<ParquetPreflightTargetInput>>(&targets_json).map_err(
        |error| JsValue::from_str(&format!("invalid Parquet preflight targets: {error}")),
    )?;
    let reader = HttpRangeReader::new();
    let mut outputs = Vec::with_capacity(targets.len());

    for target in targets {
        let scan_target = ScanTarget {
            object_source: ObjectSource::new(target.url.clone()),
            object_etag: None,
            path: target.path.clone(),
            size_bytes: target.size_bytes,
            partition_values: target.partition_values.clone(),
        };
        let metadata =
            wasm_parquet_engine::read_parquet_metadata_for_target(&reader, &scan_target, None)
                .await
                .map_err(query_error_to_js_value)?;
        outputs.push(ParquetPreflightOutput {
            path: target.path,
            url: target.url,
            size_bytes: metadata.object_size_bytes,
            partition_values: target.partition_values,
            delta_stats: target.stats,
            footer_length_bytes: metadata.footer_length_bytes,
            row_group_count: metadata.row_group_count,
            row_count: metadata.row_count,
            fields: metadata
                .fields
                .into_iter()
                .map(parquet_preflight_field)
                .collect(),
            field_stats: metadata
                .field_stats
                .into_iter()
                .map(|(name, stats)| {
                    (
                        name,
                        ParquetPreflightFieldStats {
                            min_i64: stats.min_i64,
                            max_i64: stats.max_i64,
                            null_count: stats.null_count,
                        },
                    )
                })
                .collect(),
        });
    }

    serde_json::to_string(&outputs).map_err(|error| {
        JsValue::from_str(&format!("Parquet preflight serialization failed: {error}"))
    })
}

fn cache_metrics(session: &BrowserDataFusionSession) -> SandboxCacheMetrics {
    SandboxCacheMetrics {
        session_cached_bytes: session.cached_bytes(),
        session_table_count: u64::try_from(session.table_count()).unwrap_or(u64::MAX),
        max_session_cached_bytes: session.max_cached_bytes(),
    }
}

fn sql_bridge_value(metadata: &SandboxSqlMetadata, bytes: &[u8]) -> Result<JsValue, JsValue> {
    let metadata_json = serde_json::to_string(metadata).map_err(|error| {
        JsValue::from_str(&format!(
            "SQL result metadata serialization failed: {error}"
        ))
    })?;
    let object = Object::new();
    Reflect::set(
        &object,
        &JsValue::from_str("metadata_json"),
        &JsValue::from_str(&metadata_json),
    )?;
    let arrow_ipc_bytes = Uint8Array::from(bytes);
    Reflect::set(
        &object,
        &JsValue::from_str("arrow_ipc_bytes"),
        arrow_ipc_bytes.as_ref(),
    )?;

    Ok(object.into())
}

fn preview_from_arrow_ipc(
    bytes: &[u8],
    requested_limit: u32,
) -> Result<QueryPreviewOutput, QueryError> {
    let preview_row_limit = usize::try_from(requested_limit).unwrap_or(DEFAULT_QUERY_PREVIEW_LIMIT);
    let preview_row_limit = preview_row_limit.min(DEFAULT_QUERY_PREVIEW_LIMIT);
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).map_err(preview_error)?;
    let schema = reader.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut row_count = 0_u64;

    for batch in &mut reader {
        let batch = batch.map_err(preview_error)?;
        for row_index in 0..batch.num_rows() {
            row_count = row_count
                .checked_add(1)
                .ok_or_else(|| preview_runtime_error("preview row count overflowed u64"))?;
            if rows.len() >= preview_row_limit {
                continue;
            }

            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| preview_value(array.as_ref(), row_index))
                    .collect(),
            );
        }
    }

    Ok(QueryPreviewOutput {
        columns,
        rows,
        row_count,
        preview_row_limit,
        truncated: row_count > u64::try_from(preview_row_limit).unwrap_or(u64::MAX),
    })
}

fn preview_value(array: &dyn Array, row_index: usize) -> Value {
    if array.is_null(row_index) {
        return Value::Null;
    }

    match array.data_type() {
        ArrowDataType::Boolean => json!(as_boolean_array(array).value(row_index)),
        ArrowDataType::Int32 => json!(as_primitive_array::<Int32Type>(array).value(row_index)),
        ArrowDataType::Int64 => json!(as_primitive_array::<Int64Type>(array)
            .value(row_index)
            .to_string()),
        ArrowDataType::Float32 => json!(as_primitive_array::<Float32Type>(array).value(row_index)),
        ArrowDataType::Float64 => {
            let value = as_primitive_array::<Float64Type>(array).value(row_index);
            serde_json::Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ArrowDataType::Utf8 => json!(as_string_array(array).value(row_index)),
        ArrowDataType::LargeUtf8 => json!(as_largestring_array(array).value(row_index)),
        other => json!(format!("<unsupported {other}>")),
    }
}

fn preview_error(error: impl std::fmt::Display) -> QueryError {
    preview_runtime_error(format!("Arrow IPC preview decode failed: {error}"))
}

fn preview_runtime_error(message: impl Into<String>) -> QueryError {
    QueryError::new(
        QueryErrorCode::ExecutionFailed,
        message,
        ExecutionTarget::BrowserWasm,
    )
}

fn parquet_preflight_field(
    field: wasm_parquet_engine::ParquetColumnField,
) -> ParquetPreflightField {
    ParquetPreflightField {
        name: field.name,
        physical_type: format!("{:?}", field.physical_type),
        logical_type: field.logical_type.map(|value| format!("{value:?}")),
        converted_type: field.converted_type.map(|value| format!("{value:?}")),
        repetition: format!("{:?}", field.repetition),
        nullable: field.nullable,
    }
}

fn query_error_to_js_value(error: QueryError) -> JsValue {
    let fallback = format!("{:?}: {}", error.code, error.message);
    JsValue::from_str(&serde_json::to_string(&error).unwrap_or(fallback))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parquet_preflight_output_serializes_large_numeric_fields_as_decimal_strings() {
        let output = ParquetPreflightOutput {
            path: "part-000.parquet".to_string(),
            url: "https://example.com/part-000.parquet".to_string(),
            size_bytes: 9_007_199_254_740_993,
            partition_values: BTreeMap::new(),
            delta_stats: None,
            footer_length_bytes: u32::MAX,
            row_group_count: 9_007_199_254_740_994,
            row_count: 9_007_199_254_740_995,
            fields: Vec::new(),
            field_stats: BTreeMap::from([(
                "id".to_string(),
                ParquetPreflightFieldStats {
                    min_i64: Some(9_007_199_254_740_996),
                    max_i64: Some(9_007_199_254_740_997),
                    null_count: Some(9_007_199_254_740_998),
                },
            )]),
        };

        let serialized = serde_json::to_value(output).expect("preflight output should serialize");

        assert_eq!(serialized["size_bytes"], json!("9007199254740993"));
        assert_eq!(serialized["footer_length_bytes"], json!("4294967295"));
        assert_eq!(serialized["row_group_count"], json!("9007199254740994"));
        assert_eq!(serialized["row_count"], json!("9007199254740995"));
        assert_eq!(
            serialized["field_stats"]["id"],
            json!({
                "min_i64": "9007199254740996",
                "max_i64": "9007199254740997",
                "null_count": "9007199254740998"
            })
        );
    }
}

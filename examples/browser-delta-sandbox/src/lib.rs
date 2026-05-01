use std::collections::BTreeMap;

use query_contract::{QueryError, SnapshotResolutionRequest};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
    DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};
use wasm_http_object_store::HttpRangeReader;
use wasm_parquet_engine::{ObjectSource, ScanTarget};

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

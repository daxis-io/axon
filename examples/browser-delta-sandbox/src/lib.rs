use query_contract::{QueryError, SnapshotResolutionRequest};
use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
    DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};

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

fn query_error_to_js_value(error: QueryError) -> JsValue {
    let fallback = format!("{:?}: {}", error.code, error.message);
    JsValue::from_str(&serde_json::to_string(&error).unwrap_or(fallback))
}

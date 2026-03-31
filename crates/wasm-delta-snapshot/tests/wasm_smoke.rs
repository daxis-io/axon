#![cfg(target_arch = "wasm32")]

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use query_contract::{
    CapabilityKey, CapabilityReport, CapabilityState, ExecutionTarget, PartitionColumnType,
    QueryError, ResolvedFileDescriptor, SnapshotResolutionRequest,
};
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_delta_snapshot::{
    runtime_target, DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver, StorageHandler,
};

#[wasm_bindgen_test]
fn delta_snapshot_runtime_target_is_browser_wasm() {
    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
}

#[wasm_bindgen_test]
async fn delta_snapshot_resolver_reconstructs_metadata_and_capabilities_in_wasm() {
    let resolver = SnapshotResolver::new(
        InMemoryStorageHandler::new(
            vec!["_delta_log/00000000000000000000.json".to_string()],
            BTreeMap::from([(
                "_delta_log/00000000000000000000.json".to_string(),
                Bytes::from_static(
                    concat!(
                        "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,",
                        "\"readerFeatures\":[\"deletionVectors\"],",
                        "\"writerFeatures\":[\"deletionVectors\"]}}\n",
                        "{\"metaData\":{\"id\":\"test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},",
                        "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},{\\\"name\\\":\\\"year\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}}]}\",",
                        "\"partitionColumns\":[\"year\"],\"configuration\":{\"delta.enableDeletionVectors\":\"true\"}}}\n",
                        "{\"add\":{\"path\":\"year=2024/part-000.parquet\",\"size\":10,\"partitionValues\":{\"year\":\"2024\"}}}\n"
                    )
                    .as_bytes(),
                ),
            )]),
        ),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: "gs://axon-fixtures/synthetic".to_string(),
            snapshot_version: None,
        })
        .await
        .expect("synthetic Delta log should resolve in wasm");

    assert_eq!(snapshot.snapshot_version, 0);
    assert_eq!(
        snapshot.partition_column_types,
        BTreeMap::from([("year".to_string(), PartitionColumnType::Int64)])
    );
    assert_eq!(
        snapshot.required_capabilities,
        CapabilityReport::from_pairs([(
            CapabilityKey::DeletionVectors,
            CapabilityState::NativeOnly,
        )])
    );
    assert_eq!(
        snapshot.active_files,
        vec![ResolvedFileDescriptor {
            path: "year=2024/part-000.parquet".to_string(),
            size_bytes: 10,
            partition_values: BTreeMap::from([("year".to_string(), Some("2024".to_string()))]),
        }]
    );
}

#[derive(Clone, Debug)]
struct InMemoryStorageHandler {
    listed_paths: Vec<String>,
    bytes_by_path: BTreeMap<String, Bytes>,
}

impl InMemoryStorageHandler {
    fn new(listed_paths: Vec<String>, bytes_by_path: BTreeMap<String, Bytes>) -> Self {
        Self {
            listed_paths,
            bytes_by_path,
        }
    }
}

#[async_trait(?Send)]
impl StorageHandler for InMemoryStorageHandler {
    async fn list_paths(&self, _table_uri: &str, prefix: &str) -> Result<Vec<String>, QueryError> {
        Ok(self
            .listed_paths
            .iter()
            .filter(|path| path.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn read_bytes(
        &self,
        _table_uri: &str,
        relative_path: &str,
    ) -> Result<Option<Bytes>, QueryError> {
        Ok(self.bytes_by_path.get(relative_path).cloned())
    }
}

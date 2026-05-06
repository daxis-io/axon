#![cfg(not(target_arch = "wasm32"))]

use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use query_contract::{PartitionColumnType, QueryErrorCode};
use wasm_datafusion_poc::{DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine};

#[tokio::test]
async fn open_delta_table_registers_descriptor_name_in_catalog() {
    let mut engine = WasmDataFusionEngine::new();

    engine
        .open_delta_table(delta_descriptor("events"))
        .await
        .expect("Delta descriptor should register as a DataFusion table");
    engine
        .open_delta_table(delta_descriptor("metrics"))
        .await
        .expect("second Delta descriptor should register as a DataFusion table");

    assert_eq!(engine.table_names(), vec!["events", "metrics"]);
}

#[tokio::test]
async fn open_delta_table_allows_sql_planning_over_descriptor_schema() {
    let mut engine = WasmDataFusionEngine::new();

    engine
        .open_delta_table(delta_descriptor("events"))
        .await
        .expect("Delta descriptor should register as a DataFusion table");

    let (schema, batches) = engine
        .sql_to_record_batches(
            "SELECT id, event_date FROM events WHERE event_date = '2026-01-01' ORDER BY id",
        )
        .await
        .expect("DataFusion should plan SQL over the registered Delta table");

    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "event_date");
    assert!(batches.is_empty());
}

#[tokio::test]
async fn open_delta_table_maps_registration_errors_into_query_errors() {
    let mut engine = WasmDataFusionEngine::new();

    engine
        .open_delta_table(delta_descriptor("events"))
        .await
        .expect("first registration should succeed");
    let error = engine
        .open_delta_table(delta_descriptor("events"))
        .await
        .expect_err("duplicate table registration should fail");

    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert!(
        error
            .message
            .contains("experimental browser DataFusion query failed"),
        "unexpected error message: {}",
        error.message
    );
}

fn delta_descriptor(table_name: &str) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: table_name.to_string(),
        table_version: 7,
        schema: descriptor_schema(),
        partition_columns: vec!["event_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![DeltaActiveFile {
            path: "event_date=2026-01-01/part-000.parquet".to_string(),
            url: "https://example.test/table/event_date=2026-01-01/part-000.parquet".to_string(),
            size_bytes: 1024,
            partition_values: BTreeMap::from([(
                "event_date".to_string(),
                Some("2026-01-01".to_string()),
            )]),
            object_etag: Some("\"etag-1\"".to_string()),
            stats_json: Some(r#"{"numRecords":10}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("event_date", DataType::Utf8, true),
    ]))
}

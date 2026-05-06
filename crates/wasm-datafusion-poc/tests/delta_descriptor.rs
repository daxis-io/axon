use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use query_contract::PartitionColumnType;

#[test]
fn descriptor_preserves_table_schema_version_and_active_file_facts() {
    let schema = descriptor_schema();
    let descriptor = wasm_datafusion_poc::DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 7,
        schema: Arc::clone(&schema),
        partition_columns: vec!["event_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![wasm_datafusion_poc::DeltaActiveFile {
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
    };

    assert_eq!(descriptor.table_name, "events");
    assert_eq!(descriptor.table_version, 7);
    assert_eq!(descriptor.schema, schema);
    assert_eq!(descriptor.partition_columns, vec!["event_date".to_string()]);
    assert_eq!(
        descriptor.partition_column_types,
        BTreeMap::from([("event_date".to_string(), PartitionColumnType::String)])
    );

    let file = &descriptor.active_files[0];
    assert_eq!(file.path, "event_date=2026-01-01/part-000.parquet");
    assert_eq!(
        file.url,
        "https://example.test/table/event_date=2026-01-01/part-000.parquet"
    );
    assert_eq!(file.size_bytes, 1024);
    assert_eq!(
        file.partition_values,
        BTreeMap::from([("event_date".to_string(), Some("2026-01-01".to_string()))])
    );
    assert_eq!(file.object_etag.as_deref(), Some("\"etag-1\""));
    assert_eq!(file.stats_json.as_deref(), Some(r#"{"numRecords":10}"#));
    assert!(file.deletion_vector.is_none());
}

#[test]
fn descriptor_preserves_optional_deletion_vector_facts() {
    let descriptor = wasm_datafusion_poc::DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 8,
        schema: descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![wasm_datafusion_poc::DeltaActiveFile {
            path: "part-001.parquet".to_string(),
            url: "https://example.test/table/part-001.parquet".to_string(),
            size_bytes: 2048,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: None,
            deletion_vector: Some(wasm_datafusion_poc::DeletionVectorDescriptor {
                storage_type: "u".to_string(),
                path_or_inline_dv: "dv/part-001.bin".to_string(),
                offset: Some(12),
                size_in_bytes: Some(256),
                cardinality: Some(3),
            }),
        }],
    };

    let deletion_vector = descriptor.active_files[0]
        .deletion_vector
        .as_ref()
        .expect("deletion vector descriptor should be present");

    assert_eq!(deletion_vector.storage_type, "u");
    assert_eq!(deletion_vector.path_or_inline_dv, "dv/part-001.bin");
    assert_eq!(deletion_vector.offset, Some(12));
    assert_eq!(deletion_vector.size_in_bytes, Some(256));
    assert_eq!(deletion_vector.cardinality, Some(3));
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("event_date", DataType::Utf8, true),
    ]))
}

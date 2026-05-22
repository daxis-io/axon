#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{BinaryArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{ExecutionTarget, FallbackReason, QueryError, QueryErrorCode};
use wasm_datafusion_poc::{
    BrowserQueryBudget, DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine,
};

mod support;
use support::RequestCapturingServer;

#[tokio::test]
async fn query_budget_rejects_output_ipc_over_limit() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_output_ipc_bytes: Some(128),
        ..Default::default()
    });
    let batch = controlled_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("record batches should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id, value, category FROM events ORDER BY id")
        .await
        .expect_err("output over budget should fail");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired, "{error:?}");
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(error.message.contains("max_output_ipc_bytes"));
}

#[tokio::test]
async fn query_budget_rejects_output_ipc_over_limit_for_aggregate_result() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_output_ipc_bytes: Some(512),
        ..Default::default()
    });
    let batch = controlled_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("record batches should register");

    let error = engine
        .sql_to_arrow_ipc(
            "SELECT category, COUNT(*) AS event_count \
             FROM events GROUP BY category ORDER BY category",
        )
        .await
        .expect_err("aggregate output over IPC budget should fail");

    assert_browser_runtime_budget_error(&error, "max_output_ipc_bytes", "Arrow IPC output");
}

#[tokio::test]
async fn query_budget_rejects_output_ipc_over_limit_for_wide_binary_string_projection() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_output_ipc_bytes: Some(2048),
        ..Default::default()
    });
    let batch = wide_binary_string_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("wide_events", schema, vec![batch])
        .await
        .expect("wide record batch should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT payload, blob FROM wide_events ORDER BY id")
        .await
        .expect_err("wide binary/string output over IPC budget should fail");

    assert_browser_runtime_budget_error(&error, "max_output_ipc_bytes", "Arrow IPC output");
}

#[tokio::test]
async fn query_budget_rejects_planned_scan_over_limit() {
    let schema = descriptor_schema();
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_scan_bytes: Some(1024),
        ..Default::default()
    });

    engine
        .open_delta_table_with_record_batch_partitions(
            delta_descriptor("events", Arc::clone(&schema)),
            controlled_partitions(schema),
        )
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events ORDER BY id")
        .await
        .expect_err("planned scan over budget should fail");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired, "{error:?}");
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(error.message.contains("max_scan_bytes"));
    assert!(
        error.message.contains("3072"),
        "expected planned scan byte count in error: {}",
        error.message
    );
}

#[tokio::test]
async fn query_budget_rejects_row_budget_after_filter() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_rows_returned: Some(2),
        ..Default::default()
    });
    let batch = controlled_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("record batches should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events WHERE category = 'B' ORDER BY id")
        .await
        .expect_err("filtered rows over row budget should fail");

    assert_browser_runtime_budget_error(&error, "max_rows_returned", "Arrow IPC output");
}

#[tokio::test]
async fn query_budget_rejects_planned_scan_before_fetching_file() {
    let parquet_object = parquet_bytes_with_i64_columns(&[(1, 10), (2, 20), (3, 30)]);
    let object_size = u64::try_from(parquet_object.len()).expect("object size should fit");
    assert!(object_size > 1, "test parquet object should be nonempty");
    let server = RequestCapturingServer::new(parquet_object);
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_scan_bytes: Some(object_size - 1),
        ..Default::default()
    });

    engine
        .open_delta_table(single_file_parquet_descriptor(server.url(), object_size))
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events")
        .await
        .expect_err("planned scan over budget should fail before object I/O");

    assert_browser_runtime_budget_error(&error, "max_scan_bytes", "planned scan");
    assert!(
        server.recorded_requests().is_empty(),
        "plan-time scan budget should fail before fetching the object"
    );
}

#[tokio::test]
async fn query_budget_rejects_zero_batch_budget_before_scanning() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_batches_in_flight: Some(0),
        ..Default::default()
    });

    engine
        .open_delta_table(single_unreachable_parquet_descriptor())
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events")
        .await
        .expect_err("zero output batch budget should fail before object I/O");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert!(error.message.contains("max_batches_in_flight"));
}

#[tokio::test]
async fn arrow_ipc_budget_allows_one_batch_in_flight_across_multiple_output_batches() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_batches_in_flight: Some(1),
        ..Default::default()
    });
    let schema = descriptor_schema();
    let partitions = controlled_partitions(Arc::clone(&schema))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    engine
        .register_record_batches("events", schema, partitions)
        .await
        .expect("record batches should register");

    let result = engine
        .sql_to_arrow_ipc_result("SELECT id, value, category FROM events")
        .await
        .expect("streamed Arrow IPC should keep only one output batch in flight");

    assert!(!result.ipc_bytes.is_empty());
    assert_eq!(result.row_count, 5);
}

#[tokio::test]
async fn query_budget_rejects_row_budget_before_scanning_later_files() {
    let first_object = parquet_bytes_with_i64_columns(&[(1, 10), (2, 20), (3, 30)]);
    let first_object_size = u64::try_from(first_object.len()).expect("object size should fit");
    let first_server = RequestCapturingServer::new(first_object);
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_rows_returned: Some(1),
        ..Default::default()
    });

    engine
        .open_delta_table(two_file_parquet_descriptor(
            first_server.url(),
            first_object_size,
        ))
        .await
        .expect("descriptor-backed table should register");

    let error = engine
        .sql_to_arrow_ipc("SELECT id, value FROM events")
        .await
        .expect_err("row budget should fail before scanning the unreachable second file");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert!(error.message.contains("max_rows_returned"));
    assert!(
        !first_server.recorded_requests().is_empty(),
        "first file should be scanned before the row budget is exceeded"
    );
}

#[tokio::test]
async fn query_cancellation_reports_structured_shape() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget::default());
    let cancellation = engine.cancellation_token();
    let batch = controlled_batch();
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("record batches should register");
    cancellation.cancel();

    let error = engine
        .sql_to_arrow_ipc("SELECT id FROM events ORDER BY id")
        .await
        .expect_err("cancelled query should fail");

    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(error.message.contains("cancelled"));

    let result = engine
        .sql_to_arrow_ipc("SELECT id FROM events ORDER BY id")
        .await
        .expect("a cancelled query should not poison later queries");
    assert!(!result.is_empty());
}

fn assert_browser_runtime_budget_error(error: &QueryError, budget_name: &str, point: &str) {
    assert_eq!(error.code, QueryErrorCode::FallbackRequired, "{error:?}");
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
    assert!(
        error.message.contains(budget_name),
        "expected budget name {budget_name} in error: {}",
        error.message
    );
    assert!(
        error.message.contains(point),
        "expected budget point {point} in error: {}",
        error.message
    );
}

fn controlled_batch() -> RecordBatch {
    RecordBatch::try_new(
        descriptor_schema(),
        vec![
            Arc::new(Int32Array::from(vec![3, 1, 4, 2, 5])),
            Arc::new(Int32Array::from(vec![25, 5, 20, 12, 8])),
            Arc::new(StringArray::from(vec!["B", "A", "C", "B", "B"])),
        ],
    )
    .expect("controlled batch should construct")
}

fn wide_binary_string_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("blob", DataType::Binary, false),
    ]));
    let payload = "wide-string-value".repeat(256);
    let blob = vec![42_u8; 4096];
    let payloads = std::iter::repeat(payload.as_str())
        .take(4)
        .collect::<Vec<_>>();
    let blobs = std::iter::repeat(blob.as_slice())
        .take(4)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(payloads)),
            Arc::new(BinaryArray::from_iter_values(blobs)),
        ],
    )
    .expect("wide binary/string batch should construct")
}

fn controlled_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![3, 1])),
                Arc::new(Int32Array::from(vec![25, 5])),
                Arc::new(StringArray::from(vec!["B", "A"])),
            ],
        )
        .expect("first scan partition should construct")],
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![4, 2, 5])),
                Arc::new(Int32Array::from(vec![20, 12, 8])),
                Arc::new(StringArray::from(vec!["C", "B", "B"])),
            ],
        )
        .expect("second scan partition should construct")],
    ]
}

fn delta_descriptor(table_name: &str, schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: table_name.to_string(),
        table_version: 9,
        schema,
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            DeltaActiveFile {
                path: "part-000.parquet".to_string(),
                url: "https://example.test/table/part-000.parquet".to_string(),
                size_bytes: 1024,
                partition_values: BTreeMap::new(),
                object_etag: Some("\"etag-1\"".to_string()),
                stats_json: Some(r#"{"numRecords":2}"#.to_string()),
                deletion_vector: None,
            },
            DeltaActiveFile {
                path: "part-001.parquet".to_string(),
                url: "https://example.test/table/part-001.parquet".to_string(),
                size_bytes: 2048,
                partition_values: BTreeMap::new(),
                object_etag: Some("\"etag-2\"".to_string()),
                stats_json: Some(r#"{"numRecords":3}"#.to_string()),
                deletion_vector: None,
            },
        ],
    }
}

fn single_file_parquet_descriptor(url: String, size_bytes: u64) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 9,
        schema: i64_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url,
            size_bytes,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":3}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn single_unreachable_parquet_descriptor() -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 9,
        schema: i64_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:9/part-000.parquet".to_string(),
            size_bytes: 1024,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":1}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn two_file_parquet_descriptor(first_url: String, first_size_bytes: u64) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 9,
        schema: i64_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            DeltaActiveFile {
                path: "part-000.parquet".to_string(),
                url: first_url,
                size_bytes: first_size_bytes,
                partition_values: BTreeMap::new(),
                object_etag: None,
                stats_json: Some(r#"{"numRecords":3}"#.to_string()),
                deletion_vector: None,
            },
            DeltaActiveFile {
                path: "part-001.parquet".to_string(),
                url: "http://127.0.0.1:9/part-001.parquet".to_string(),
                size_bytes: 1024,
                partition_values: BTreeMap::new(),
                object_etag: None,
                stats_json: Some(r#"{"numRecords":1}"#.to_string()),
                deletion_vector: None,
            },
        ],
    }
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]))
}

fn i64_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn parquet_bytes_with_i64_columns(rows: &[(i64, i64)]) -> Vec<u8> {
    parquet_bytes_with_i64_column_row_groups(&[rows])
}

fn parquet_bytes_with_i64_column_row_groups(row_groups: &[&[(i64, i64)]]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 value; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    for rows in row_groups {
        let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let mut row_group = writer
            .next_row_group()
            .expect("row-group writer should construct");
        if let Some(mut column) = row_group
            .next_column()
            .expect("id column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(&ids, None, None)
                .expect("id values should write");
            column.close().expect("id column writer should close");
        }
        if let Some(mut column) = row_group
            .next_column()
            .expect("value column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(&values, None, None)
                .expect("value values should write");
            column.close().expect("value column writer should close");
        }
        row_group.close().expect("row-group writer should close");
    }
    writer.close().expect("file writer should close");
    bytes
}

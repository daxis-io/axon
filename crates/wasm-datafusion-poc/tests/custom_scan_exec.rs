#![cfg(not(target_arch = "wasm32"))]

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{cast::AsArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use wasm_datafusion_poc::{DeltaActiveFile, DeltaTableDescriptor, WasmDataFusionEngine};

#[tokio::test]
async fn datafusion_executes_filter_order_and_limit_above_custom_scan() {
    let mut engine = WasmDataFusionEngine::new();
    let schema = descriptor_schema();

    engine
        .open_delta_table_with_record_batch_partitions(
            delta_descriptor("events", Arc::clone(&schema)),
            controlled_partitions(Arc::clone(&schema)),
        )
        .await
        .expect("descriptor-backed table should register with in-memory scan partitions");

    let (_schema, batches) = engine
        .sql_to_record_batches(
            "SELECT id, value FROM events \
             WHERE category = 'B' AND value > 10 \
             ORDER BY id \
             LIMIT 2",
        )
        .await
        .expect("DataFusion should execute SQL above the custom scan");
    let result = single_batch(&batches);

    assert_eq!(result.schema().field(0).name(), "id");
    assert_eq!(result.schema().field(1).name(), "value");
    assert_eq!(int32_values(result, 0), vec![2, 3]);
    assert_eq!(int32_values(result, 1), vec![12, 25]);
}

#[tokio::test]
async fn datafusion_executes_aggregate_above_custom_scan() {
    let mut engine = WasmDataFusionEngine::new();
    let schema = descriptor_schema();

    engine
        .open_delta_table_with_record_batch_partitions(
            delta_descriptor("events", Arc::clone(&schema)),
            controlled_partitions(Arc::clone(&schema)),
        )
        .await
        .expect("descriptor-backed table should register with in-memory scan partitions");

    let (_schema, batches) = engine
        .sql_to_record_batches(
            "SELECT category, COUNT(*) AS rows, SUM(value) AS total_value \
             FROM events \
             GROUP BY category \
             ORDER BY category",
        )
        .await
        .expect("DataFusion should execute aggregate SQL above the custom scan");
    let result = single_batch(&batches);

    assert_eq!(utf8_values(result, 0), vec!["A", "B", "C"]);
    assert_eq!(int64_values(result, 1), vec![1, 3, 1]);
    assert_eq!(int64_values(result, 2), vec![5, 45, 20]);
}

#[tokio::test]
async fn datafusion_plan_keeps_operators_above_custom_scan() {
    let mut engine = WasmDataFusionEngine::new();
    let schema = descriptor_schema();

    engine
        .open_delta_table_with_record_batch_partitions(
            delta_descriptor("events", Arc::clone(&schema)),
            controlled_partitions(Arc::clone(&schema)),
        )
        .await
        .expect("descriptor-backed table should register with in-memory scan partitions");

    let physical_plan = explain_physical_plan_text(
        &engine,
        "SELECT id \
         FROM events \
         WHERE category = 'B' AND value > 10 \
         ORDER BY id \
         LIMIT 2",
    )
    .await;
    let filter_index = physical_plan
        .find("FilterExec")
        .expect("expected DataFusion filter above custom scan");
    let scan_index = physical_plan
        .find("AxonParquetScanExec")
        .expect("expected custom scan in physical plan");
    let sort_index = physical_plan
        .find("SortExec")
        .or_else(|| physical_plan.find("SortPreservingMergeExec"))
        .expect("expected DataFusion sort above custom scan");

    assert!(
        filter_index < scan_index,
        "expected DataFusion filter above custom scan:\n{physical_plan}"
    );
    assert!(
        sort_index < filter_index,
        "expected DataFusion sort above filter and custom scan:\n{physical_plan}"
    );
    assert!(
        physical_plan.contains("fetch=2"),
        "expected DataFusion top-k/limit fetch above custom scan:\n{physical_plan}"
    );

    let aggregate_plan = explain_physical_plan_text(
        &engine,
        "SELECT category, COUNT(*) AS rows \
         FROM events \
         GROUP BY category \
         ORDER BY category",
    )
    .await;
    let aggregate_index = aggregate_plan
        .find("AggregateExec")
        .expect("expected DataFusion aggregate above custom scan");
    let aggregate_scan_index = aggregate_plan
        .find("AxonParquetScanExec")
        .expect("expected custom scan below aggregate");

    assert!(
        aggregate_index < aggregate_scan_index,
        "expected DataFusion aggregate above custom scan:\n{aggregate_plan}"
    );
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

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]))
}

fn single_batch(batches: &[RecordBatch]) -> &RecordBatch {
    assert_eq!(batches.len(), 1, "query should return one output batch");
    &batches[0]
}

fn int32_values(batch: &RecordBatch, column_index: usize) -> Vec<i32> {
    batch
        .column(column_index)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values()
        .to_vec()
}

fn int64_values(batch: &RecordBatch, column_index: usize) -> Vec<i64> {
    batch
        .column(column_index)
        .as_primitive::<arrow_array::types::Int64Type>()
        .values()
        .to_vec()
}

fn utf8_values(batch: &RecordBatch, column_index: usize) -> Vec<&str> {
    batch
        .column(column_index)
        .as_string::<i32>()
        .iter()
        .map(|value| value.expect("test data should not contain null strings"))
        .collect()
}

async fn explain_physical_plan_text(engine: &WasmDataFusionEngine, sql: &str) -> String {
    let (_schema, batches) = engine
        .sql_to_record_batches(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN should run through DataFusion");
    let mut lines = Vec::new();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                match batch.schema().field(column_index).data_type() {
                    DataType::Utf8 => lines.push(
                        batch
                            .column(column_index)
                            .as_string::<i32>()
                            .value(row_index)
                            .to_string(),
                    ),
                    DataType::LargeUtf8 => lines.push(
                        batch
                            .column(column_index)
                            .as_string::<i64>()
                            .value(row_index)
                            .to_string(),
                    ),
                    _ => {}
                }
            }
        }
    }

    lines
        .join("\n")
        .split_once("physical_plan\n")
        .map(|(_header, physical_plan)| physical_plan.to_string())
        .expect("EXPLAIN output should include a physical plan")
}

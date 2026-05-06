#![cfg(not(target_arch = "wasm32"))]

use arrow_array::{cast::AsArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::test]
async fn datafusion_queries_registered_record_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![5, 12, 25, 20])),
            Arc::new(StringArray::from(vec!["A", "B", "B", "C"])),
        ],
    )
    .expect("batch should construct");

    let batches = wasm_datafusion_poc::query_record_batches(wasm_datafusion_poc::SMOKE_SQL, batch)
        .await
        .expect("DataFusion query should run");
    let result = batches.first().expect("query should return one batch");

    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.schema().field(0).name(), "id");
    assert_eq!(result.schema().field(1).name(), "value");
    assert_eq!(int32_values(result, 0), vec![2, 3]);
    assert_eq!(int32_values(result, 1), vec![12, 25]);
}

#[tokio::test]
async fn engine_keeps_registered_batches_available_across_queries() {
    let mut engine = wasm_datafusion_poc::WasmDataFusionEngine::new();
    let batch = wasm_datafusion_poc::synthetic_record_batch().expect("batch should construct");
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("engine should register batches");

    let (schema, batches) = engine
        .sql_to_record_batches("SELECT id FROM events WHERE category = 'B' ORDER BY id")
        .await
        .expect("engine should query registered table");
    let result = batches.first().expect("query should return one batch");

    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(int32_values(result, 0), vec![2, 3]);

    let (_schema, batches) = engine
        .sql_to_record_batches("SELECT value FROM events WHERE id = 4")
        .await
        .expect("engine should keep table registration for later queries");
    let result = batches
        .first()
        .expect("second query should return one batch");

    assert_eq!(int32_values(result, 0), vec![20]);
}

#[tokio::test]
async fn datafusion_query_summary_preserves_column_names_for_empty_results() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .expect("empty batch should construct");

    let result =
        wasm_datafusion_poc::query_record_batch("SELECT id, value FROM t ORDER BY id", batch)
            .await
            .expect("empty DataFusion query should run");

    assert_eq!(result.row_count, 0);
    assert_eq!(result.column_names, vec!["id", "value"]);
}

fn int32_values(batch: &RecordBatch, column_index: usize) -> Vec<i32> {
    batch
        .column(column_index)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values()
        .to_vec()
}

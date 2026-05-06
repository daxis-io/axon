#![cfg(not(target_arch = "wasm32"))]

use std::{io::Cursor, sync::Arc};

use arrow_array::{cast::AsArray, Int32Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};

#[tokio::test]
async fn datafusion_query_encodes_arrow_ipc_stream() {
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

    let ipc =
        wasm_datafusion_poc::query_record_batch_to_arrow_ipc(wasm_datafusion_poc::SMOKE_SQL, batch)
            .await
            .expect("DataFusion query should encode Arrow IPC");
    let decoded = decode_ipc(&ipc);

    assert_eq!(decoded.schema().field(0).name(), "id");
    assert_eq!(decoded.schema().field(1).name(), "value");
    assert_eq!(decoded.num_rows(), 2);
    assert_eq!(int32_values(&decoded, 0), vec![2, 3]);
    assert_eq!(int32_values(&decoded, 1), vec![12, 25]);
}

#[tokio::test]
async fn engine_registers_batches_and_runs_sql_to_arrow_ipc() {
    let mut engine = wasm_datafusion_poc::WasmDataFusionEngine::new();
    let batch = wasm_datafusion_poc::synthetic_record_batch().expect("batch should construct");
    let schema = batch.schema();

    engine
        .register_record_batches("events", schema, vec![batch])
        .await
        .expect("engine should register batches");

    let ipc = engine
        .sql_to_arrow_ipc("SELECT id, value FROM events WHERE category = 'B' ORDER BY id")
        .await
        .expect("engine should encode Arrow IPC");
    let decoded = decode_ipc(&ipc);

    assert_eq!(decoded.schema().field(0).name(), "id");
    assert_eq!(decoded.schema().field(1).name(), "value");
    assert_eq!(decoded.num_rows(), 2);
    assert_eq!(int32_values(&decoded, 0), vec![2, 3]);
    assert_eq!(int32_values(&decoded, 1), vec![12, 25]);
}

#[tokio::test]
async fn datafusion_query_encodes_empty_arrow_ipc_stream_with_schema() {
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

    let ipc = wasm_datafusion_poc::query_record_batch_to_arrow_ipc(
        "SELECT id, value FROM t ORDER BY id",
        batch,
    )
    .await
    .expect("empty DataFusion query should still encode Arrow IPC");
    let reader = StreamReader::try_new(Cursor::new(ipc), None).expect("ipc should decode");
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("ipc batches should decode");

    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "value");
    assert_eq!(batches.len(), 0);
}

fn decode_ipc(ipc: &[u8]) -> RecordBatch {
    let reader = StreamReader::try_new(Cursor::new(ipc), None).expect("ipc should decode");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("ipc batches should decode");

    assert_eq!(batches.len(), 1);
    batches.into_iter().next().expect("one decoded batch")
}

fn int32_values(batch: &RecordBatch, column_index: usize) -> Vec<i32> {
    batch
        .column(column_index)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values()
        .to_vec()
}

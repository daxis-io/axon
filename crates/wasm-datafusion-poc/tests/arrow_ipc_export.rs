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

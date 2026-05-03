#![cfg(target_arch = "wasm32")]

use std::io::Cursor;

use arrow_array::{cast::AsArray, RecordBatch};
use arrow_ipc::reader::StreamReader;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn datafusion_session_context_constructs_in_wasm() {
    let marker = wasm_datafusion_poc::datafusion_compile_marker();
    assert_eq!(marker.table_name, "t");
    assert!(marker.datafusion_version.starts_with("52."));
}

#[wasm_bindgen_test]
async fn datafusion_queries_record_batches_in_wasm() {
    let batch =
        wasm_datafusion_poc::synthetic_record_batch().expect("synthetic batch should construct");
    let result = wasm_datafusion_poc::query_record_batch(wasm_datafusion_poc::SMOKE_SQL, batch)
        .await
        .expect("DataFusion query should run in wasm");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id", "value"]);
}

#[wasm_bindgen_test]
async fn wasm_bindgen_export_returns_arrow_ipc_for_synthetic_query() {
    let ipc = wasm_datafusion_poc::run_datafusion_smoke_query()
        .await
        .expect("wasm export should return Arrow IPC");
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

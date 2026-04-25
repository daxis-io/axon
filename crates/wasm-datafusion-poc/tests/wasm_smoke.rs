#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn datafusion_session_context_constructs_in_wasm() {
    let marker = wasm_datafusion_poc::datafusion_compile_marker();
    assert_eq!(marker.table_name, "axon_table");
    assert!(marker.datafusion_version.starts_with("52."));
}

#[wasm_bindgen_test]
async fn datafusion_queries_record_batches_in_wasm() {
    let batch =
        wasm_datafusion_poc::synthetic_record_batch().expect("synthetic batch should construct");
    let result = wasm_datafusion_poc::query_record_batch(
        "SELECT id FROM axon_table WHERE id >= 2 ORDER BY id",
        batch,
    )
    .await
    .expect("DataFusion query should run in wasm");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id"]);
}

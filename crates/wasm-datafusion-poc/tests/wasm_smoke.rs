#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn datafusion_session_context_constructs_in_wasm() {
    let marker = wasm_datafusion_poc::datafusion_compile_marker();
    assert_eq!(marker.table_name, "axon_table");
    assert!(marker.datafusion_version.starts_with("52."));
}

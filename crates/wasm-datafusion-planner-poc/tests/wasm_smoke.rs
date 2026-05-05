#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn planner_only_smoke_runs_in_wasm() {
    let display = wasm_datafusion_planner_poc::plan_sql_to_display(
        "SELECT id FROM axon_table WHERE category = 'B' LIMIT 1",
    )
    .expect("planner-only POC export should run in wasm");
    assert!(display.contains("Limit"));
}

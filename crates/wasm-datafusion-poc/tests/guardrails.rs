use query_contract::ExecutionTarget;

#[test]
fn poc_reports_browser_wasm_target_and_experimental_status() {
    assert_eq!(
        wasm_datafusion_poc::runtime_target(),
        ExecutionTarget::BrowserWasm
    );
    assert!(wasm_datafusion_poc::is_experimental());
    assert_eq!(wasm_datafusion_poc::DEFAULT_TABLE_NAME, "axon_table");
}

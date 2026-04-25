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

#[test]
fn datafusion_session_context_constructs_on_host() {
    let marker = wasm_datafusion_poc::datafusion_compile_marker();
    assert_eq!(marker.table_name, "axon_table");
    assert!(marker.datafusion_version.starts_with("52."));
}

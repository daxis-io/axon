#![cfg(target_arch = "wasm32")]

use browser_engine_worker::{artifact_report, worker_target};
use query_contract::{BrowserAccessMode, ExecutionTarget};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn worker_reports_cold_start_and_memory_baseline_in_wasm() {
    let report = artifact_report().expect("worker artifact report should be produced in wasm");

    println!(
        "{}",
        serde_json::to_string(&report).expect("worker artifact report should serialize in wasm")
    );
    assert_eq!(worker_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(report.startup.target, ExecutionTarget::BrowserWasm);
    assert_eq!(
        report.startup.access_mode,
        BrowserAccessMode::BrowserSafeHttp
    );
    assert!(report.startup.request_envelope_bytes > 0);
    assert!(report.startup.error_envelope_bytes > 0);
    assert!(report.memory.runtime_session_bytes > 0);
    assert!(report.memory.request_envelope_json_bytes > 0);
}

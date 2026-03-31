use browser_engine_worker::{
    artifact_report, cold_start_report, memory_baseline_report, worker_target,
};
use query_contract::{BrowserAccessMode, ExecutionTarget};

#[test]
fn worker_artifact_reports_browser_safe_defaults() {
    let report = cold_start_report().expect("worker startup report should be produced");

    assert_eq!(worker_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(report.target, ExecutionTarget::BrowserWasm);
    assert_eq!(report.access_mode, BrowserAccessMode::BrowserSafeHttp);
    assert!(report.request_envelope_bytes > 0);
    assert!(report.error_envelope_bytes > 0);
    assert!(report.request_timeout_ms > 0);
    assert!(report.execution_timeout_ms > 0);
    assert!(report.snapshot_preflight_timeout_ms > 0);
    assert!(report.snapshot_preflight_max_concurrency > 0);
}

#[test]
fn report_worker_memory_baseline() {
    let report = memory_baseline_report().expect("worker memory report should be produced");

    println!(
        "{}",
        serde_json::to_string(&report).expect("worker memory report should serialize")
    );
    assert!(report.runtime_session_bytes > 0);
    assert!(report.runtime_config_bytes > 0);
    assert!(report.request_envelope_struct_bytes > 0);
    assert!(report.response_envelope_struct_bytes > 0);
    assert!(report.query_metrics_struct_bytes > 0);
    assert!(report.request_envelope_json_bytes > 0);
    assert!(report.error_envelope_json_bytes > 0);
}

#[test]
fn report_worker_artifact_baseline() {
    let report = artifact_report().expect("combined artifact report should be produced");

    println!(
        "{}",
        serde_json::to_string(&report).expect("combined artifact report should serialize")
    );
    assert!(report.startup.request_envelope_bytes > 0);
    assert!(report.memory.runtime_session_bytes > 0);
}

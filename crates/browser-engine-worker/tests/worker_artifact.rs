use std::path::PathBuf;

use browser_engine_worker::{
    artifact_report, capabilities, cold_start_report, memory_baseline_report, worker_target,
    BrowserResultTransport, BrowserRuntimeSku,
};
use query_contract::{BrowserAccessMode, ExecutionTarget};

fn example_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/program/browser-lakehouse-release-handoff-examples")
        .join(file_name)
}

fn read_example(file_name: &str) -> String {
    std::fs::read_to_string(example_path(file_name))
        .unwrap_or_else(|error| panic!("failed to read example '{}': {error}", file_name))
}

#[test]
fn worker_artifact_reports_browser_safe_defaults() {
    let report = cold_start_report().expect("worker startup report should be produced");

    assert_eq!(worker_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(report.target, ExecutionTarget::BrowserWasm);
    assert_eq!(report.access_mode, BrowserAccessMode::BrowserSafeHttp);
    assert!(report.command_envelope_bytes > 0);
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
    assert!(report.query_session_bytes > 0);
    assert!(report.runtime_config_bytes > 0);
    assert!(report.command_struct_bytes > 0);
    assert!(report.response_envelope_struct_bytes > 0);
    assert!(report.query_metrics_struct_bytes > 0);
    assert!(report.command_envelope_json_bytes > 0);
    assert!(report.error_envelope_json_bytes > 0);
}

#[test]
fn report_worker_artifact_baseline() {
    let report = artifact_report().expect("combined artifact report should be produced");

    println!(
        "{}",
        serde_json::to_string(&report).expect("combined artifact report should serialize")
    );
    assert!(report.startup.command_envelope_bytes > 0);
    assert!(report.memory.runtime_session_bytes > 0);
    assert!(report.memory.query_session_bytes > 0);
}

#[test]
fn worker_artifact_reports_narrow_default_capability() {
    let report = artifact_report().expect("combined artifact report should be produced");

    assert_eq!(report.runtime_sku, BrowserRuntimeSku::Narrow);
    assert_eq!(report.result_transport, BrowserResultTransport::ArrowIpc);
    assert_eq!(report.capabilities, capabilities());
    assert!(report.capabilities.session_shell);
    assert!(!report.capabilities.browser_datafusion);
    assert_eq!(report.identity.package_name, "browser-engine-worker");
    assert_eq!(report.identity.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(report.identity.wasm_artifact, "browser_engine_worker.wasm");
}

#[test]
fn worker_artifact_example_matches_contract() {
    let report: browser_engine_worker::BrowserWorkerArtifactReport =
        serde_json::from_str(&read_example("browser-worker-artifact-report.narrow.json"))
            .expect("worker artifact example should deserialize");
    let generated = artifact_report().expect("combined artifact report should be produced");

    assert_eq!(report.runtime_sku, BrowserRuntimeSku::Narrow);
    assert_eq!(report.result_transport, BrowserResultTransport::ArrowIpc);
    assert!(report.capabilities.session_shell);
    assert!(!report.capabilities.browser_datafusion);
    assert_eq!(report.identity.package_name, "browser-engine-worker");
    assert_eq!(report.identity.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(report.identity.wasm_artifact, "browser_engine_worker.wasm");
    assert_eq!(report.startup.target, ExecutionTarget::BrowserWasm);
    assert_eq!(
        report.startup.access_mode,
        BrowserAccessMode::BrowserSafeHttp
    );
    assert_eq!(
        report.startup.command_envelope_bytes,
        generated.startup.command_envelope_bytes
    );
    assert_eq!(
        report.startup.error_envelope_bytes,
        generated.startup.error_envelope_bytes
    );
    assert_eq!(
        report.startup.request_timeout_ms,
        generated.startup.request_timeout_ms
    );
    assert_eq!(
        report.startup.execution_timeout_ms,
        generated.startup.execution_timeout_ms
    );
    assert_eq!(
        report.startup.snapshot_preflight_timeout_ms,
        generated.startup.snapshot_preflight_timeout_ms
    );
    assert_eq!(
        report.startup.snapshot_preflight_max_concurrency,
        generated.startup.snapshot_preflight_max_concurrency
    );
    assert_eq!(
        report.startup.datafusion_query_budget,
        generated.startup.datafusion_query_budget
    );
    assert_eq!(report.memory, generated.memory);
}

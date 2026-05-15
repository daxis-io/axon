#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;

#[cfg(target_arch = "wasm32")]
use browser_engine_worker::{
    artifact_report, capabilities, runtime_sku, worker_target, BrowserResultTransport,
};
#[cfg(not(target_arch = "wasm32"))]
use browser_engine_worker::{BrowserWorker, DEFAULT_SESSION_CACHE_BYTES};
#[cfg(not(target_arch = "wasm32"))]
use browser_sdk::{ArrowIpcFormat, BrowserWorkerCommand, BrowserWorkerResponseEnvelope};
#[cfg(target_arch = "wasm32")]
use query_contract::{BrowserAccessMode, ExecutionTarget};
#[cfg(not(target_arch = "wasm32"))]
use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, CapabilityReport, ExecutionTarget,
    QueryErrorCode, QueryRequest,
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(not(target_arch = "wasm32"))]
use wasm_query_runtime::BrowserRuntimeConfig;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn worker_reports_cold_start_and_memory_baseline_in_wasm() {
    let report = artifact_report().expect("worker artifact report should be produced in wasm");

    println!(
        "{}",
        serde_json::to_string(&report).expect("worker artifact report should serialize in wasm")
    );
    assert_eq!(worker_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(report.startup.target, ExecutionTarget::BrowserWasm);
    assert_eq!(report.runtime_sku, runtime_sku());
    assert_eq!(report.result_transport, BrowserResultTransport::ArrowIpc);
    assert_eq!(report.capabilities, capabilities());
    assert_eq!(
        report.capabilities.browser_datafusion,
        cfg!(feature = "datafusion")
    );
    assert_eq!(
        report.startup.access_mode,
        BrowserAccessMode::BrowserSafeHttp
    );
    assert_eq!(report.identity.package_name, "browser-engine-worker");
    assert_eq!(report.identity.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(report.identity.wasm_artifact, "browser_engine_worker.wasm");
    assert!(report.startup.command_envelope_bytes > 0);
    assert!(report.startup.error_envelope_bytes > 0);
    assert!(report.memory.runtime_session_bytes > 0);
    assert!(report.memory.query_session_bytes > 0);
    assert!(report.memory.command_envelope_json_bytes > 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_rejects_sql_before_open_delta_table() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");

    let response = test_runtime().block_on(worker.handle_command(BrowserWorkerCommand::sql(
        "req-sql-before-open",
        "events",
        QueryRequest::new(
            "gs://axon-fixtures/empty",
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        ),
    )));

    match response {
        BrowserWorkerResponseEnvelope::Error(error) => {
            assert_eq!(error.request_id, "req-sql-before-open");
            assert_eq!(error.error.code, QueryErrorCode::InvalidRequest);
            assert!(error.error.message.contains("open table named 'events'"));
        }
        other => panic!("expected SQL-before-open to fail, got {other:?}"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn worker_opens_delta_table_and_returns_sql_arrow_ipc_stream() {
    let mut worker =
        BrowserWorker::new(BrowserRuntimeConfig::default(), DEFAULT_SESSION_CACHE_BYTES)
            .expect("worker should construct");
    let descriptor = empty_delta_descriptor();

    let opened = test_runtime().block_on(worker.handle_command(
        BrowserWorkerCommand::open_delta_table("req-open-delta", "events", descriptor.clone()),
    ));
    assert_eq!(
        opened.opened_envelope().expect("opened response").name,
        "events"
    );

    let sql = if cfg!(feature = "datafusion") {
        "SELECT COUNT(*) AS rows FROM events"
    } else {
        "SELECT COUNT(*) AS rows FROM axon_table"
    };
    let response = test_runtime().block_on(worker.handle_command(BrowserWorkerCommand::sql(
        "req-sql-ipc",
        "events",
        QueryRequest::new(descriptor.table_uri, sql, ExecutionTarget::BrowserWasm),
    )));

    let success = response
        .success_envelope()
        .expect("SQL should return a success envelope");
    assert_eq!(success.request_id, "req-sql-ipc");
    assert_eq!(success.result.format, ArrowIpcFormat::Stream);
    assert_eq!(
        success.result.content_type,
        "application/vnd.apache.arrow.stream"
    );
    assert!(!success.result.bytes.is_empty());
    assert_eq!(success.response.metrics.rows_emitted, 0);
}

#[cfg(not(target_arch = "wasm32"))]
fn empty_delta_descriptor() -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/empty".to_string(),
        snapshot_version: 1,
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        active_files: Vec::<BrowserHttpFileDescriptor>::new(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("test runtime should construct")
}

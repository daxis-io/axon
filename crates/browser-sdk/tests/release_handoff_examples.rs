use std::path::PathBuf;

use browser_sdk::{ArrowIpcFormat, BrowserWorkerCommand, BrowserWorkerResponseEnvelope};
use query_contract::{CapabilityKey, CapabilityState, ExecutionTarget, FallbackReason};

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
fn browser_worker_open_table_example_matches_contract() {
    let command: BrowserWorkerCommand =
        serde_json::from_str(&read_example("browser-worker-command.open-table.json"))
            .expect("browser worker open_table example should deserialize");

    match command {
        BrowserWorkerCommand::OpenTable(command) => {
            assert_eq!(command.request_id, "req-open-001");
            assert_eq!(command.name, "supported_table");
            assert_eq!(command.snapshot.snapshot_version, 7);
        }
        _ => panic!("expected open_table command example"),
    }
}

#[test]
fn browser_worker_open_parquet_dataset_example_matches_contract() {
    let command: BrowserWorkerCommand = serde_json::from_str(&read_example(
        "browser-worker-command.open-parquet-dataset.json",
    ))
    .expect("browser worker open_parquet_dataset example should deserialize");

    match command {
        BrowserWorkerCommand::OpenParquetDataset(command) => {
            assert_eq!(command.request_id, "req-open-parquet-001");
            assert_eq!(command.name, "plain_parquet_events");
            assert_eq!(
                command.dataset.table_uri,
                "https://storage.example.invalid/datasets/plain-parquet-events"
            );
            assert_eq!(command.dataset.files.len(), 1);
        }
        _ => panic!("expected open_parquet_dataset command example"),
    }
}

#[test]
fn browser_worker_sql_example_matches_contract() {
    let command: BrowserWorkerCommand =
        serde_json::from_str(&read_example("browser-worker-request.supported.json"))
            .expect("browser worker sql example should deserialize");

    match command {
        BrowserWorkerCommand::Sql(command) => {
            assert_eq!(command.request_id, "req-supported-001");
            assert_eq!(command.name, "supported_table");
            assert_eq!(
                command.request.preferred_target,
                ExecutionTarget::BrowserWasm
            );
        }
        _ => panic!("expected sql command example"),
    }
}

#[test]
fn browser_worker_dispose_example_matches_contract() {
    let command: BrowserWorkerCommand =
        serde_json::from_str(&read_example("browser-worker-command.dispose.json"))
            .expect("browser worker dispose example should deserialize");

    match command {
        BrowserWorkerCommand::Dispose(command) => {
            assert_eq!(command.request_id, "req-dispose-001");
            assert_eq!(command.name, "supported_table");
        }
        _ => panic!("expected dispose command example"),
    }
}

#[test]
fn browser_worker_opened_response_example_matches_contract() {
    let response: BrowserWorkerResponseEnvelope =
        serde_json::from_str(&read_example("browser-worker-response.opened.json"))
            .expect("browser worker opened example should deserialize");

    assert_eq!(
        response
            .opened_envelope()
            .expect("opened envelope should be present")
            .name,
        "supported_table"
    );
}

#[test]
fn browser_worker_success_response_example_matches_contract() {
    let response: BrowserWorkerResponseEnvelope =
        serde_json::from_str(&read_example("browser-worker-response.supported.json"))
            .expect("browser worker success example should deserialize");

    let success = response
        .success_envelope()
        .expect("success envelope should be present");
    assert_eq!(success.request_id, "req-supported-001");
    assert_eq!(success.response.executed_on, ExecutionTarget::BrowserWasm);
    assert_eq!(success.response.metrics.files_touched, 2);
    assert_eq!(success.response.metrics.files_skipped, 1);
    assert_eq!(success.response.metrics.row_groups_touched, 2);
    assert_eq!(success.response.metrics.row_groups_skipped, 1);
    assert_eq!(success.response.metrics.footer_reads, Some(2));
    assert_eq!(success.response.metrics.rows_emitted, 3);
    assert_eq!(success.result.format, ArrowIpcFormat::Stream);
    assert_eq!(response.fallback_reason(), None);
}

#[test]
fn browser_worker_native_fallback_response_example_matches_contract() {
    let response: BrowserWorkerResponseEnvelope = serde_json::from_str(&read_example(
        "browser-worker-response.native-fallback.json",
    ))
    .expect("browser worker fallback example should deserialize");

    let success = response
        .success_envelope()
        .expect("success envelope should be present");
    assert_eq!(success.response.executed_on, ExecutionTarget::Native);
    assert_eq!(success.response.metrics.files_touched, 3);
    assert_eq!(success.response.metrics.files_skipped, 2);
    assert_eq!(success.response.metrics.row_groups_touched, 0);
    assert_eq!(success.response.metrics.row_groups_skipped, 0);
    assert_eq!(success.response.metrics.footer_reads, None);
    assert_eq!(success.response.metrics.rows_emitted, 0);
    assert_eq!(
        response.fallback_reason(),
        Some(&FallbackReason::CapabilityGate {
            capability: CapabilityKey::MultiPartitionExecution,
            required_state: CapabilityState::NativeOnly,
        })
    );
}

#[test]
fn browser_worker_disposed_response_example_matches_contract() {
    let response: BrowserWorkerResponseEnvelope =
        serde_json::from_str(&read_example("browser-worker-response.disposed.json"))
            .expect("browser worker disposed example should deserialize");

    assert_eq!(
        response
            .disposed_envelope()
            .expect("disposed envelope should be present")
            .name,
        "supported_table"
    );
}

#[test]
fn browser_worker_hard_fail_response_example_matches_contract() {
    let response: BrowserWorkerResponseEnvelope =
        serde_json::from_str(&read_example("browser-worker-response.hard-fail.json"))
            .expect("browser worker hard-fail example should deserialize");

    assert!(response.success_envelope().is_none());
    assert_eq!(response.fallback_reason(), None);
}

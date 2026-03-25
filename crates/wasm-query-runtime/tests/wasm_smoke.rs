#![cfg(target_arch = "wasm32")]

use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryErrorCode,
};
use wasm_bindgen_test::wasm_bindgen_test;
use wasm_query_runtime::{
    runtime_target, BrowserObjectSource, BrowserRuntimeConfig, BrowserRuntimeSession,
};

#[wasm_bindgen_test]
fn browser_runtime_session_and_object_source_construct_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported in wasm");

    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(session.config(), &BrowserRuntimeConfig::default());
    assert_eq!(source.url(), "https://example.com/object");
}

#[wasm_bindgen_test]
fn plain_http_sources_are_rejected_in_wasm() {
    let error = BrowserObjectSource::from_url("http://127.0.0.1:8080/object")
        .expect_err("plain HTTP should be rejected in wasm");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[wasm_bindgen_test]
fn parquet_footer_api_surface_constructs_a_future_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported in wasm");

    let footer_read = session.read_parquet_footer(&source);
    drop(footer_read);
}

#[wasm_bindgen_test]
fn browser_runtime_materializes_https_descriptors_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 4,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://example.com/object".to_string(),
            size_bytes: 128,
            partition_values: std::collections::BTreeMap::new(),
        }],
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("https descriptors should materialize in wasm");

    assert_eq!(materialized.table_uri(), descriptor.table_uri);
    assert_eq!(materialized.snapshot_version(), descriptor.snapshot_version);
    assert_eq!(materialized.active_files().len(), 1);
    assert_eq!(
        materialized.active_files()[0].object_source().url(),
        descriptor.active_files[0].url
    );
}

#[wasm_bindgen_test]
fn materialize_snapshot_rejects_loopback_http_in_wasm() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported in wasm");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 4,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:8080/object".to_string(),
            size_bytes: 128,
            partition_values: std::collections::BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("loopback HTTP should be rejected in wasm");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

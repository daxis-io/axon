#![cfg(target_arch = "wasm32")]

use query_contract::{ExecutionTarget, QueryErrorCode};
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

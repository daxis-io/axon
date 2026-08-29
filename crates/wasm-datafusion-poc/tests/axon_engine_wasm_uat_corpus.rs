#![cfg(target_arch = "wasm32")]

//! wasm32 runner for the Axon engine UAT query corpus.
//!
//! This executes the *same* corpus as the host runner, from the same JSON file,
//! against the same fixtures, through the same two execution paths. It was
//! previously four hand-written cases against a narrower schema, which meant a
//! green wasm probe proved far less than its name suggested; the shared module
//! in `support/uat.rs` now makes that divergence impossible by construction.

use wasm_bindgen_test::wasm_bindgen_test;

#[path = "support/uat.rs"]
mod uat;

use uat::{assert_suite_coverage, corpus_execution_paths, execute_uat_case, load_corpus};

#[wasm_bindgen_test]
async fn actual_wasm_engine_executes_full_uat_query_corpus() {
    let corpus = load_corpus();
    assert_suite_coverage(&corpus);

    let execution_paths = corpus_execution_paths();
    assert!(
        execution_paths
            .iter()
            .any(|path| path.uses_axon_scan_exec()),
        "actual wasm UAT run must prove the descriptor-backed AxonParquetScanExec path"
    );

    for case in &corpus {
        for path in execution_paths {
            execute_uat_case("actual wasm", path, case).await;
        }
    }
}

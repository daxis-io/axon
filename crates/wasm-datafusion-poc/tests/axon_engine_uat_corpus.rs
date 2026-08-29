#![cfg(not(target_arch = "wasm32"))]

//! Host-target runner for the Axon engine UAT query corpus.
//!
//! The corpus, the fixtures, and the comparison rules live in `support/uat.rs`
//! and are shared verbatim with the wasm32 runner, so the two targets cannot
//! drift apart. This file supplies only the host test entry point.

#[path = "support/uat.rs"]
mod uat;

use uat::{assert_suite_coverage, corpus_execution_paths, execute_uat_case, load_corpus};

#[tokio::test]
async fn axon_engine_executes_full_uat_query_corpus() {
    let corpus = load_corpus();
    assert_suite_coverage(&corpus);

    let execution_paths = corpus_execution_paths();
    assert!(
        execution_paths
            .iter()
            .any(|path| path.uses_axon_scan_exec()),
        "UAT corpus should run through the descriptor-backed AxonParquetScanExec path"
    );

    for case in &corpus {
        for path in execution_paths {
            execute_uat_case("host", path, case).await;
        }
    }
}

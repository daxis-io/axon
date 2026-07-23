#![cfg(not(target_arch = "wasm32"))]

use serde_json::Value;
use wasm_datafusion_poc::BrowserQueryBudget;

const DAXIS_DEFAULT_WORKER_BROTLI_BUDGET_BYTES: u64 = 6 * 1024 * 1024;
const DAXIS_DEFAULT_WORKER_SIZE_COMMAND: &str = "AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh";

#[test]
fn daxis_browser_datafusion_budget_profile_is_release_gate_ready() {
    let profile: Value = serde_json::from_str(include_str!(
        "../../../docs/release-gates/daxis-browser-datafusion-budget-profile.json"
    ))
    .expect("Daxis browser DataFusion budget profile should parse as JSON");

    assert_eq!(profile["profile"], "daxis_browser_datafusion_m3");
    assert_eq!(profile["runtimeSku"], "browser_datafusion");
    assert_eq!(profile["defaultWorkerSku"], true);
    assert_eq!(
        profile["workerArtifactReport"],
        "docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json"
    );
    assert_eq!(profile["owner"], "Runtime / engine team");

    let query = &profile["queryBudget"];
    let budget = BrowserQueryBudget {
        max_scan_bytes: Some(unsigned(query, "maxScanBytes")),
        max_scan_overfetch_bytes: Some(unsigned(query, "maxScanOverfetchBytes")),
        max_output_ipc_bytes: Some(unsigned(query, "maxOutputIpcBytes")),
        max_batches_in_flight: Some(usize_unsigned(query, "maxBatchesInFlight")),
        max_rows_returned: Some(unsigned(query, "maxRowsReturned")),
    };

    assert!(
        budget.max_scan_bytes.unwrap() <= 64 * 1024 * 1024,
        "M3 Daxis scan budget should stay inside the documented interactive envelope"
    );
    assert_eq!(
        budget.max_scan_overfetch_bytes,
        Some(1024 * 1024),
        "Daxis should bound speculative scan overfetch while admitting the 512 KiB readahead cap"
    );
    assert!(
        budget.max_output_ipc_bytes.unwrap() <= 16 * 1024 * 1024,
        "M3 Daxis output budget should stay inside the documented interactive envelope"
    );
    assert!(
        budget.max_rows_returned.unwrap() <= 100_000,
        "M3 Daxis row budget should stay inside the documented interactive envelope"
    );
    assert_eq!(
        budget.max_batches_in_flight,
        Some(1),
        "browser DataFusion should keep one output batch in flight for M3"
    );

    assert_eq!(
        unsigned(&profile["artifactBudget"], "brotliBudgetBytes"),
        DAXIS_DEFAULT_WORKER_BROTLI_BUDGET_BYTES
    );
    assert!(DAXIS_DEFAULT_WORKER_SIZE_COMMAND.contains(&format!(
        "AXON_DF_BROTLI_BUDGET_BYTES={DAXIS_DEFAULT_WORKER_BROTLI_BUDGET_BYTES}"
    )));
    assert_eq!(
        profile["artifactBudget"]["reportCommand"],
        DAXIS_DEFAULT_WORKER_SIZE_COMMAND
    );
    assert!(unsigned(&profile["startupBudget"], "firstRealDeltaParquetQueryMs") > 0);
    assert!(unsigned(&profile["memoryBudget"], "sessionStructBytes") > 0);

    let commands = profile["verificationCommands"]
        .as_array()
        .expect("verificationCommands should be an array")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("verification commands should be strings")
        })
        .collect::<Vec<_>>();

    for expected in [
        "cargo test -p wasm-datafusion-poc --test query_budgets",
        "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
        DAXIS_DEFAULT_WORKER_SIZE_COMMAND,
        "bash tests/perf/browser_datafusion_engine_smoke.sh",
    ] {
        assert!(
            commands.contains(&expected),
            "Daxis DataFusion budget profile should name verification command: {expected}"
        );
    }
}

fn unsigned(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("{field} should be an unsigned integer"))
}

fn usize_unsigned(value: &Value, field: &str) -> usize {
    let parsed = unsigned(value, field);
    usize::try_from(parsed).unwrap_or_else(|_| panic!("{field} should fit usize"))
}

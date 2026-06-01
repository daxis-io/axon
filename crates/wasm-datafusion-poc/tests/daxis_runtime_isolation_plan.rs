#![cfg(not(target_arch = "wasm32"))]

use serde_json::Value;

#[test]
fn daxis_runtime_isolation_plan_keeps_legacy_and_datafusion_boundaries_explicit() {
    let plan: Value = serde_json::from_str(include_str!(
        "../../../docs/release-gates/daxis-browser-runtime-isolation-plan.json"
    ))
    .expect("Daxis browser runtime isolation plan should parse as JSON");

    assert_eq!(plan["plan"], "daxis_browser_runtime_isolation_m3");
    assert_eq!(plan["daxisTargetRuntimeSku"], "browser_datafusion");
    assert_eq!(plan["currentDaxisWorkerSku"], "browser_datafusion");
    assert_eq!(plan["legacyCompatibilityWorkerSku"], "narrow_session");
    assert_eq!(plan["datafusionDefaultWorkerSku"], true);
    assert_eq!(
        plan["defaultWorkerArtifactReport"],
        "docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json"
    );

    let boundaries = plan["runtimeBoundaries"]
        .as_array()
        .expect("runtimeBoundaries should be an array");
    assert_eq!(
        boundaries.len(),
        2,
        "M3 should document exactly the legacy narrow boundary and the Daxis DataFusion default boundary"
    );

    let legacy = find_boundary(boundaries, "legacy_narrow_worker");
    assert_eq!(legacy["crate"], "browser-engine-worker");
    assert_eq!(legacy["routingStatus"], "compatibility_scaffold");
    assert_eq!(legacy["defaultWorkerSku"], false);
    assert_eq!(
        string_array(legacy, "mustNotDependOn"),
        ["wasm-datafusion-poc", "datafusion"]
    );
    assert_eq!(
        string_array(legacy, "verificationCommands"),
        ["bash tests/conformance/verify_browser_worker_dependency_boundary.sh"]
    );

    let datafusion = find_boundary(boundaries, "daxis_datafusion_default");
    assert_eq!(datafusion["crate"], "axon-web-wasm");
    assert_eq!(datafusion["routingStatus"], "daxis_default");
    assert_eq!(datafusion["defaultWorkerSku"], true);
    assert_eq!(
        datafusion["artifactReport"],
        "docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json"
    );
    assert_eq!(
        string_array(datafusion, "mustDependOn"),
        [
            "wasm-datafusion-session",
            "wasm-datafusion-poc",
            "datafusion"
        ]
    );
    assert_eq!(
        string_array(datafusion, "mustNotDependOn"),
        ["wasm-query-session"]
    );
    assert_eq!(
        string_array(datafusion, "verificationCommands"),
        ["bash tests/conformance/verify_axon_web_datafusion_runtime.sh"]
    );

    let phases = plan["removalPlan"]
        .as_array()
        .expect("removalPlan should be an array");
    assert_eq!(
        phases
            .iter()
            .map(|phase| phase["phase"].as_str().expect("phase should be a string"))
            .collect::<Vec<_>>(),
        ["integration", "candidate", "stable"]
    );

    let verification_commands = string_array(&plan, "verificationCommands");
    for expected in [
        "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
        "bash tests/conformance/verify_axon_web_datafusion_runtime.sh",
        "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
        "cargo test -p wasm-datafusion-poc --test daxis_budget_profile",
    ] {
        assert!(
            verification_commands.contains(&expected),
            "Daxis runtime isolation plan should require verification command: {expected}"
        );
    }
}

fn find_boundary<'a>(boundaries: &'a [Value], name: &str) -> &'a Value {
    boundaries
        .iter()
        .find(|boundary| boundary["name"] == name)
        .unwrap_or_else(|| panic!("missing runtime boundary: {name}"))
}

fn string_array<'a>(value: &'a Value, field: &str) -> Vec<&'a str> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{field} should be an array"))
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .unwrap_or_else(|| panic!("{field} entries should be strings"))
        })
        .collect()
}

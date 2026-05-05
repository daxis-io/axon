#[test]
fn planner_poc_is_experimental_and_uses_axon_table() {
    assert!(wasm_datafusion_planner_poc::is_experimental());
    assert_eq!(
        wasm_datafusion_planner_poc::DEFAULT_TABLE_NAME,
        "axon_table"
    );
}

#[test]
fn planner_poc_manifest_does_not_depend_on_umbrella_datafusion() {
    let manifest = include_str!("../Cargo.toml");

    assert!(
        !manifest_has_direct_umbrella_datafusion_dependency(manifest),
        "planner-only POC must use direct DataFusion subcrates, not the umbrella datafusion crate"
    );
}

fn manifest_has_direct_umbrella_datafusion_dependency(manifest: &str) -> bool {
    manifest.lines().any(|line| {
        let Some(line) = line.split('#').next() else {
            return false;
        };
        let compact: String = line.chars().filter(|ch| !ch.is_whitespace()).collect();

        compact.starts_with("datafusion=") || compact.starts_with("[dependencies.datafusion]")
    })
}

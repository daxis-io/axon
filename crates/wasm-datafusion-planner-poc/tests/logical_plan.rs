#[test]
fn plans_projection_filter_and_limit_without_umbrella_datafusion() {
    let summary = wasm_datafusion_planner_poc::plan_sql_summary(
        "SELECT id, value FROM axon_table WHERE value > 10 LIMIT 2",
    )
    .expect("planner-only POC should produce a logical plan");

    assert!(summary.display.contains("Projection"));
    assert!(summary.display.contains("Filter"));
    assert!(summary.display.contains("Limit"));
    assert_eq!(
        summary.sql,
        "SELECT id, value FROM axon_table WHERE value > 10 LIMIT 2"
    );
}

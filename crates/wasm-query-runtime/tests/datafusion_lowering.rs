#![cfg(feature = "datafusion-planner-poc")]

#[test]
fn lowers_projection_filter_and_limit_summary() {
    let summary = wasm_query_runtime::datafusion_planner_poc::lower_sql_to_axon_summary(
        "SELECT id, value FROM axon_table WHERE value > 10 LIMIT 2",
    )
    .expect("supported SQL should lower");

    assert_eq!(summary.required_columns, vec!["id", "value"]);
    assert_eq!(summary.limit, Some(2));
    assert_eq!(summary.filter_columns, vec!["value"]);
}

#[test]
fn rejects_join_before_runtime_planning() {
    let error = wasm_query_runtime::datafusion_planner_poc::lower_sql_to_axon_summary(
        "SELECT a.id FROM axon_table a JOIN other b ON a.id = b.id",
    )
    .expect_err("joins stay outside browser V1");

    assert!(error.to_string().contains("not supported"));
}

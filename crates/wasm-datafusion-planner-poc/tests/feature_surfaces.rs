#[test]
#[cfg(feature = "optimizer")]
fn optimizer_surface_is_explicit() {
    assert!(wasm_datafusion_planner_poc::optimizer_surface_marker() > 0);
}

#[test]
#[cfg(feature = "physical-expr-floor")]
fn physical_expr_surface_is_explicit() {
    assert!(wasm_datafusion_planner_poc::physical_expr_surface_marker().contains("id"));
}

#[test]
#[cfg(feature = "physical-plan-floor")]
fn physical_plan_surface_is_explicit() {
    assert!(wasm_datafusion_planner_poc::physical_plan_surface_marker().contains("Placeholder"));
}

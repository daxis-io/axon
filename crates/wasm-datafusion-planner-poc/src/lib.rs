use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result, TableReference};
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::{ContextProvider, SqlToRel};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

pub const OWNER: &str = "browser-engine";
pub const RESPONSIBILITY: &str =
    "Experimental planner-only DataFusion proof of concept for browser SQL planning.";
pub const DEFAULT_TABLE_NAME: &str = "axon_table";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalPlanSummary {
    pub sql: String,
    pub display: String,
}

pub fn is_experimental() -> bool {
    true
}

#[cfg(feature = "optimizer")]
pub fn optimizer_surface_marker() -> usize {
    datafusion_optimizer::optimizer::Optimizer::new().rules.len()
}

#[cfg(feature = "physical-expr-floor")]
pub fn physical_expr_surface_marker() -> String {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let expr = datafusion_physical_expr::expressions::col("id", &schema)
        .expect("physical expr marker should build");
    format!("{expr:?}")
}

#[cfg(feature = "physical-plan-floor")]
pub fn physical_plan_surface_marker() -> String {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let exec = datafusion_physical_plan::placeholder_row::PlaceholderRowExec::new(schema);
    format!("{exec:?}")
}

pub fn plan_sql_summary(sql: &str) -> Result<LogicalPlanSummary> {
    let mut statements = DFParser::parse_sql(sql)?;
    let Some(statement) = statements.pop_front() else {
        return plan_err!("Expected one SQL statement, found none");
    };
    if !statements.is_empty() {
        return plan_err!("Expected one SQL statement, found more than one");
    }

    let context = AxonPlannerContext::new();
    let planner = SqlToRel::new(&context);
    let plan = planner.statement_to_plan(statement)?;

    Ok(LogicalPlanSummary {
        sql: sql.to_string(),
        display: format!("{plan}"),
    })
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn plan_sql_to_display(sql: &str) -> std::result::Result<String, JsValue> {
    plan_sql_summary(sql)
        .map(|summary| summary.display)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[cfg(all(target_arch = "wasm32", feature = "optimizer"))]
#[wasm_bindgen]
pub fn optimizer_surface_marker_wasm() -> usize {
    optimizer_surface_marker()
}

#[cfg(all(target_arch = "wasm32", feature = "physical-expr-floor"))]
#[wasm_bindgen]
pub fn physical_expr_surface_marker_wasm() -> String {
    physical_expr_surface_marker()
}

#[cfg(all(target_arch = "wasm32", feature = "physical-plan-floor"))]
#[wasm_bindgen]
pub fn physical_plan_surface_marker_wasm() -> String {
    physical_plan_surface_marker()
}

struct AxonPlannerContext {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl AxonPlannerContext {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            DEFAULT_TABLE_NAME.to_string(),
            Arc::new(LogicalTableSource::new(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
                Field::new("category", DataType::Utf8, false),
            ])))) as Arc<dyn TableSource>,
        );

        Self {
            options: ConfigOptions::default(),
            tables,
            expr_planners: Vec::new(),
        }
    }
}

impl ContextProvider for AxonPlannerContext {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(Arc::clone(table)),
            None => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

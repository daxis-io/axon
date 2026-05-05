use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result, ScalarValue, TableReference};
use datafusion_expr::expr::Alias;
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{
    AggregateUDF, Expr, LogicalPlan, Operator, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_sql::parser::{DFParser, Statement as DataFusionStatement};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::{
    Query as SqlQuery, SetExpr as SqlSetExpr, Statement as SqlStatement,
};

use crate::DEFAULT_TABLE_NAME;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AxonLoweringSummary {
    pub required_columns: Vec<String>,
    pub filter_columns: Vec<String>,
    pub limit: Option<u64>,
}

#[derive(Default)]
struct LoweringBuilder {
    required_columns: BTreeSet<String>,
    filter_columns: BTreeSet<String>,
    limit: Option<u64>,
}

impl LoweringBuilder {
    fn finish(self) -> AxonLoweringSummary {
        AxonLoweringSummary {
            required_columns: self.required_columns.into_iter().collect(),
            filter_columns: self.filter_columns.into_iter().collect(),
            limit: self.limit,
        }
    }
}

pub fn lower_sql_to_axon_summary(sql: &str) -> Result<AxonLoweringSummary> {
    let mut statements = DFParser::parse_sql(sql)?;
    let Some(statement) = statements.pop_front() else {
        return plan_err!("Expected one SQL statement, found none");
    };
    if !statements.is_empty() {
        return plan_err!("Expected one SQL statement, found more than one");
    }
    reject_unsupported_sql_shape(&statement)?;

    let context = AxonPlannerContext::new();
    let planner = SqlToRel::new(&context);
    let plan = planner.statement_to_plan(statement)?;

    let mut builder = LoweringBuilder::default();
    lower_plan(&plan, &mut builder)?;
    Ok(builder.finish())
}

fn reject_unsupported_sql_shape(statement: &DataFusionStatement) -> Result<()> {
    if statement_has_join(statement) {
        return plan_err!("joins are not supported in browser V1 lowering");
    }
    Ok(())
}

fn statement_has_join(statement: &DataFusionStatement) -> bool {
    match statement {
        DataFusionStatement::Statement(statement) => match statement.as_ref() {
            SqlStatement::Query(query) => query_has_join(query),
            _ => false,
        },
        DataFusionStatement::Explain(explain) => statement_has_join(&explain.statement),
        DataFusionStatement::CreateExternalTable(_)
        | DataFusionStatement::CopyTo(_)
        | DataFusionStatement::Reset(_) => false,
    }
}

fn query_has_join(query: &SqlQuery) -> bool {
    set_expr_has_join(&query.body)
}

fn set_expr_has_join(set_expr: &SqlSetExpr) -> bool {
    match set_expr {
        SqlSetExpr::Select(select) => select.from.iter().any(|table| !table.joins.is_empty()),
        SqlSetExpr::Query(query) => query_has_join(query),
        SqlSetExpr::SetOperation { left, right, .. } => {
            set_expr_has_join(left) || set_expr_has_join(right)
        }
        SqlSetExpr::Values(_)
        | SqlSetExpr::Insert(_)
        | SqlSetExpr::Update(_)
        | SqlSetExpr::Delete(_)
        | SqlSetExpr::Merge(_)
        | SqlSetExpr::Table(_) => false,
    }
}

fn lower_plan(plan: &LogicalPlan, builder: &mut LoweringBuilder) -> Result<()> {
    match plan {
        LogicalPlan::Projection(projection) => {
            for expr in &projection.expr {
                collect_projection_columns(expr, &mut builder.required_columns)?;
            }
            lower_plan(&projection.input, builder)
        }
        LogicalPlan::Filter(filter) => {
            let mut filter_columns = BTreeSet::new();
            collect_expr_columns(&filter.predicate, &mut filter_columns)?;
            builder
                .required_columns
                .extend(filter_columns.iter().cloned());
            builder.filter_columns.extend(filter_columns);
            lower_plan(&filter.input, builder)
        }
        LogicalPlan::Limit(limit) => {
            reject_nonzero_skip(limit.skip.as_deref())?;
            builder.limit = limit
                .fetch
                .as_deref()
                .map(literal_limit)
                .transpose()?
                .or(builder.limit);
            lower_plan(&limit.input, builder)
        }
        LogicalPlan::SubqueryAlias(alias) => lower_plan(&alias.input, builder),
        LogicalPlan::TableScan(scan) => {
            if scan.table_name.table() != DEFAULT_TABLE_NAME {
                return plan_err!("table '{}' is not supported", scan.table_name.table());
            }
            Ok(())
        }
        LogicalPlan::Join(_) => plan_err!("joins are not supported in browser V1 lowering"),
        LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::RecursiveQuery(_) => {
            plan_err!("logical plan node is not supported in browser V1 lowering: {plan:?}")
        }
    }
}

fn reject_nonzero_skip(skip: Option<&Expr>) -> Result<()> {
    match skip {
        None => Ok(()),
        Some(expr) if literal_limit(expr)? == 0 => Ok(()),
        Some(_) => plan_err!("OFFSET is not supported in browser V1 lowering"),
    }
}

fn literal_limit(expr: &Expr) -> Result<u64> {
    match expr {
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Ok(*value),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Ok((*value).into()),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value >= 0 => Ok(*value as u64),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) if *value >= 0 => Ok(*value as u64),
        _ => plan_err!("LIMIT must be a non-negative integer literal"),
    }
}

fn collect_projection_columns(expr: &Expr, columns: &mut BTreeSet<String>) -> Result<()> {
    match expr {
        Expr::Column(column) => {
            columns.insert(column.name.clone());
            Ok(())
        }
        Expr::Alias(Alias { expr, .. }) => collect_projection_columns(expr, columns),
        _ => plan_err!("projection expression is not supported in browser V1 lowering: {expr:?}"),
    }
}

fn collect_expr_columns(expr: &Expr, columns: &mut BTreeSet<String>) -> Result<()> {
    match expr {
        Expr::Column(column) => {
            columns.insert(column.name.clone());
            Ok(())
        }
        Expr::Literal(_, _) => Ok(()),
        Expr::Alias(Alias { expr, .. }) => collect_expr_columns(expr, columns),
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            collect_expr_columns(&binary.left, columns)?;
            collect_expr_columns(&binary.right, columns)
        }
        Expr::BinaryExpr(binary) if is_supported_filter_operator(binary.op) => {
            collect_expr_columns(&binary.left, columns)?;
            collect_expr_columns(&binary.right, columns)
        }
        Expr::Not(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Negative(inner) => collect_expr_columns(inner, columns),
        _ => plan_err!("filter expression is not supported in browser V1 lowering: {expr:?}"),
    }
}

fn is_supported_filter_operator(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or
    )
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

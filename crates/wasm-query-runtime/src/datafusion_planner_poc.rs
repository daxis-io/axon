use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result, ScalarValue, TableReference};
use datafusion_expr::expr::Alias;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, LogicalPlan, Operator, ScalarUDF, Signature,
    TableSource, Volatility, WindowUDF,
};
use datafusion_sql::parser::{DFParser, Statement as DataFusionStatement};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::{
    Query as SqlQuery, SetExpr as SqlSetExpr, Statement as SqlStatement,
};

use crate::{BrowserAggregateFunction, DEFAULT_TABLE_NAME};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AxonAggregateMeasureSummary {
    pub function: BrowserAggregateFunction,
    pub source_column: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AxonLoweringSummary {
    pub required_columns: Vec<String>,
    pub filter_columns: Vec<String>,
    pub order_by_columns: Vec<String>,
    pub group_by_columns: Vec<String>,
    pub aggregate_measures: Vec<AxonAggregateMeasureSummary>,
    pub limit: Option<u64>,
}

#[derive(Default)]
struct LoweringBuilder {
    required_columns: BTreeSet<String>,
    filter_columns: BTreeSet<String>,
    projected_output_columns: BTreeSet<String>,
    order_by_columns: Vec<String>,
    group_by_columns: BTreeSet<String>,
    aggregate_measures: Vec<AxonAggregateMeasureSummary>,
    limit: Option<u64>,
}

impl LoweringBuilder {
    fn finish(self) -> AxonLoweringSummary {
        AxonLoweringSummary {
            required_columns: self.required_columns.into_iter().collect(),
            filter_columns: self.filter_columns.into_iter().collect(),
            order_by_columns: self.order_by_columns,
            group_by_columns: self.group_by_columns.into_iter().collect(),
            aggregate_measures: self.aggregate_measures,
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
            let aggregate_input = matches!(projection.input.as_ref(), LogicalPlan::Aggregate(_));
            if !aggregate_input && builder.projected_output_columns.is_empty() {
                builder.projected_output_columns = projection_output_columns(&projection.expr)?;
            }
            lower_plan(&projection.input, builder)?;
            if let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() {
                validate_aggregate_projection(&projection.expr, aggregate, builder)
            } else {
                collect_source_projection(&projection.expr, builder)
            }
        }
        LogicalPlan::Filter(filter) => {
            if plan_outputs_aggregate_rows(filter.input.as_ref()) {
                return plan_err!(
                    "aggregate output filters are not supported in browser V1 lowering"
                );
            }
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
        LogicalPlan::Sort(sort) => {
            lower_plan(&sort.input, builder)?;
            for sort_expr in &sort.expr {
                let column = direct_column_name(&sort_expr.expr).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "ORDER BY expression is not supported in browser V1 lowering: {:?}",
                        sort_expr.expr
                    ))
                })?;
                if !builder.projected_output_columns.contains(&column) {
                    return plan_err!(
                        "ORDER BY expressions must reference projected output columns in browser V1 lowering"
                    );
                }
                push_unique(&mut builder.order_by_columns, column.clone());
            }
            Ok(())
        }
        LogicalPlan::Aggregate(aggregate) => {
            for expr in &aggregate.group_expr {
                let column = direct_column_name(expr).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "GROUP BY expression is not supported in browser V1 lowering: {expr:?}"
                    ))
                })?;
                builder.required_columns.insert(column.clone());
                builder.group_by_columns.insert(column);
            }
            for expr in &aggregate.aggr_expr {
                let measure = lower_aggregate_measure(expr)?;
                if let Some(source_column) = measure.source_column.as_ref() {
                    builder.required_columns.insert(source_column.clone());
                }
                builder.aggregate_measures.push(measure);
            }
            lower_plan(&aggregate.input, builder)
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

fn collect_source_projection(exprs: &[Expr], builder: &mut LoweringBuilder) -> Result<()> {
    for expr in exprs {
        collect_projection_columns(expr, &mut builder.required_columns)?;
    }
    if builder.projected_output_columns.is_empty() {
        builder.projected_output_columns = projection_output_columns(exprs)?;
    }
    Ok(())
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

fn projection_output_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Column(column) => Ok(column.name.clone()),
        Expr::Alias(Alias { name, .. }) => Ok(name.clone()),
        _ => plan_err!("projection expression is not supported in browser V1 lowering: {expr:?}"),
    }
}

fn projection_output_columns(exprs: &[Expr]) -> Result<BTreeSet<String>> {
    exprs.iter().map(projection_output_name).collect()
}

fn validate_aggregate_projection(
    exprs: &[Expr],
    aggregate: &datafusion_expr::logical_plan::Aggregate,
    builder: &mut LoweringBuilder,
) -> Result<()> {
    let aggregate_outputs = aggregate_output_columns(aggregate);
    let mut projected_outputs = BTreeSet::new();
    for expr in exprs {
        let output_name = aggregate_projection_output_name(expr, &aggregate_outputs)?;
        projected_outputs.insert(output_name);
    }
    if builder.projected_output_columns.is_empty() {
        builder.projected_output_columns = projected_outputs;
    }
    Ok(())
}

fn aggregate_output_columns(
    aggregate: &datafusion_expr::logical_plan::Aggregate,
) -> BTreeSet<String> {
    aggregate
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn aggregate_projection_output_name(
    expr: &Expr,
    aggregate_outputs: &BTreeSet<String>,
) -> Result<String> {
    match expr {
        Expr::Column(column) if aggregate_outputs.contains(&column.name) => Ok(column.name.clone()),
        Expr::Alias(Alias { expr, name, .. }) => {
            validate_aggregate_projection_column(expr, aggregate_outputs)?;
            Ok(name.clone())
        }
        _ => plan_err!(
            "aggregate projection expression is not supported in browser V1 lowering: {expr:?}"
        ),
    }
}

fn validate_aggregate_projection_column(
    expr: &Expr,
    aggregate_outputs: &BTreeSet<String>,
) -> Result<()> {
    match expr {
        Expr::Column(column) if aggregate_outputs.contains(&column.name) => Ok(()),
        _ => plan_err!(
            "aggregate projection expression is not supported in browser V1 lowering: {expr:?}"
        ),
    }
}

fn plan_outputs_aggregate_rows(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Projection(projection) => plan_outputs_aggregate_rows(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => plan_outputs_aggregate_rows(&alias.input),
        LogicalPlan::Sort(sort) => plan_outputs_aggregate_rows(&sort.input),
        LogicalPlan::Limit(limit) => plan_outputs_aggregate_rows(&limit.input),
        _ => false,
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

fn lower_aggregate_measure(expr: &Expr) -> Result<AxonAggregateMeasureSummary> {
    let Expr::AggregateFunction(function) = expr else {
        return plan_err!("aggregate expression is not supported in browser V1 lowering: {expr:?}");
    };
    if function.params.distinct {
        return plan_err!("DISTINCT aggregates are not supported in browser V1 lowering");
    }
    if function.params.filter.is_some() {
        return plan_err!("aggregate FILTER clauses are not supported in browser V1 lowering");
    }
    if !function.params.order_by.is_empty() {
        return plan_err!("ordered aggregates are not supported in browser V1 lowering");
    }
    if function.params.null_treatment.is_some() {
        return plan_err!("aggregate null treatment is not supported in browser V1 lowering");
    }

    match function.func.name().to_ascii_lowercase().as_str() {
        "count" => lower_count_measure(&function.params.args),
        "sum" => {
            lower_single_column_measure(&function.params.args, BrowserAggregateFunction::Sum, "SUM")
        }
        "min" => {
            lower_single_column_measure(&function.params.args, BrowserAggregateFunction::Min, "MIN")
        }
        "max" => {
            lower_single_column_measure(&function.params.args, BrowserAggregateFunction::Max, "MAX")
        }
        other => plan_err!("aggregate function '{other}' is not supported in browser V1 lowering"),
    }
}

fn lower_count_measure(args: &[Expr]) -> Result<AxonAggregateMeasureSummary> {
    let [arg] = args else {
        return plan_err!("COUNT aggregates must have exactly one argument");
    };
    if is_count_star_arg(arg) {
        return Ok(AxonAggregateMeasureSummary {
            function: BrowserAggregateFunction::CountStar,
            source_column: None,
        });
    }
    let source_column = direct_column_name(arg).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(format!(
            "COUNT aggregate argument is not supported in browser V1 lowering: {arg:?}"
        ))
    })?;
    Ok(AxonAggregateMeasureSummary {
        function: BrowserAggregateFunction::Count,
        source_column: Some(source_column),
    })
}

fn lower_single_column_measure(
    args: &[Expr],
    function: BrowserAggregateFunction,
    function_name: &str,
) -> Result<AxonAggregateMeasureSummary> {
    let [arg] = args else {
        return plan_err!("{function_name} aggregates must have exactly one argument");
    };
    let source_column = direct_column_name(arg).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(format!(
            "{function_name} aggregate argument is not supported in browser V1 lowering: {arg:?}"
        ))
    })?;
    Ok(AxonAggregateMeasureSummary {
        function,
        source_column: Some(source_column),
    })
}

#[allow(deprecated)]
fn is_count_star_arg(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(value, _) if value == &COUNT_STAR_EXPANSION
    ) || matches!(expr, Expr::Wildcard { .. })
}

fn direct_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Alias(Alias { expr, .. }) => direct_column_name(expr),
        _ => None,
    }
}

fn push_unique(columns: &mut Vec<String>, column: String) {
    if !columns.contains(&column) {
        columns.push(column);
    }
}

struct AxonPlannerContext {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
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

        let mut aggregate_functions = HashMap::new();
        aggregate_functions.insert(
            "count".to_string(),
            planner_aggregate_udf(
                "count",
                Signature::variadic_any(Volatility::Immutable),
                PlannerAggregateReturn::Count,
            ),
        );
        aggregate_functions.insert(
            "sum".to_string(),
            planner_aggregate_udf(
                "sum",
                Signature::numeric(1, Volatility::Immutable),
                PlannerAggregateReturn::Sum,
            ),
        );
        aggregate_functions.insert(
            "min".to_string(),
            planner_aggregate_udf(
                "min",
                Signature::any(1, Volatility::Immutable),
                PlannerAggregateReturn::FirstArg,
            ),
        );
        aggregate_functions.insert(
            "max".to_string(),
            planner_aggregate_udf(
                "max",
                Signature::any(1, Volatility::Immutable),
                PlannerAggregateReturn::FirstArg,
            ),
        );

        Self {
            options: ConfigOptions::default(),
            tables,
            aggregate_functions,
            expr_planners: Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PlannerAggregateUdf {
    name: &'static str,
    signature: Signature,
    return_type: PlannerAggregateReturn,
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum PlannerAggregateReturn {
    Count,
    Sum,
    FirstArg,
}

fn planner_aggregate_udf(
    name: &'static str,
    signature: Signature,
    return_type: PlannerAggregateReturn,
) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::from(PlannerAggregateUdf {
        name,
        signature,
        return_type,
    }))
}

impl AggregateUDFImpl for PlannerAggregateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.return_type {
            PlannerAggregateReturn::Count => Ok(DataType::Int64),
            PlannerAggregateReturn::Sum => sum_return_type(arg_types),
            PlannerAggregateReturn::FirstArg => arg_types.first().cloned().ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(format!(
                    "aggregate function '{}' requires one argument",
                    self.name
                ))
            }),
        }
    }

    fn is_nullable(&self) -> bool {
        !matches!(self.return_type, PlannerAggregateReturn::Count)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        plan_err!(
            "planner-only aggregate function '{}' cannot execute in browser V1 lowering",
            self.name
        )
    }
}

fn sum_return_type(arg_types: &[DataType]) -> Result<DataType> {
    let Some(arg_type) = arg_types.first() else {
        return plan_err!("SUM aggregate requires one argument");
    };
    match arg_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(DataType::Int64),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(DataType::UInt64)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(DataType::Float64),
        other => plan_err!("SUM aggregate is not supported for type {other}"),
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

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions
            .get(&name.to_ascii_lowercase())
            .map(Arc::clone)
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
        self.aggregate_functions.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

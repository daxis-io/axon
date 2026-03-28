use super::*;

#[derive(Clone, Debug)]
enum BrowserSourceBindings {
    BaseTable,
    Named(BTreeMap<String, String>),
}

impl BrowserSourceBindings {
    fn resolve_passthrough_expr(&self, expr: &SqlExpr) -> Option<String> {
        match self {
            Self::BaseTable => base_table_passthrough_column(expr),
            Self::Named(bindings) => {
                passthrough_binding_name(expr).and_then(|name| bindings.get(&name).cloned())
            }
        }
    }
}

pub(super) fn lower_browser_execution_plan(
    query: &SqlQuery,
    snapshot: &BootstrappedBrowserSnapshot,
    planned: &BrowserPlannedQuery,
) -> Result<BrowserExecutionPlan, QueryError> {
    let select = query
        .body
        .as_select()
        .ok_or_else(|| execution_plan_error("browser execution plans require a SELECT query"))?;
    let source_bindings = lower_query_source_bindings(query)?;
    let group_by_columns = lower_group_by_columns(select, &source_bindings)?;
    if select.having.is_some() {
        return Err(execution_plan_error(
            "HAVING is not supported in browser execution plans",
        ));
    }

    let (outputs, measures) = lower_select_outputs(select, &source_bindings)?;
    validate_unique_execution_output_names(&outputs)?;
    let grouped_query = !group_by_columns.is_empty();
    let aggregate_query = grouped_query || !measures.is_empty();
    if aggregate_query {
        let grouped_columns = group_by_columns.iter().cloned().collect::<BTreeSet<_>>();
        for output in &outputs {
            if let BrowserExecutionOutputKind::Passthrough { source_column } = output.kind() {
                if !grouped_columns.contains(source_column) {
                    return Err(execution_plan_error(
                        "non-aggregate outputs must be grouped columns in browser execution plans",
                    ));
                }
            }
        }
    }

    let aggregation = aggregate_query.then(|| BrowserAggregationPlan {
        group_by_columns,
        measures,
    });
    let filter = lower_execution_filter(select.selection.as_ref(), &source_bindings)?;
    let order_by = lower_order_by_keys(query, &outputs)?;
    let required_columns = lower_required_columns(filter.as_ref(), &outputs, aggregation.as_ref());

    Ok(BrowserExecutionPlan {
        table_uri: planned.table_uri.clone(),
        snapshot_version: planned.snapshot_version,
        scan: browser_scan_plan(snapshot, planned, required_columns)?,
        filter,
        outputs,
        aggregation,
        order_by,
        limit: planned.query_shape.limit,
        pruning: planned.pruning.clone(),
    })
}

fn lower_query_source_bindings(query: &SqlQuery) -> Result<BrowserSourceBindings, QueryError> {
    let mut cte_bindings = BTreeMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if !cte.alias.columns.is_empty() {
                return Err(execution_plan_error(
                    "CTE column lists are not supported in browser execution plans",
                ));
            }

            let bindings = lower_query_passthrough_bindings(cte.query.as_ref(), &cte_bindings)?;
            cte_bindings.insert(normalize_name(&cte.alias.name.value), bindings);
        }
    }

    let select = query
        .body
        .as_select()
        .ok_or_else(|| execution_plan_error("browser execution plans require a SELECT query"))?;
    let source = select.from.first().ok_or_else(|| {
        execution_plan_error("browser execution plans require a single source relation")
    })?;
    if !source.joins.is_empty() {
        return Err(execution_plan_error(
            "browser execution plans require a single source relation",
        ));
    }

    lower_table_factor_bindings(&source.relation, &cte_bindings)
}

fn lower_table_factor_bindings(
    table_factor: &SqlTableFactor,
    cte_bindings: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<BrowserSourceBindings, QueryError> {
    match table_factor {
        SqlTableFactor::Table { name, alias, .. } => {
            if alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(execution_plan_error(
                    "table alias column lists are not supported in browser execution plans",
                ));
            }

            let relation_name = object_name_to_relation_name(name)
                .ok_or_else(|| execution_plan_error("query must read only from axon_table"))?;
            if relation_name == DEFAULT_TABLE_NAME {
                Ok(BrowserSourceBindings::BaseTable)
            } else if let Some(bindings) = cte_bindings.get(&relation_name) {
                Ok(BrowserSourceBindings::Named(bindings.clone()))
            } else {
                Err(execution_plan_error("query must read only from axon_table"))
            }
        }
        SqlTableFactor::Derived {
            subquery, alias, ..
        } => {
            if alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(execution_plan_error(
                    "derived-table alias column lists are not supported in browser execution plans",
                ));
            }

            Ok(BrowserSourceBindings::Named(
                lower_query_passthrough_bindings(subquery.as_ref(), cte_bindings)?,
            ))
        }
        _ => Err(execution_plan_error("query must read only from axon_table")),
    }
}

fn lower_query_passthrough_bindings(
    query: &SqlQuery,
    cte_bindings: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, QueryError> {
    if query.order_by.is_some() || query.limit_clause.is_some() {
        return Err(execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        ));
    }

    let select = query.body.as_select().ok_or_else(|| {
        execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        )
    })?;
    if select.having.is_some()
        || matches!(select.group_by, GroupByExpr::All(_))
        || matches!(&select.group_by, GroupByExpr::Expressions(expressions, _) if !expressions.is_empty())
    {
        return Err(execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        ));
    }

    let source = select.from.first().ok_or_else(|| {
        execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        )
    })?;
    if !source.joins.is_empty() {
        return Err(execution_plan_error(
            "CTE and derived queries must remain pure passthrough projections in browser execution plans",
        ));
    }

    let source_bindings = lower_table_factor_bindings(&source.relation, cte_bindings)?;
    let mut bindings = BTreeMap::new();
    for select_item in &select.projection {
        let output = lower_passthrough_output(select_item, &source_bindings)?;
        let BrowserExecutionOutputKind::Passthrough { source_column } = output.kind else {
            return Err(execution_plan_error(
                "CTE and derived queries must remain pure passthrough projections in browser execution plans",
            ));
        };
        if bindings
            .insert(output.output_name.clone(), source_column.clone())
            .is_some()
        {
            return Err(execution_plan_error(
                "duplicate output columns are not supported in browser execution plans",
            ));
        }
    }

    Ok(bindings)
}

fn lower_group_by_columns(
    select: &Select,
    source_bindings: &BrowserSourceBindings,
) -> Result<Vec<String>, QueryError> {
    match &select.group_by {
        GroupByExpr::All(_) => Err(execution_plan_error(
            "GROUP BY ALL is not supported in browser execution plans",
        )),
        GroupByExpr::Expressions(expressions, _) => expressions
            .iter()
            .map(|expression| {
                source_bindings
                    .resolve_passthrough_expr(expression)
                    .ok_or_else(|| {
                        execution_plan_error(
                            "GROUP BY expressions must be passthrough column references in browser execution plans",
                        )
                    })
            })
            .collect(),
    }
}

fn lower_select_outputs(
    select: &Select,
    source_bindings: &BrowserSourceBindings,
) -> Result<(Vec<BrowserExecutionOutput>, Vec<BrowserAggregateMeasure>), QueryError> {
    let mut outputs = Vec::new();
    let mut measures = Vec::new();

    for select_item in &select.projection {
        if let Some((output, measure)) = lower_aggregate_output(select_item, source_bindings)? {
            outputs.push(output);
            measures.push(measure);
        } else {
            outputs.push(lower_passthrough_output(select_item, source_bindings)?);
        }
    }

    Ok((outputs, measures))
}

fn lower_aggregate_output(
    select_item: &SelectItem,
    source_bindings: &BrowserSourceBindings,
) -> Result<Option<(BrowserExecutionOutput, BrowserAggregateMeasure)>, QueryError> {
    let (expr, output_name) = match select_item {
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(normalize_name(&alias.value))),
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(execution_plan_error(
                "wildcard projections are not supported in browser execution plans",
            ))
        }
    };

    let SqlExpr::Function(function) = expr else {
        return Ok(None);
    };
    let function_name = function
        .name
        .0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| normalize_name(&ident.value))
        .ok_or_else(|| {
            execution_plan_error(
                "aggregate projections are not supported in browser execution plans",
            )
        })?;

    let aggregate_function = match function_name.as_str() {
        "avg" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Avg)?
        }
        "array_agg" => lower_single_column_aggregate(
            function,
            source_bindings,
            BrowserAggregateFunction::ArrayAgg,
        )?,
        "bool_and" => lower_single_column_aggregate(
            function,
            source_bindings,
            BrowserAggregateFunction::BoolAnd,
        )?,
        "bool_or" => lower_single_column_aggregate(
            function,
            source_bindings,
            BrowserAggregateFunction::BoolOr,
        )?,
        "count" => lower_count_aggregate(function, source_bindings)?,
        "sum" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Sum)?
        }
        "min" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Min)?
        }
        "max" => {
            lower_single_column_aggregate(function, source_bindings, BrowserAggregateFunction::Max)?
        }
        _ => return Ok(None),
    };
    let output_name = output_name.ok_or_else(|| {
        execution_plan_error(
            "aggregate projections must use explicit aliases in browser execution plans",
        )
    })?;

    let measure = BrowserAggregateMeasure {
        output_name: output_name.clone(),
        function: aggregate_function.0,
        source_column: aggregate_function.1.clone(),
    };
    let output = BrowserExecutionOutput {
        output_name,
        kind: BrowserExecutionOutputKind::Aggregate {
            function: measure.function.clone(),
            source_column: measure.source_column.clone(),
        },
    };

    Ok(Some((output, measure)))
}

fn lower_count_aggregate(
    function: &sqlparser::ast::Function,
    source_bindings: &BrowserSourceBindings,
) -> Result<(BrowserAggregateFunction, Option<String>), QueryError> {
    validate_supported_aggregate_function(function)?;
    let args = single_function_argument(function)?;
    match args {
        FunctionArgExpr::Wildcard => Ok((BrowserAggregateFunction::CountStar, None)),
        FunctionArgExpr::Expr(expr) => {
            let source_column = source_bindings.resolve_passthrough_expr(expr).ok_or_else(|| {
                execution_plan_error(
                    "aggregate arguments must be passthrough column references in browser execution plans",
                )
            })?;
            Ok((BrowserAggregateFunction::Count, Some(source_column)))
        }
        FunctionArgExpr::QualifiedWildcard(_) => Err(execution_plan_error(
            "qualified wildcard aggregates are not supported in browser execution plans",
        )),
    }
}

fn lower_single_column_aggregate(
    function: &sqlparser::ast::Function,
    source_bindings: &BrowserSourceBindings,
    aggregate_function: BrowserAggregateFunction,
) -> Result<(BrowserAggregateFunction, Option<String>), QueryError> {
    validate_supported_aggregate_function(function)?;
    let FunctionArgExpr::Expr(expr) = single_function_argument(function)? else {
        return Err(execution_plan_error(
            "aggregate arguments must be passthrough column references in browser execution plans",
        ));
    };
    let source_column = source_bindings
        .resolve_passthrough_expr(expr)
        .ok_or_else(|| {
            execution_plan_error(
                "aggregate arguments must be passthrough column references in browser execution plans",
            )
        })?;

    Ok((aggregate_function, Some(source_column)))
}

fn validate_supported_aggregate_function(
    function: &sqlparser::ast::Function,
) -> Result<(), QueryError> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    }

    let FunctionArguments::List(arguments) = &function.args else {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    }

    Ok(())
}

fn single_function_argument(
    function: &sqlparser::ast::Function,
) -> Result<&FunctionArgExpr, QueryError> {
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(execution_plan_error(
            "aggregate function form is not supported in browser execution plans",
        ));
    };
    let [argument] = arguments.args.as_slice() else {
        return Err(execution_plan_error(
            "aggregate functions must take exactly one argument in browser execution plans",
        ));
    };

    match argument {
        FunctionArg::Named { arg, .. }
        | FunctionArg::ExprNamed { arg, .. }
        | FunctionArg::Unnamed(arg) => Ok(arg),
    }
}

fn lower_passthrough_output(
    select_item: &SelectItem,
    source_bindings: &BrowserSourceBindings,
) -> Result<BrowserExecutionOutput, QueryError> {
    match select_item {
        SelectItem::ExprWithAlias { expr, alias } => {
            let source_column = source_bindings.resolve_passthrough_expr(expr).ok_or_else(|| {
                execution_plan_error(
                    "projection expressions must be passthrough column references in browser execution plans",
                )
            })?;
            Ok(BrowserExecutionOutput {
                output_name: normalize_name(&alias.value),
                kind: BrowserExecutionOutputKind::Passthrough { source_column },
            })
        }
        SelectItem::UnnamedExpr(expr) => {
            let source_column = source_bindings.resolve_passthrough_expr(expr).ok_or_else(|| {
                execution_plan_error(
                    "projection expressions must be passthrough column references in browser execution plans",
                )
            })?;
            let output_name = passthrough_output_name(expr).ok_or_else(|| {
                execution_plan_error(
                    "projection expressions must be passthrough column references in browser execution plans",
                )
            })?;
            Ok(BrowserExecutionOutput {
                output_name,
                kind: BrowserExecutionOutputKind::Passthrough { source_column },
            })
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => Err(execution_plan_error(
            "wildcard projections are not supported in browser execution plans",
        )),
    }
}

fn passthrough_output_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Nested(expr) => passthrough_output_name(expr),
        SqlExpr::Identifier(ident) => Some(normalize_name(&ident.value)),
        SqlExpr::CompoundIdentifier(parts) => Some(normalize_name(&parts.last()?.value)),
        _ => None,
    }
}

fn passthrough_binding_name(expr: &SqlExpr) -> Option<String> {
    passthrough_output_name(expr)
}

fn base_table_passthrough_column(expr: &SqlExpr) -> Option<String> {
    passthrough_output_name(expr)
}

fn lower_order_by_keys(
    query: &SqlQuery,
    outputs: &[BrowserExecutionOutput],
) -> Result<Vec<BrowserSortKey>, QueryError> {
    let output_names = outputs
        .iter()
        .map(|output| output.output_name.clone())
        .collect::<BTreeSet<_>>();
    let Some(order_by) = &query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(execution_plan_error(
            "ORDER BY expressions are not supported in browser execution plans",
        ));
    };

    expressions
        .iter()
        .map(|expression| {
            if expression.with_fill.is_some() || expression.options.nulls_first.is_some() {
                return Err(execution_plan_error(
                    "ORDER BY options are not supported in browser execution plans",
                ));
            }

            let output_name =
                expr_named_column(&expression.expr, &output_names).ok_or_else(|| {
                    execution_plan_error(
                        "ORDER BY expressions must reference projected output columns in browser execution plans",
                    )
                })?;
            Ok(BrowserSortKey {
                output_name,
                descending: expression.options.asc == Some(false),
            })
        })
        .collect()
}

fn lower_execution_filter(
    selection: Option<&SqlExpr>,
    source_bindings: &BrowserSourceBindings,
) -> Result<Option<BrowserFilterExpr>, QueryError> {
    selection
        .map(|expr| lower_execution_filter_expr(expr, source_bindings))
        .transpose()
}

fn lower_execution_filter_expr(
    expr: &SqlExpr,
    source_bindings: &BrowserSourceBindings,
) -> Result<BrowserFilterExpr, QueryError> {
    match expr {
        SqlExpr::Nested(expr) => lower_execution_filter_expr(expr, source_bindings),
        SqlExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::And => {
            let mut children = Vec::new();
            push_filter_child(
                &mut children,
                lower_execution_filter_expr(left, source_bindings)?,
            );
            push_filter_child(
                &mut children,
                lower_execution_filter_expr(right, source_bindings)?,
            );
            Ok(BrowserFilterExpr::And { children })
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let source_column = source_bindings
                .resolve_passthrough_expr(expr)
                .ok_or_else(where_filter_error)?;
            let literals = list
                .iter()
                .map(|item| expr_non_null_scalar_value(item).ok_or_else(where_filter_error))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(BrowserFilterExpr::InList {
                source_column,
                literals,
            })
        }
        SqlExpr::IsNull(expr) => Ok(BrowserFilterExpr::IsNull {
            source_column: source_bindings
                .resolve_passthrough_expr(expr)
                .ok_or_else(where_filter_error)?,
        }),
        SqlExpr::IsNotNull(expr) => Ok(BrowserFilterExpr::IsNotNull {
            source_column: source_bindings
                .resolve_passthrough_expr(expr)
                .ok_or_else(where_filter_error)?,
        }),
        SqlExpr::BinaryOp { left, op, right } => {
            lower_compare_filter(left, op, right, source_bindings).ok_or_else(where_filter_error)
        }
        _ => Err(where_filter_error()),
    }
}

fn push_filter_child(children: &mut Vec<BrowserFilterExpr>, child: BrowserFilterExpr) {
    match child {
        BrowserFilterExpr::And { children: nested } => children.extend(nested),
        other => children.push(other),
    }
}

fn lower_compare_filter(
    left: &SqlExpr,
    op: &SqlBinaryOperator,
    right: &SqlExpr,
    source_bindings: &BrowserSourceBindings,
) -> Option<BrowserFilterExpr> {
    if let Some(source_column) = source_bindings.resolve_passthrough_expr(left) {
        let literal = expr_non_null_scalar_value(right)?;
        let comparison = browser_comparison_op(op)?;
        return Some(BrowserFilterExpr::Compare {
            source_column,
            comparison,
            literal,
        });
    }

    let source_column = source_bindings.resolve_passthrough_expr(right)?;
    let literal = expr_non_null_scalar_value(left)?;
    let reversed = reverse_binary_operator(op)?;
    let comparison = browser_comparison_op(&reversed)?;
    Some(BrowserFilterExpr::Compare {
        source_column,
        comparison,
        literal,
    })
}

fn browser_comparison_op(op: &SqlBinaryOperator) -> Option<BrowserComparisonOp> {
    Some(match op {
        SqlBinaryOperator::Eq => BrowserComparisonOp::Eq,
        SqlBinaryOperator::Gt => BrowserComparisonOp::Gt,
        SqlBinaryOperator::GtEq => BrowserComparisonOp::Gte,
        SqlBinaryOperator::Lt => BrowserComparisonOp::Lt,
        SqlBinaryOperator::LtEq => BrowserComparisonOp::Lte,
        _ => return None,
    })
}

fn expr_scalar_value(expr: &SqlExpr) -> Option<BrowserScalarValue> {
    match expr {
        SqlExpr::Nested(expr) => expr_scalar_value(expr),
        SqlExpr::UnaryOp { op, expr } => match op {
            SqlUnaryOperator::Plus => Some(BrowserScalarValue::Int64(expr_i64_literal(expr)?)),
            SqlUnaryOperator::Minus => Some(BrowserScalarValue::Int64(
                expr_i64_literal(expr)?.checked_neg()?,
            )),
            _ => None,
        },
        SqlExpr::Value(value) => {
            if let Some(string) = value.value.clone().into_string() {
                return Some(BrowserScalarValue::String(string));
            }

            match &value.value {
                SqlValue::Number(number, _) => {
                    number.parse::<i64>().ok().map(BrowserScalarValue::Int64)
                }
                SqlValue::Boolean(value) => Some(BrowserScalarValue::Boolean(*value)),
                SqlValue::Null => Some(BrowserScalarValue::Null),
                _ => None,
            }
        }
        _ => None,
    }
}

fn expr_non_null_scalar_value(expr: &SqlExpr) -> Option<BrowserScalarValue> {
    match expr_scalar_value(expr)? {
        BrowserScalarValue::Null => None,
        value => Some(value),
    }
}

fn lower_required_columns(
    filter: Option<&BrowserFilterExpr>,
    outputs: &[BrowserExecutionOutput],
    aggregation: Option<&BrowserAggregationPlan>,
) -> Vec<String> {
    let mut required_columns = BTreeSet::new();

    if let Some(filter) = filter {
        collect_required_filter_columns(filter, &mut required_columns);
    }

    for output in outputs {
        match output.kind() {
            BrowserExecutionOutputKind::Passthrough { source_column } => {
                required_columns.insert(source_column.clone());
            }
            BrowserExecutionOutputKind::Aggregate { source_column, .. } => {
                if let Some(source_column) = source_column {
                    required_columns.insert(source_column.clone());
                }
            }
        }
    }

    if let Some(aggregation) = aggregation {
        required_columns.extend(aggregation.group_by_columns.iter().cloned());
        for measure in &aggregation.measures {
            if let Some(source_column) = &measure.source_column {
                required_columns.insert(source_column.clone());
            }
        }
    }

    required_columns.into_iter().collect()
}

fn collect_required_filter_columns(
    filter: &BrowserFilterExpr,
    required_columns: &mut BTreeSet<String>,
) {
    match filter {
        BrowserFilterExpr::And { children } => {
            for child in children {
                collect_required_filter_columns(child, required_columns);
            }
        }
        BrowserFilterExpr::Compare { source_column, .. }
        | BrowserFilterExpr::InList { source_column, .. }
        | BrowserFilterExpr::IsNull { source_column }
        | BrowserFilterExpr::IsNotNull { source_column } => {
            required_columns.insert(source_column.clone());
        }
    }
}

fn where_filter_error() -> QueryError {
    execution_plan_error("WHERE filters must be lowerable into browser execution plans")
}

fn validate_unique_execution_output_names(
    outputs: &[BrowserExecutionOutput],
) -> Result<(), QueryError> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();

    for output in outputs {
        if !seen.insert(output.output_name.clone()) {
            duplicates.insert(output.output_name.clone());
        }
    }

    if duplicates.is_empty() {
        return Ok(());
    }

    Err(execution_plan_error(format!(
        "duplicate output columns are not supported in browser execution plans [{}]",
        duplicates
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

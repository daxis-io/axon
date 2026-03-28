use super::*;

pub(super) fn execute_non_aggregate_plan_rows(
    plan: &BrowserExecutionPlan,
    rows: Vec<BrowserInputRow>,
) -> Result<BrowserExecutionResult, QueryError> {
    if plan.aggregation().is_some() {
        return Err(execution_runtime_error(
            "aggregate browser execution requires the aggregate executor",
        ));
    }

    let mut projected_rows = filter_input_rows(rows, plan.filter())?
        .into_iter()
        .map(|row| project_non_aggregate_row(plan, &row))
        .collect::<Result<Vec<_>, QueryError>>()?;

    sort_projected_rows(&mut projected_rows, plan.order_by());
    apply_limit(&mut projected_rows, plan.limit())?;

    Ok(BrowserExecutionResult {
        output_names: plan
            .outputs()
            .iter()
            .map(|output| output.output_name().to_string())
            .collect(),
        rows: projected_rows.into_iter().map(|row| row.row).collect(),
        metrics: QueryMetricsSummary::default(),
    })
}

fn project_non_aggregate_row(
    plan: &BrowserExecutionPlan,
    row: &BrowserInputRow,
) -> Result<BrowserProjectedRow, QueryError> {
    let mut values_by_name = BTreeMap::new();
    let mut values = Vec::with_capacity(plan.outputs().len());

    for output in plan.outputs() {
        let value = match output.kind() {
            BrowserExecutionOutputKind::Passthrough { source_column } => {
                input_row_value(row, source_column)?.clone()
            }
            BrowserExecutionOutputKind::Aggregate { .. } => {
                return Err(execution_runtime_error(format!(
                    "non-aggregate browser execution cannot project aggregate output '{}'",
                    output.output_name()
                )))
            }
        };
        values_by_name.insert(output.output_name().to_string(), value.clone());
        values.push(value);
    }

    Ok(BrowserProjectedRow {
        values_by_name,
        row: BrowserExecutionRow { values },
    })
}

pub(super) fn execute_aggregate_plan_rows(
    plan: &BrowserExecutionPlan,
    rows: Vec<BrowserInputRow>,
) -> Result<BrowserExecutionResult, QueryError> {
    let aggregation = plan.aggregation().ok_or_else(|| {
        execution_runtime_error("aggregate execution requires an aggregation plan")
    })?;
    let filtered_rows = filter_input_rows(rows, plan.filter())?;
    let mut grouped_rows = group_input_rows(&filtered_rows, aggregation)?;
    let mut projected_rows = Vec::new();

    if aggregation.group_by_columns().is_empty() {
        let rows = grouped_rows.remove(&Vec::new()).unwrap_or_default();
        projected_rows.push(project_aggregate_group(plan, aggregation, &[], &rows)?);
    } else {
        for (group_key, rows) in grouped_rows {
            projected_rows.push(project_aggregate_group(
                plan,
                aggregation,
                &group_key,
                &rows,
            )?);
        }
    }

    sort_projected_rows(&mut projected_rows, plan.order_by());
    apply_limit(&mut projected_rows, plan.limit())?;

    Ok(BrowserExecutionResult {
        output_names: plan
            .outputs()
            .iter()
            .map(|output| output.output_name().to_string())
            .collect(),
        rows: projected_rows.into_iter().map(|row| row.row).collect(),
        metrics: QueryMetricsSummary::default(),
    })
}

fn filter_input_rows(
    rows: Vec<BrowserInputRow>,
    filter: Option<&BrowserFilterExpr>,
) -> Result<Vec<BrowserInputRow>, QueryError> {
    rows.into_iter()
        .filter_map(|row| match filter {
            Some(filter) => match filter_matches(&row, filter) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            },
            None => Some(Ok(row)),
        })
        .collect()
}

fn group_input_rows(
    rows: &[BrowserInputRow],
    aggregation: &BrowserAggregationPlan,
) -> Result<BTreeMap<Vec<BrowserScalarValue>, Vec<BrowserInputRow>>, QueryError> {
    if aggregation.group_by_columns().is_empty() {
        return Ok(BTreeMap::from([(Vec::new(), rows.to_vec())]));
    }

    let mut groups = BTreeMap::<Vec<BrowserScalarValue>, Vec<BrowserInputRow>>::new();
    for row in rows {
        let group_key = aggregation
            .group_by_columns()
            .iter()
            .map(|column| input_row_value(row, column).cloned())
            .collect::<Result<Vec<_>, _>>()?;
        groups.entry(group_key).or_default().push(row.clone());
    }

    Ok(groups)
}

fn project_aggregate_group(
    plan: &BrowserExecutionPlan,
    aggregation: &BrowserAggregationPlan,
    group_key: &[BrowserScalarValue],
    rows: &[BrowserInputRow],
) -> Result<BrowserProjectedRow, QueryError> {
    let group_values = aggregation
        .group_by_columns()
        .iter()
        .cloned()
        .zip(group_key.iter().cloned())
        .collect::<BTreeMap<_, _>>();
    let mut values_by_name = BTreeMap::new();
    let mut values = Vec::with_capacity(plan.outputs().len());

    for output in plan.outputs() {
        let value = match output.kind() {
            BrowserExecutionOutputKind::Passthrough { source_column } => {
                group_values.get(source_column).cloned().ok_or_else(|| {
                    execution_runtime_error(format!(
                        "aggregate browser execution could not resolve group-by output '{}'",
                        output.output_name()
                    ))
                })?
            }
            BrowserExecutionOutputKind::Aggregate {
                function,
                source_column,
            } => aggregate_scalar(function, source_column.as_deref(), rows)?,
        };
        values_by_name.insert(output.output_name().to_string(), value.clone());
        values.push(value);
    }

    Ok(BrowserProjectedRow {
        values_by_name,
        row: BrowserExecutionRow { values },
    })
}

fn aggregate_scalar(
    function: &BrowserAggregateFunction,
    source_column: Option<&str>,
    rows: &[BrowserInputRow],
) -> Result<BrowserScalarValue, QueryError> {
    match function {
        BrowserAggregateFunction::CountStar => {
            let count = i64::try_from(rows.len())
                .map_err(|_| execution_runtime_error("COUNT(*) overflowed i64"))?;
            Ok(BrowserScalarValue::Int64(count))
        }
        BrowserAggregateFunction::Count => {
            let count = aggregate_non_null_scalars(rows, source_column, "COUNT")?.len();
            let count = i64::try_from(count)
                .map_err(|_| execution_runtime_error("COUNT aggregate overflowed i64"))?;
            Ok(BrowserScalarValue::Int64(count))
        }
        BrowserAggregateFunction::Sum => {
            let values = aggregate_non_null_i64_values(rows, source_column, "SUM")?;
            if values.is_empty() {
                return Ok(BrowserScalarValue::Null);
            }
            let Some(sum) = values
                .into_iter()
                .try_fold(0_i64, |acc, value| acc.checked_add(value))
            else {
                return Err(execution_runtime_error("SUM aggregate overflowed i64"));
            };
            Ok(BrowserScalarValue::Int64(sum))
        }
        BrowserAggregateFunction::Avg => {
            let values = aggregate_non_null_i64_values(rows, source_column, "AVG")?;
            if values.is_empty() {
                return Ok(BrowserScalarValue::Null);
            }
            let sum = values
                .iter()
                .try_fold(0_i64, |acc, value| acc.checked_add(*value))
                .ok_or_else(|| execution_runtime_error("AVG aggregate overflowed i64"))?;
            let count = i64::try_from(values.len())
                .map_err(|_| execution_runtime_error("AVG aggregate count overflowed i64"))?;
            if sum % count != 0 {
                return Err(QueryError::new(
                    QueryErrorCode::FallbackRequired,
                    "browser runtime AVG execution requires native fallback for non-integral results",
                    runtime_target(),
                )
                .with_fallback_reason(FallbackReason::NativeRequired));
            }
            Ok(BrowserScalarValue::Int64(sum / count))
        }
        BrowserAggregateFunction::Min => {
            let values = aggregate_non_null_scalars(rows, source_column, "MIN")?;
            Ok(values.into_iter().min().unwrap_or(BrowserScalarValue::Null))
        }
        BrowserAggregateFunction::Max => {
            let values = aggregate_non_null_scalars(rows, source_column, "MAX")?;
            Ok(values.into_iter().max().unwrap_or(BrowserScalarValue::Null))
        }
        BrowserAggregateFunction::ArrayAgg
        | BrowserAggregateFunction::BoolAnd
        | BrowserAggregateFunction::BoolOr => Err(QueryError::new(
            QueryErrorCode::FallbackRequired,
            format!(
                "browser runtime aggregate execution requires native fallback for {:?}",
                function
            ),
            runtime_target(),
        )
        .with_fallback_reason(FallbackReason::NativeRequired)),
    }
}

fn aggregate_non_null_i64_values(
    rows: &[BrowserInputRow],
    source_column: Option<&str>,
    function_name: &str,
) -> Result<Vec<i64>, QueryError> {
    aggregate_non_null_scalars(rows, source_column, function_name)?
        .into_iter()
        .map(|value| match value {
            BrowserScalarValue::Int64(value) => Ok(value),
            other => Err(execution_runtime_error(format!(
                "{function_name} aggregate requires i64-compatible inputs, found {other:?}"
            ))),
        })
        .collect()
}

fn aggregate_non_null_scalars(
    rows: &[BrowserInputRow],
    source_column: Option<&str>,
    function_name: &str,
) -> Result<Vec<BrowserScalarValue>, QueryError> {
    let source_column = source_column.ok_or_else(|| {
        execution_runtime_error(format!(
            "{function_name} aggregate outputs must reference a source column"
        ))
    })?;

    rows.iter()
        .filter_map(|row| match input_row_value(row, source_column) {
            Ok(BrowserScalarValue::Null) => None,
            Ok(value) => Some(Ok(value.clone())),
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn sort_projected_rows(rows: &mut [BrowserProjectedRow], order_by: &[BrowserSortKey]) {
    if order_by.is_empty() {
        return;
    }

    rows.sort_by(|left, right| {
        for sort_key in order_by {
            let left_value = left
                .values_by_name
                .get(sort_key.output_name())
                .expect("projected rows should expose every sort-key output");
            let right_value = right
                .values_by_name
                .get(sort_key.output_name())
                .expect("projected rows should expose every sort-key output");
            let ordering = scalar_sort_order(left_value, right_value, sort_key.descending());
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        Ordering::Equal
    });
}

fn apply_limit(rows: &mut Vec<BrowserProjectedRow>, limit: Option<u64>) -> Result<(), QueryError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let limit = usize::try_from(limit)
        .map_err(|_| execution_runtime_error("browser execution limit overflowed usize"))?;
    if rows.len() > limit {
        rows.truncate(limit);
    }
    Ok(())
}

fn filter_matches(row: &BrowserInputRow, filter: &BrowserFilterExpr) -> Result<bool, QueryError> {
    match filter {
        BrowserFilterExpr::And { children } => children.iter().try_fold(true, |matched, child| {
            Ok(matched && filter_matches(row, child)?)
        }),
        BrowserFilterExpr::Compare {
            source_column,
            comparison,
            literal,
        } => {
            let value = input_row_value(row, source_column)?;
            Ok(compare_scalar_values(value, comparison, literal))
        }
        BrowserFilterExpr::InList {
            source_column,
            literals,
        } => {
            let value = input_row_value(row, source_column)?;
            Ok(literals.iter().any(|literal| scalar_equals(value, literal)))
        }
        BrowserFilterExpr::IsNull { source_column } => Ok(matches!(
            input_row_value(row, source_column)?,
            BrowserScalarValue::Null
        )),
        BrowserFilterExpr::IsNotNull { source_column } => Ok(!matches!(
            input_row_value(row, source_column)?,
            BrowserScalarValue::Null
        )),
    }
}

fn input_row_value<'a>(
    row: &'a BrowserInputRow,
    source_column: &str,
) -> Result<&'a BrowserScalarValue, QueryError> {
    row.get(source_column).ok_or_else(|| {
        execution_runtime_error(format!(
            "browser execution row was missing required column '{source_column}'"
        ))
    })
}

fn compare_scalar_values(
    value: &BrowserScalarValue,
    comparison: &BrowserComparisonOp,
    literal: &BrowserScalarValue,
) -> bool {
    let Some(ordering) = scalar_compare(value, literal) else {
        return false;
    };

    match comparison {
        BrowserComparisonOp::Eq => ordering == Ordering::Equal,
        BrowserComparisonOp::Gt => ordering == Ordering::Greater,
        BrowserComparisonOp::Gte => ordering == Ordering::Greater || ordering == Ordering::Equal,
        BrowserComparisonOp::Lt => ordering == Ordering::Less,
        BrowserComparisonOp::Lte => ordering == Ordering::Less || ordering == Ordering::Equal,
    }
}

fn scalar_compare(left: &BrowserScalarValue, right: &BrowserScalarValue) -> Option<Ordering> {
    match (left, right) {
        (BrowserScalarValue::Int64(left), BrowserScalarValue::Int64(right)) => {
            Some(left.cmp(right))
        }
        (BrowserScalarValue::String(left), BrowserScalarValue::String(right)) => {
            Some(left.cmp(right))
        }
        (BrowserScalarValue::Boolean(left), BrowserScalarValue::Boolean(right)) => {
            Some(left.cmp(right))
        }
        (BrowserScalarValue::Null, BrowserScalarValue::Null) => Some(Ordering::Equal),
        _ => None,
    }
}

fn scalar_equals(left: &BrowserScalarValue, right: &BrowserScalarValue) -> bool {
    matches!(scalar_compare(left, right), Some(Ordering::Equal))
}

fn scalar_sort_order(
    left: &BrowserScalarValue,
    right: &BrowserScalarValue,
    descending: bool,
) -> Ordering {
    match (left, right) {
        (BrowserScalarValue::Null, BrowserScalarValue::Null) => Ordering::Equal,
        (BrowserScalarValue::Null, _) => {
            if descending {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, BrowserScalarValue::Null) => {
            if descending {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let ordering = scalar_compare(left, right)
                .unwrap_or_else(|| scalar_variant_rank(left).cmp(&scalar_variant_rank(right)));
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        }
    }
}

fn scalar_variant_rank(value: &BrowserScalarValue) -> u8 {
    match value {
        BrowserScalarValue::Null => 0,
        BrowserScalarValue::Boolean(_) => 1,
        BrowserScalarValue::Int64(_) => 2,
        BrowserScalarValue::String(_) => 3,
    }
}

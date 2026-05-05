#![cfg(feature = "datafusion-planner-poc")]

use std::path::PathBuf;

use serde::Deserialize;
use wasm_query_runtime::datafusion_planner_poc::lower_sql_to_axon_summary;
use wasm_query_runtime::BrowserAggregateFunction;

#[derive(Debug, Deserialize)]
struct LoweringCorpusCase {
    name: String,
    sql: String,
    expected: Option<ExpectedSummary>,
    expected_error_contains: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExpectedSummary {
    required_columns: Vec<String>,
    filter_columns: Vec<String>,
    order_by_columns: Vec<String>,
    group_by_columns: Vec<String>,
    aggregate_measures: Vec<ExpectedAggregateMeasure>,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ExpectedAggregateMeasure {
    function: String,
    source_column: Option<String>,
}

#[test]
fn lowers_datafusion_sql_corpus_to_axon_summary() {
    for case in load_lowering_corpus() {
        let result = lower_sql_to_axon_summary(&case.sql);

        if let Some(expected_error) = case.expected_error_contains {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains(&expected_error),
                "case '{}': expected error containing '{}', got '{}'",
                case.name,
                expected_error,
                error
            );
            continue;
        }

        let expected = case
            .expected
            .expect("supported case should declare expected");
        let summary = result.unwrap_or_else(|error| {
            panic!("case '{}': expected supported SQL, got {error}", case.name)
        });

        assert_eq!(
            summary.required_columns, expected.required_columns,
            "case '{}': required columns should match",
            case.name
        );
        assert_eq!(
            summary.filter_columns, expected.filter_columns,
            "case '{}': filter columns should match",
            case.name
        );
        assert_eq!(
            summary.order_by_columns, expected.order_by_columns,
            "case '{}': order-by columns should match",
            case.name
        );
        assert_eq!(
            summary.group_by_columns, expected.group_by_columns,
            "case '{}': group-by columns should match",
            case.name
        );
        assert_eq!(
            summary.limit, expected.limit,
            "case '{}': limit should match",
            case.name
        );
        assert_eq!(
            summary.aggregate_measures.len(),
            expected.aggregate_measures.len(),
            "case '{}': aggregate measure count should match",
            case.name
        );
        for (actual, expected) in summary
            .aggregate_measures
            .iter()
            .zip(expected.aggregate_measures.iter())
        {
            assert_eq!(
                actual.function,
                expected_aggregate_function(&expected.function),
                "case '{}': aggregate function should match",
                case.name
            );
            assert_eq!(
                actual.source_column, expected.source_column,
                "case '{}': aggregate source column should match",
                case.name
            );
        }
    }
}

fn lowering_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/conformance/browser-datafusion-lowering-corpus.json")
}

fn load_lowering_corpus() -> Vec<LoweringCorpusCase> {
    serde_json::from_str(
        &std::fs::read_to_string(lowering_corpus_path())
            .expect("datafusion lowering corpus should be readable"),
    )
    .expect("datafusion lowering corpus should deserialize")
}

fn expected_aggregate_function(name: &str) -> BrowserAggregateFunction {
    match name {
        "count_star" => BrowserAggregateFunction::CountStar,
        "count" => BrowserAggregateFunction::Count,
        "sum" => BrowserAggregateFunction::Sum,
        "min" => BrowserAggregateFunction::Min,
        "max" => BrowserAggregateFunction::Max,
        other => panic!("unsupported expected aggregate function '{other}'"),
    }
}

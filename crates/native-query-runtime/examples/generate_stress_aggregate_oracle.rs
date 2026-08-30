use std::env;
use std::fs;
use std::path::PathBuf;

use deltalake::arrow::util::display::array_value_to_string;
use native_query_runtime::{execute_query, DEFAULT_TABLE_NAME};
use query_contract::{ExecutionTarget, QueryExecutionOptions, QueryRequest, QueryResultPage};
use serde::Serialize;

const PAGE_ROWS: u64 = 500;
const EXPECTED_STRESS_GROUP_COUNT: u64 = 10_500_000;
const SQL_TEMPLATE: &str = include_str!(
    "../../../apps/axon-web/tests/fixtures/browser-external-memory/stress-aggregate.sql"
);
const FINGERPRINT_SQL_TEMPLATE: &str = include_str!(
    "../../../apps/axon-web/tests/fixtures/browser-external-memory/stress-aggregate-fingerprint.sql"
);

#[derive(Serialize)]
struct OracleRows {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct StressAggregateOracle {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    full_result_fingerprint: OracleRows,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args_os().skip(1);
    let table_uri = args
        .next()
        .ok_or("usage: generate_stress_aggregate_oracle <delta-table-path> <output-json>")?;
    let output_path = PathBuf::from(
        args.next()
            .ok_or("usage: generate_stress_aggregate_oracle <delta-table-path> <output-json>")?,
    );
    if args.next().is_some() {
        return Err(
            "usage: generate_stress_aggregate_oracle <delta-table-path> <output-json>".into(),
        );
    }

    // The native reference runtime intentionally exposes its one registered table under a fixed
    // name. Keep the query body and ordering canonical while adapting only that binding.
    let sql = SQL_TEMPLATE.replace("query_engine_stress_delta", DEFAULT_TABLE_NAME);
    let fingerprint_sql =
        FINGERPRINT_SQL_TEMPLATE.replace("query_engine_stress_delta", DEFAULT_TABLE_NAME);

    let table_uri = PathBuf::from(table_uri).to_string_lossy().into_owned();
    let first_page = execute_rows(
        &table_uri,
        sql.clone(),
        Some(QueryResultPage {
            limit: PAGE_ROWS,
            offset: 0,
        }),
    )?;
    if first_page.rows.len() != PAGE_ROWS as usize {
        return Err(format!(
            "native oracle returned {} first-page rows; expected {PAGE_ROWS}",
            first_page.rows.len()
        )
        .into());
    }
    let full_result_fingerprint = execute_rows(&table_uri, fingerprint_sql, None)?;
    if full_result_fingerprint.rows.len() != 1 {
        return Err(format!(
            "native full-result fingerprint returned {} rows; expected 1",
            full_result_fingerprint.rows.len()
        )
        .into());
    }
    let group_count = full_result_fingerprint.rows[0][0].parse::<u64>()?;
    if group_count != EXPECTED_STRESS_GROUP_COUNT {
        return Err(format!(
            "native full-result fingerprint reported {group_count} groups; expected the exact 4.4 GiB stress fixture with {EXPECTED_STRESS_GROUP_COUNT} groups"
        )
        .into());
    }
    let oracle = StressAggregateOracle {
        columns: first_page.columns,
        rows: first_page.rows,
        full_result_fingerprint,
    };
    fs::write(output_path, serde_json::to_vec_pretty(&oracle)?)?;
    Ok(())
}

fn execute_rows(
    table_uri: &str,
    sql: String,
    result_page: Option<QueryResultPage>,
) -> Result<OracleRows, Box<dyn std::error::Error>> {
    let request = QueryRequest::new(table_uri, sql, ExecutionTarget::Native).with_options(
        QueryExecutionOptions {
            result_page,
            ..QueryExecutionOptions::default()
        },
    );
    let result = execute_query(request).map_err(|error| error.message)?;
    let schema = result
        .batches
        .first()
        .map(|batch| batch.schema())
        .ok_or("native oracle query returned no record batches")?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in result.batches {
        for row_index in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column.as_ref(), row_index))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(OracleRows { columns, rows })
}

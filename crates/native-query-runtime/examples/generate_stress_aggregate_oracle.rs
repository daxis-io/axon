use std::env;
use std::fs;
use std::path::PathBuf;

use deltalake::arrow::util::display::array_value_to_string;
use native_query_runtime::{execute_query, DEFAULT_TABLE_NAME};
use query_contract::{ExecutionTarget, QueryExecutionOptions, QueryRequest, QueryResultPage};
use serde::Serialize;

const PAGE_ROWS: u64 = 500;
const SQL_TEMPLATE: &str = include_str!(
    "../../../apps/axon-web/tests/fixtures/browser-external-memory/stress-aggregate.sql"
);

#[derive(Serialize)]
struct StressAggregateOracle {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
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

    let request = QueryRequest::new(
        PathBuf::from(table_uri).to_string_lossy(),
        sql,
        ExecutionTarget::Native,
    )
    .with_options(QueryExecutionOptions {
        result_page: Some(QueryResultPage {
            limit: PAGE_ROWS,
            offset: 0,
        }),
        ..QueryExecutionOptions::default()
    });
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
    if rows.len() != PAGE_ROWS as usize {
        return Err(format!(
            "native oracle returned {} rows; expected {PAGE_ROWS}",
            rows.len()
        )
        .into());
    }

    let oracle = StressAggregateOracle { columns, rows };
    fs::write(output_path, serde_json::to_vec_pretty(&oracle)?)?;
    Ok(())
}

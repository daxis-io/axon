use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::properties::{WriterProperties, WriterVersion};
use deltalake::DeltaTable;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const FIXTURE_SCHEMA_VERSION: u32 = 1;
const FIXTURE_REVISION: &str = "browser-external-memory-v1";
const TABLE_NAME: &str = "spill_conformance";
const DEFAULT_ACTIVE_FILES: usize = 16;
const DEFAULT_ROWS_PER_FILE: usize = 100_000;
const DEFAULT_GROUP_COUNT: usize = 800_000;
const FIXED_MODIFICATION_TIME_MS: i64 = 1_780_000_000_000;
const FIXTURE_ROOT_MARKER: &str = ".axon-spill-fixture-root";
const FIXTURE_ROOT_MARKER_CONTENTS: &str = "axon browser spill fixture v1\n";

const AGGREGATE_SQL: &str = r#"
SELECT group_id % 256 AS bucket,
       COUNT(*) AS group_count,
       SUM(quantity_sum) AS quantity_sum,
       SUM(score_sum) AS score_sum
FROM (
  SELECT group_id, group_key, SUM(quantity) AS quantity_sum, SUM(score) AS score_sum
  FROM spill_conformance
  GROUP BY group_id, group_key
) AS grouped
GROUP BY group_id % 256
ORDER BY bucket
"#;

const AGGREGATE_STATES_SQL: &str = r#"
SELECT group_id % 256 AS bucket,
       SUM(non_null_count) AS non_null_count,
       SUM(average_score) AS average_score_sum,
       MIN(minimum_nullable) AS minimum_nullable,
       MAX(maximum_nullable) AS maximum_nullable
FROM (
  SELECT group_id,
         COUNT(nullable_value) AS non_null_count,
         AVG(score) AS average_score,
         MIN(nullable_value) AS minimum_nullable,
         MAX(nullable_value) AS maximum_nullable
  FROM spill_conformance
  GROUP BY group_id
) AS grouped
GROUP BY group_id % 256
ORDER BY bucket
"#;

const EXTERNAL_SORT_SQL: &str = r#"
SELECT group_id % 256 AS bucket,
       COUNT(*) AS row_count,
       SUM(sorted_position) AS sorted_position_sum
FROM (
  SELECT group_id,
         CAST(ROW_NUMBER() OVER (ORDER BY group_key DESC, row_id) AS BIGINT) AS sorted_position
  FROM spill_conformance
) AS ranked
GROUP BY group_id % 256
ORDER BY bucket
"#;

#[derive(Debug, Clone)]
struct FixtureConfig {
    active_files: usize,
    rows_per_file: usize,
    group_count: usize,
}

impl FixtureConfig {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let test_mode = env::var("AXON_SPILL_FIXTURE_TEST_MODE").as_deref() == Ok("1");
        let config = if test_mode {
            Self {
                active_files: env_usize("AXON_SPILL_FIXTURE_ACTIVE_FILES")?,
                rows_per_file: env_usize("AXON_SPILL_FIXTURE_ROWS_PER_FILE")?,
                group_count: env_usize("AXON_SPILL_FIXTURE_GROUP_COUNT")?,
            }
        } else {
            Self {
                active_files: DEFAULT_ACTIVE_FILES,
                rows_per_file: DEFAULT_ROWS_PER_FILE,
                group_count: DEFAULT_GROUP_COUNT,
            }
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.active_files == 0 || self.rows_per_file == 0 || self.group_count < 256 {
            return Err("spill fixture requires files, rows, and at least 256 groups".into());
        }
        if self.group_count > self.row_count() {
            return Err("spill fixture group count cannot exceed row count".into());
        }
        Ok(())
    }

    fn row_count(&self) -> usize {
        self.active_files * self.rows_per_file
    }
}

#[derive(Debug, Serialize)]
struct FixtureManifest {
    schema_version: u32,
    fixture_revision: &'static str,
    table_name: &'static str,
    seed: String,
    row_count: usize,
    group_count: usize,
    active_file_count: usize,
    rows_per_file: usize,
    objects: Vec<FixtureObject>,
    queries: Vec<QuerySpec>,
}

#[derive(Debug, Serialize)]
struct FixtureObject {
    relative_path: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct QuerySpec {
    id: &'static str,
    sql: &'static str,
    expected_operator: &'static str,
}

#[derive(Debug, Serialize)]
struct NativeOracle {
    schema_version: u32,
    fixture_revision: &'static str,
    queries: Vec<QueryOracle>,
}

#[derive(Debug, Serialize)]
struct QueryOracle {
    id: &'static str,
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let output_root = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fixtures/browser-external-memory-v1"));
    generate_fixture(&output_root, &FixtureConfig::from_env()?).await
}

async fn generate_fixture(
    output_root: &Path,
    config: &FixtureConfig,
) -> Result<(), Box<dyn Error>> {
    prepare_output_root(output_root)?;
    let table_root = output_root.join("table");
    fs::create_dir_all(table_root.join("_delta_log"))?;

    let schema = fixture_schema();
    let mut objects = Vec::with_capacity(config.active_files + 1);
    for file_index in 0..config.active_files {
        let relative_path = format!("part-{file_index:05}.snappy.parquet");
        let path = table_root.join(&relative_path);
        write_parquet_file(&path, Arc::clone(&schema), config, file_index)?;
        objects.push(fixture_object(&table_root, &relative_path)?);
    }

    let log_relative_path = "_delta_log/00000000000000000000.json";
    write_delta_log(&table_root.join(log_relative_path), config, &objects)?;
    objects.push(fixture_object(&table_root, log_relative_path)?);

    let queries = query_specs();
    let manifest = FixtureManifest {
        schema_version: FIXTURE_SCHEMA_VERSION,
        fixture_revision: FIXTURE_REVISION,
        table_name: TABLE_NAME,
        seed: "0xa80d5eed20260801".to_owned(),
        row_count: config.row_count(),
        group_count: config.group_count,
        active_file_count: config.active_files,
        rows_per_file: config.rows_per_file,
        objects,
        queries: queries.clone(),
    };
    fs::write(
        output_root.join("fixture-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    let oracle = build_native_oracle(&table_root, &queries).await?;
    fs::write(
        output_root.join("native-oracle.json"),
        serde_json::to_vec_pretty(&oracle)?,
    )?;
    println!(
        "Generated browser external-memory fixture at {}",
        output_root.display()
    );
    Ok(())
}

fn prepare_output_root(output_root: &Path) -> Result<(), Box<dyn Error>> {
    if output_root.exists() {
        let metadata = fs::symlink_metadata(output_root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "refusing to replace non-directory or symlink fixture output '{}'",
                output_root.display()
            )
            .into());
        }
        if !is_owned_fixture_root(output_root) {
            return Err(format!(
                "refusing to replace unmarked fixture output '{}'; choose a new path or a previously generated Axon spill fixture",
                output_root.display()
            )
            .into());
        }
        fs::remove_dir_all(output_root)?;
    }
    fs::create_dir_all(output_root)?;
    fs::write(
        output_root.join(FIXTURE_ROOT_MARKER),
        FIXTURE_ROOT_MARKER_CONTENTS,
    )?;
    Ok(())
}

fn is_owned_fixture_root(output_root: &Path) -> bool {
    if matches!(
        fs::read_to_string(output_root.join(FIXTURE_ROOT_MARKER)),
        Ok(contents) if contents == FIXTURE_ROOT_MARKER_CONTENTS
    ) {
        return true;
    }

    // Compatibility for v1 fixtures generated before the ownership marker was introduced.
    let Ok(bytes) = fs::read(output_root.join("fixture-manifest.json")) else {
        return false;
    };
    let Ok(manifest) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return false;
    };
    manifest
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        == Some(u64::from(FIXTURE_SCHEMA_VERSION))
        && manifest
            .get("fixture_revision")
            .and_then(serde_json::Value::as_str)
            == Some(FIXTURE_REVISION)
        && manifest
            .get("table_name")
            .and_then(serde_json::Value::as_str)
            == Some(TABLE_NAME)
}

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("group_id", DataType::Int64, false),
        Field::new("group_key", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("nullable_value", DataType::Int64, true),
    ]))
}

fn write_parquet_file(
    path: &Path,
    schema: Arc<Schema>,
    config: &FixtureConfig,
    file_index: usize,
) -> Result<(), Box<dyn Error>> {
    let row_start = file_index * config.rows_per_file;
    let row_end = row_start + config.rows_per_file;
    let row_ids = (row_start..row_end)
        .map(|value| i64::try_from(value).expect("fixture row id must fit i64"))
        .collect::<Vec<_>>();
    let group_ids = row_ids
        .iter()
        .map(|row_id| *row_id % i64::try_from(config.group_count).unwrap())
        .collect::<Vec<_>>();
    let group_keys = group_ids
        .iter()
        .map(|group_id| format!("group-{group_id:016x}-axon-spill-conformance"))
        .collect::<Vec<_>>();
    let quantities = row_ids
        .iter()
        .map(|row_id| row_id % 17 + 1)
        .collect::<Vec<_>>();
    let scores = row_ids
        .iter()
        .map(|row_id| (row_id % 1_000) as f64 / 8.0)
        .collect::<Vec<_>>();
    let nullable_values = row_ids
        .iter()
        .map(|row_id| (row_id % 7 != 0).then_some(row_id % 101))
        .collect::<Vec<_>>();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(Int64Array::from(group_ids)),
            Arc::new(StringArray::from(group_keys)),
            Arc::new(Int64Array::from(quantities)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(Int64Array::from(nullable_values)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(25_000.min(config.rows_per_file)))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_delta_log(
    path: &Path,
    config: &FixtureConfig,
    data_files: &[FixtureObject],
) -> Result<(), Box<dyn Error>> {
    let schema_string = json!({
        "type": "struct",
        "fields": [
            {"name":"row_id","type":"long","nullable":false,"metadata":{}},
            {"name":"group_id","type":"long","nullable":false,"metadata":{}},
            {"name":"group_key","type":"string","nullable":false,"metadata":{}},
            {"name":"quantity","type":"long","nullable":false,"metadata":{}},
            {"name":"score","type":"double","nullable":false,"metadata":{}},
            {"name":"nullable_value","type":"long","nullable":true,"metadata":{}}
        ]
    })
    .to_string();
    let mut actions = vec![
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        json!({"metaData":{
            "id":"a80d5eed-2026-0801-8000-000000000001",
            "name":TABLE_NAME,
            "description":"Deterministic browser external-memory spill conformance fixture",
            "format":{"provider":"parquet","options":{}},
            "schemaString":schema_string,
            "partitionColumns":[],
            "configuration":{},
            "createdTime":FIXED_MODIFICATION_TIME_MS
        }}),
    ];
    for file in data_files {
        actions.push(json!({"add":{
            "path":file.relative_path,
            "partitionValues":{},
            "size":file.size_bytes,
            "modificationTime":FIXED_MODIFICATION_TIME_MS,
            "dataChange":true,
            "stats":json!({"numRecords":config.rows_per_file}).to_string()
        }}));
    }
    let mut bytes = Vec::new();
    for action in actions {
        serde_json::to_writer(&mut bytes, &action)?;
        bytes.push(b'\n');
    }
    fs::write(path, bytes)?;
    Ok(())
}

fn query_specs() -> Vec<QuerySpec> {
    vec![
        QuerySpec {
            id: "aggregate",
            sql: AGGREGATE_SQL.trim(),
            expected_operator: "GROUPED_AGGREGATE",
        },
        QuerySpec {
            id: "aggregate_states",
            sql: AGGREGATE_STATES_SQL.trim(),
            expected_operator: "GROUPED_AGGREGATE",
        },
        QuerySpec {
            id: "external_sort",
            sql: EXTERNAL_SORT_SQL.trim(),
            expected_operator: "EXTERNAL_SORT",
        },
    ]
}

async fn build_native_oracle(
    table_root: &Path,
    queries: &[QuerySpec],
) -> Result<NativeOracle, Box<dyn Error>> {
    let table_uri = deltalake::ensure_table_uri(table_root.to_string_lossy())?;
    let table: DeltaTable = deltalake::open_table(table_uri).await?;
    let session = SessionContext::new();
    table.update_datafusion_session(&session.state())?;
    session.register_table(TABLE_NAME, table.table_provider().await?)?;

    let mut query_oracles = Vec::with_capacity(queries.len());
    for query in queries {
        let batches = session.sql(query.sql).await?.collect().await?;
        let schema = batches
            .first()
            .ok_or("native oracle query returned no batches")?
            .schema();
        let columns = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let mut rows = Vec::new();
        for batch in &batches {
            for row_index in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for column in batch.columns() {
                    row.push(if column.is_null(row_index) {
                        None
                    } else if column.data_type() == &DataType::Float64 {
                        Some(
                            column
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .expect("Float64 column must downcast")
                                .value(row_index)
                                .to_string(),
                        )
                    } else {
                        Some(deltalake::arrow::util::display::array_value_to_string(
                            column.as_ref(),
                            row_index,
                        )?)
                    });
                }
                rows.push(row);
            }
        }
        query_oracles.push(QueryOracle {
            id: query.id,
            columns,
            rows,
        });
    }

    Ok(NativeOracle {
        schema_version: FIXTURE_SCHEMA_VERSION,
        fixture_revision: FIXTURE_REVISION,
        queries: query_oracles,
    })
}

fn fixture_object(table_root: &Path, relative_path: &str) -> Result<FixtureObject, Box<dyn Error>> {
    let path = table_root.join(relative_path);
    let bytes = fs::read(&path)?;
    Ok(FixtureObject {
        relative_path: relative_path.to_owned(),
        size_bytes: u64::try_from(bytes.len())?,
        sha256: format!("{:x}", Sha256::digest(bytes)),
    })
}

fn env_usize(name: &str) -> Result<usize, Box<dyn Error>> {
    Ok(env::var(name)
        .map_err(|_| format!("{name} is required in spill fixture test mode"))?
        .parse::<usize>()?)
}

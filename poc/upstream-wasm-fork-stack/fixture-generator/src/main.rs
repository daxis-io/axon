use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

const QUERY: &str =
    "SELECT category, SUM(value) AS total FROM delta GROUP BY category ORDER BY category";

#[tokio::main]
async fn main() {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures"));
    let mut tables = generate(&output).expect("fixture generation must succeed");
    tables.push(
        generate_checkpointed(&output)
            .await
            .expect("checkpoint fixture generation must succeed"),
    );
    write_manifest(&output, tables).expect("manifest must be written");
}

fn generate(output: &Path) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let rows = [
        ("alpha", 2_i64),
        ("beta", 3_i64),
        ("alpha", 5_i64),
        ("beta", 7_i64),
    ];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(category, _)| *category)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )?;

    fs::create_dir_all(output)?;
    let mut tables = Vec::new();
    for (name, compression) in [
        ("snappy", Compression::SNAPPY),
        ("zstd", Compression::ZSTD(ZstdLevel::default())),
    ] {
        let table_root = output.join(name);
        let log_root = table_root.join("_delta_log");
        fs::create_dir_all(&log_root)?;

        let parquet_name = format!("part-00000.{name}.parquet");
        let parquet_path = table_root.join(&parquet_name);
        let mut parquet = Vec::new();
        let properties = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut writer = ArrowWriter::try_new(&mut parquet, Arc::clone(&schema), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        fs::write(&parquet_path, &parquet)?;

        let schema_string = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "category",
                    "type": "string",
                    "nullable": false,
                    "metadata": {}
                },
                {
                    "name": "value",
                    "type": "long",
                    "nullable": false,
                    "metadata": {}
                }
            ]
        })
        .to_string();
        let table_id = if name == "snappy" {
            "00000000-0000-4000-8000-000000000001"
        } else {
            "00000000-0000-4000-8000-000000000002"
        };
        let log = [
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": table_id,
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema_string,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0
                }
            }),
            json!({
                "add": {
                    "path": parquet_name,
                    "partitionValues": {},
                    "size": parquet.len(),
                    "modificationTime": 0,
                    "dataChange": true,
                    "stats": "{\"numRecords\":4}"
                }
            }),
            json!({
                "commitInfo": {
                    "timestamp": 0,
                    "operation": "WRITE"
                }
            }),
        ]
        .into_iter()
        .map(|action| action.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        let log_path = log_root.join("00000000000000000000.json");
        fs::write(&log_path, log.as_bytes())?;

        tables.push(json!({
            "name": name,
            "codec": name,
            "root": name,
            "schema": [
                {"name": "category", "type": "string", "nullable": false},
                {"name": "value", "type": "long", "nullable": false}
            ],
            "rows": rows
                .iter()
                .map(|(category, value)| json!({"category": category, "value": value}))
                .collect::<Vec<Value>>(),
            "expected": {
                "query": QUERY,
                "row_count": 2,
                "rows": [
                    {"category": "alpha", "total": 7},
                    {"category": "beta", "total": 10}
                ]
            },
            "files": [
                file_record(output, &log_path, log.as_bytes()),
                file_record(output, &parquet_path, &parquet)
            ]
        }));
    }

    Ok(tables)
}

fn write_manifest(output: &Path, tables: Vec<Value>) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = json!({
        "schema_version": 1,
        "generator": {
            "arrow": "58.3.0",
            "parquet": "58.3.0",
            "deltalake": "0.32.4"
        },
        "tables": tables
    });
    fs::write(
        output.join("manifest.json"),
        format!("{}\n", serde_json::to_string_pretty(&manifest)?),
    )?;
    Ok(())
}

/// Build a table whose pre-checkpoint commits have been cleaned up.
///
/// Creation writes version 0, the first two data writes produce versions 1 and
/// 2, and version 2 is checkpointed. The version 0 and 1 commit JSON files are
/// then deleted — which the Delta protocol permits once a checkpoint covers
/// them — before version 3 is appended. A reader that ignores the checkpoint
/// cannot reconstruct the version-1 rows, so this fixture distinguishes real
/// checkpoint replay from a reader that merely tolerates a checkpoint's
/// presence.
async fn generate_checkpointed(output: &Path) -> Result<Value, Box<dyn std::error::Error>> {
    use deltalake::DeltaTable;
    use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
    use deltalake::protocol::checkpoints::create_checkpoint;

    let name = "checkpointed";
    let table_root = output.join(name);
    if table_root.exists() {
        fs::remove_dir_all(&table_root)?;
    }
    fs::create_dir_all(&table_root)?;
    let table_url = Url::from_directory_path(table_root.canonicalize()?)
        .map_err(|_| "fixture table root must be an absolute path")?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table = DeltaTable::try_from_url(table_url)
        .await?
        .create()
        .with_columns(vec![
            StructField::new(
                "category",
                DeltaDataType::Primitive(PrimitiveType::String),
                false,
            ),
            StructField::new(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Long),
                false,
            ),
        ])
        .await?;

    // v1 and v2 are covered by the checkpoint; v3 lands after it.
    let commits: [&[(&str, i64)]; 3] = [
        &[("alpha", 2), ("beta", 3)],
        &[("alpha", 5), ("beta", 7)],
        &[("alpha", 11), ("beta", 13)],
    ];
    let mut table = table;
    for (index, rows) in commits.iter().enumerate() {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )?;
        table = table.write(vec![batch]).await?;
        if index == 1 {
            create_checkpoint(&table, None).await?;
        }
    }

    // Read the checkpoint version the writer actually chose rather than assuming
    // one: `create()` consumes version 0, so the writes land at 1..=3.
    let log_root = table_root.join("_delta_log");
    let last_checkpoint: Value =
        serde_json::from_slice(&fs::read(log_root.join("_last_checkpoint"))?)?;
    let checkpoint_version = last_checkpoint["version"]
        .as_u64()
        .ok_or("_last_checkpoint is missing a version")?;

    // Delete every commit before the checkpoint boundary, so the checkpoint is
    // the only route to the version-1 rows.
    let mut removed = Vec::new();
    for version in 0..checkpoint_version {
        let commit = log_root.join(format!("{version:020}.json"));
        if commit.exists() {
            fs::remove_file(&commit)?;
            removed.push(format!("_delta_log/{version:020}.json"));
        }
    }

    let mut files = Vec::new();
    collect_files(output, &table_root, &mut files)?;
    files.sort_by_key(|value| value["path"].as_str().unwrap_or_default().to_string());

    Ok(json!({
        "name": name,
        "codec": "snappy",
        "root": name,
        "checkpoint": {
            "version": checkpoint_version,
            "removed_commits": removed,
            "latest_version": table.version()
        },
        "schema": [
            {"name": "category", "type": "string", "nullable": false},
            {"name": "value", "type": "long", "nullable": false}
        ],
        "expected": {
            "query": QUERY,
            "row_count": 2,
            // Only correct if the checkpoint is replayed: alpha 2+5+11, beta 3+7+13.
            "rows": [
                {"category": "alpha", "total": 18},
                {"category": "beta", "total": 23}
            ]
        },
        "files": files
    }))
}

fn collect_files(
    output: &Path,
    dir: &Path,
    files: &mut Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(output, &path, files)?;
        } else {
            let bytes = fs::read(&path)?;
            files.push(file_record(output, &path, &bytes));
        }
    }
    Ok(())
}

fn file_record(output: &Path, path: &Path, bytes: &[u8]) -> Value {
    json!({
        "path": path
            .strip_prefix(output)
            .expect("fixture file must be under the output root")
            .to_string_lossy()
            .replace('\\', "/"),
        "bytes": bytes.len(),
        "sha256": format!("{:x}", Sha256::digest(bytes))
    })
}

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

const QUERY: &str =
    "SELECT category, SUM(value) AS total FROM delta GROUP BY category ORDER BY category";

fn main() {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures"));
    generate(&output).expect("fixture generation must succeed");
}

fn generate(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
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

    let manifest = json!({
        "schema_version": 1,
        "generator": {
            "arrow": "58.3.0",
            "parquet": "58.3.0"
        },
        "tables": tables
    });
    fs::write(
        output.join("manifest.json"),
        format!("{}\n", serde_json::to_string_pretty(&manifest)?),
    )?;
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

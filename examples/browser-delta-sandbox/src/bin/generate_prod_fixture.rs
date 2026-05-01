use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{checkpoints, DeltaTable};
use serde::Serialize;

const BROWSER_TABLE_URI: &str = "gs://axon-sandbox/prod-like-events";

#[derive(Debug, Serialize)]
struct FixtureManifest {
    name: &'static str,
    table_uri: &'static str,
    expected_latest_version: i64,
    checkpoint_version: i64,
    generated_steps: Vec<GeneratedStep>,
    objects: Vec<ManifestObject>,
    data_files: Vec<DataFileInventory>,
}

#[derive(Debug, Serialize)]
struct GeneratedStep {
    version: i64,
    label: &'static str,
    detail: &'static str,
}

#[derive(Debug, Serialize)]
struct ManifestObject {
    relative_path: String,
    url_path: String,
    kind: ObjectKind,
    size_bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObjectKind {
    CommitJson,
    CheckpointParquet,
    LastCheckpoint,
}

#[derive(Debug, Serialize)]
struct DataFileInventory {
    relative_path: String,
    url_path: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let output_root = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("public/fixtures/prod-like"));
    generate_fixture(&output_root).await
}

async fn generate_fixture(output_root: &Path) -> Result<(), Box<dyn Error>> {
    if output_root.exists() {
        fs::remove_dir_all(output_root)?;
    }
    fs::create_dir_all(output_root)?;
    let table_root = output_root.join("table");
    fs::create_dir_all(&table_root)?;

    let table_url = deltalake::ensure_table_uri(table_root.to_string_lossy())?;
    let table = DeltaTable::try_from_url(table_url.clone()).await?;
    let table = table
        .create()
        .with_columns(table_columns())
        .with_partition_columns(vec!["category"])
        .with_table_name("axon_prod_like_fixture")
        .with_configuration(vec![
            (
                "delta.checkpoint.writeStatsAsJson".to_string(),
                Some("true".to_string()),
            ),
            (
                "delta.checkpoint.writeStatsAsStruct".to_string(),
                Some("false".to_string()),
            ),
        ])
        .await?;

    let table = table
        .write(vec![fixture_batch(
            &[1, 2, 3, 4],
            &["A", "A", "B", "B"],
            &[10, 20, 30, 40],
        )])
        .await?;
    let table = table
        .write(vec![fixture_batch(&[5, 6], &["C", "C"], &[50, 60])])
        .with_save_mode(SaveMode::Append)
        .await?;

    checkpoints::create_checkpoint(&table, None).await?;

    table
        .write(vec![fixture_batch(
            &[7, 8, 9, 10],
            &["B", "B", "D", "D"],
            &[70, 80, 90, 100],
        )])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    let manifest = FixtureManifest {
        name: "Prod-like generated Delta table",
        table_uri: BROWSER_TABLE_URI,
        expected_latest_version: 3,
        checkpoint_version: 2,
        generated_steps: vec![
            GeneratedStep {
                version: 0,
                label: "create table",
                detail: "Create schema and partition metadata for category-partitioned events.",
            },
            GeneratedStep {
                version: 1,
                label: "append A/B",
                detail: "Write initial partitioned Parquet data files.",
            },
            GeneratedStep {
                version: 2,
                label: "append C + checkpoint",
                detail: "Append another partition and write a checkpoint parquet file.",
            },
            GeneratedStep {
                version: 3,
                label: "overwrite B/D",
                detail: "Replay removes old active files and adds the latest B/D data files.",
            },
        ],
        objects: collect_manifest_objects(output_root, &table_root)?,
        data_files: collect_data_files(output_root, &table_root)?,
    };
    let manifest_path = output_root.join("delta-log-manifest.json");
    fs::write(manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(())
}

fn table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "category".to_string(),
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "value".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
    ]
}

fn fixture_batch(ids: &[i32], categories: &[&str], values: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("category", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Int32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(categories.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )
    .expect("fixture batch should be valid")
}

fn collect_manifest_objects(
    output_root: &Path,
    table_root: &Path,
) -> Result<Vec<ManifestObject>, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(&table_root.join("_delta_log"), &mut files)?;
    files.sort();
    Ok(files
        .into_iter()
        .filter_map(|path| {
            let relative_path = relative_to(table_root, &path).ok()?;
            let kind = classify_log_object(&relative_path)?;
            let size_bytes = fs::metadata(&path).ok()?.len();
            Some(ManifestObject {
                url_path: public_url_path(output_root, &path).ok()?,
                relative_path,
                kind,
                size_bytes,
            })
        })
        .collect())
}

fn collect_data_files(
    output_root: &Path,
    table_root: &Path,
) -> Result<Vec<DataFileInventory>, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(table_root, &mut files)?;
    files.sort();
    files
        .into_iter()
        .filter(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension == "parquet")
                && !path.components().any(|component| {
                    matches!(component, Component::Normal(value) if value == "_delta_log")
                })
        })
        .map(|path| {
            let relative_path = relative_to(table_root, &path)?;
            Ok(DataFileInventory {
                url_path: public_url_path(output_root, &path)?,
                size_bytes: fs::metadata(&path)?.len(),
                partition_values: partition_values_from_path(&relative_path),
                relative_path,
            })
        })
        .collect()
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

fn classify_log_object(relative_path: &str) -> Option<ObjectKind> {
    if relative_path == "_delta_log/_last_checkpoint" {
        Some(ObjectKind::LastCheckpoint)
    } else if relative_path.ends_with(".checkpoint.parquet") {
        Some(ObjectKind::CheckpointParquet)
    } else if relative_path.ends_with(".json") {
        Some(ObjectKind::CommitJson)
    } else {
        None
    }
}

fn relative_to(root: &Path, path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(path
        .strip_prefix(root)?
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn public_url_path(output_root: &Path, path: &Path) -> Result<String, Box<dyn Error>> {
    let relative_path = path.strip_prefix(output_root)?;
    Ok(format!(
        "/fixtures/prod-like/{}",
        relative_path
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    ))
}

fn partition_values_from_path(relative_path: &str) -> BTreeMap<String, String> {
    relative_path
        .split('/')
        .filter_map(|component| component.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

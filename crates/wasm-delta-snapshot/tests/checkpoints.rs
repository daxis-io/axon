use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, MapBuilder, MapFieldNames, StringBuilder, StructBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use query_contract::{QueryError, ResolvedFileDescriptor, SnapshotResolutionRequest};
use tempfile::TempDir;
use wasm_delta_snapshot::{
    DefaultJsonHandler, DefaultParquetHandler, LocalFileStorageHandler, SnapshotResolver,
    StorageHandler,
};

#[tokio::test]
async fn prefers_latest_complete_checkpoint_before_json_replay() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[r#"{"add":{"path":"data/a.parquet","size":10,"partitionValues":{}}}"#],
    );
    write_classic_checkpoint(
        fixture.path(),
        0,
        &[CheckpointRow::add("data/a.parquet", 10, BTreeMap::new())],
    );
    write_json_commit(
        fixture.path(),
        1,
        &[r#"{"add":{"path":"data/b.parquet","size":20,"partitionValues":{}}}"#],
    );
    write_multipart_checkpoint_part(
        fixture.path(),
        1,
        1,
        2,
        &[CheckpointRow::add("data/b.parquet", 20, BTreeMap::new())],
    );
    write_last_checkpoint(fixture.path(), 1, Some(2));
    write_json_commit(
        fixture.path(),
        2,
        &[r#"{"remove":{"path":"data/a.parquet"}}"#],
    );
    write_json_commit(
        fixture.path(),
        3,
        &[r#"{"add":{"path":"data/c.parquet","size":30,"partitionValues":{}}}"#],
    );

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: fixture.path().display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect("snapshot should resolve");

    assert_eq!(snapshot.snapshot_version, 3);
    assert_eq!(
        snapshot.active_files,
        vec![
            ResolvedFileDescriptor {
                path: "data/b.parquet".to_string(),
                size_bytes: 20,
                partition_values: BTreeMap::new(),
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::new(),
            },
        ]
    );
}

#[tokio::test]
async fn loads_v2_checkpoint_sidecars_when_present() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[r#"{"add":{"path":"data/a.parquet","size":10,"partitionValues":{"category":"A"}}}"#],
    );
    write_json_commit(
        fixture.path(),
        1,
        &[r#"{"add":{"path":"data/b.parquet","size":20,"partitionValues":{"category":"B"}}}"#],
    );
    write_uuid_checkpoint(
        fixture.path(),
        1,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar("part-00001.parquet")],
    );
    write_sidecar(
        fixture.path(),
        "part-00001.parquet",
        &[
            CheckpointRow::add(
                "data/a.parquet",
                10,
                BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            ),
            CheckpointRow::add(
                "data/b.parquet",
                20,
                BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
            ),
        ],
    );
    write_last_checkpoint(fixture.path(), 1, None);
    write_json_commit(
        fixture.path(),
        2,
        &[r#"{"remove":{"path":"data/a.parquet"}}"#],
    );
    write_json_commit(
        fixture.path(),
        3,
        &[r#"{"add":{"path":"data/c.parquet","size":30,"partitionValues":{"category":"C"}}}"#],
    );

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: fixture.path().display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect("snapshot should resolve");

    assert_eq!(snapshot.snapshot_version, 3);
    assert_eq!(
        snapshot.active_files,
        vec![
            ResolvedFileDescriptor {
                path: "data/b.parquet".to_string(),
                size_bytes: 20,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("B".to_string()),)
                ]),
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("C".to_string()),)
                ]),
            },
        ]
    );
}

#[tokio::test]
async fn loads_v2_json_checkpoint_sidecars_when_present() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[r#"{"add":{"path":"data/a.parquet","size":10,"partitionValues":{"category":"A"}}}"#],
    );
    write_json_commit(
        fixture.path(),
        1,
        &[r#"{"add":{"path":"data/b.parquet","size":20,"partitionValues":{"category":"B"}}}"#],
    );
    write_json_uuid_checkpoint(
        fixture.path(),
        1,
        "11111111-1111-1111-1111-111111111111",
        &[r#"{"sidecar":{"path":"part-00001.parquet"}}"#],
    );
    write_sidecar(
        fixture.path(),
        "part-00001.parquet",
        &[
            CheckpointRow::add(
                "data/a.parquet",
                10,
                BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
            ),
            CheckpointRow::add(
                "data/b.parquet",
                20,
                BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
            ),
        ],
    );
    write_last_checkpoint(fixture.path(), 1, None);
    write_json_commit(
        fixture.path(),
        2,
        &[r#"{"remove":{"path":"data/a.parquet"}}"#],
    );
    write_json_commit(
        fixture.path(),
        3,
        &[r#"{"add":{"path":"data/c.parquet","size":30,"partitionValues":{"category":"C"}}}"#],
    );

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: fixture.path().display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect("snapshot should resolve");

    assert_eq!(snapshot.snapshot_version, 3);
    assert_eq!(
        snapshot.active_files,
        vec![
            ResolvedFileDescriptor {
                path: "data/b.parquet".to_string(),
                size_bytes: 20,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("B".to_string()),)
                ]),
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("C".to_string()),)
                ]),
            },
        ]
    );
}

#[tokio::test]
async fn preserves_absolute_sidecar_paths() {
    let absolute_sidecar_path = "https://example.com/_delta_log/_sidecars/part-00001.parquet";
    let checkpoint_path =
        "_delta_log/00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.json";
    let storage = InMemoryStorageHandler::new(
        vec![checkpoint_path.to_string()],
        BTreeMap::from([
            (
                checkpoint_path.to_string(),
                Bytes::from(format!(
                    r#"{{"sidecar":{{"path":"{absolute_sidecar_path}"}}}}"#
                )),
            ),
            (
                absolute_sidecar_path.to_string(),
                checkpoint_bytes(&[CheckpointRow::add(
                    "data/b.parquet",
                    20,
                    BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                )]),
            ),
        ]),
    );
    let resolver = SnapshotResolver::new(
        storage,
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: "memory://fixture".to_string(),
            snapshot_version: None,
        })
        .await
        .expect("snapshot should resolve");

    assert_eq!(snapshot.snapshot_version, 1);
    assert_eq!(
        snapshot.active_files,
        vec![ResolvedFileDescriptor {
            path: "data/b.parquet".to_string(),
            size_bytes: 20,
            partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()),)]),
        }]
    );
}

#[derive(Clone)]
enum CheckpointRow {
    Add(ResolvedFileDescriptor),
    Sidecar { path: String },
}

impl CheckpointRow {
    fn add(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
    ) -> Self {
        Self::Add(ResolvedFileDescriptor {
            path: path.into(),
            size_bytes,
            partition_values,
        })
    }

    fn sidecar(path: impl Into<String>) -> Self {
        Self::Sidecar { path: path.into() }
    }
}

fn write_json_commit(root: &Path, version: u64, lines: &[&str]) {
    let path = delta_log_dir(root).join(format!("{version:020}.json"));
    fs::write(path, lines.join("\n")).expect("json commit should be written");
}

fn write_last_checkpoint(root: &Path, version: u64, parts: Option<u32>) {
    let mut fields = vec![format!(r#""version":{version}"#), r#""size":1"#.to_string()];
    if let Some(parts) = parts {
        fields.push(format!(r#""parts":{parts}"#));
    }
    let path = delta_log_dir(root).join("_last_checkpoint");
    fs::write(path, format!("{{{}}}", fields.join(","))).expect("_last_checkpoint should write");
}

fn write_classic_checkpoint(root: &Path, version: u64, rows: &[CheckpointRow]) {
    write_checkpoint_file(
        &delta_log_dir(root).join(format!("{version:020}.checkpoint.parquet")),
        rows,
    );
}

fn write_uuid_checkpoint(root: &Path, version: u64, uuid: &str, rows: &[CheckpointRow]) {
    write_checkpoint_file(
        &delta_log_dir(root).join(format!("{version:020}.checkpoint.{uuid}.parquet")),
        rows,
    );
}

fn write_json_uuid_checkpoint(root: &Path, version: u64, uuid: &str, lines: &[&str]) {
    let path = delta_log_dir(root).join(format!("{version:020}.checkpoint.{uuid}.json"));
    fs::write(path, lines.join("\n")).expect("json checkpoint should be written");
}

fn write_multipart_checkpoint_part(
    root: &Path,
    version: u64,
    part: u32,
    parts: u32,
    rows: &[CheckpointRow],
) {
    write_checkpoint_file(
        &delta_log_dir(root).join(format!(
            "{version:020}.checkpoint.{part:010}.{parts:010}.parquet"
        )),
        rows,
    );
}

fn write_sidecar(root: &Path, filename: &str, rows: &[CheckpointRow]) {
    let path = delta_log_dir(root).join("_sidecars").join(filename);
    fs::create_dir_all(path.parent().expect("sidecar parent should exist"))
        .expect("sidecar dir should be created");
    write_checkpoint_file(&path, rows);
}

fn delta_log_dir(root: &Path) -> std::path::PathBuf {
    let path = root.join("_delta_log");
    fs::create_dir_all(&path).expect("delta log dir should exist");
    path
}

fn write_checkpoint_file(path: &Path, rows: &[CheckpointRow]) {
    let file = fs::File::create(path).expect("checkpoint file should be created");
    let mut writer = ArrowWriter::try_new(file, checkpoint_record_batch(rows).schema(), None)
        .expect("writer should be created");
    writer
        .write(&checkpoint_record_batch(rows))
        .expect("batch should write");
    writer.close().expect("writer should close");
}

fn checkpoint_bytes(rows: &[CheckpointRow]) -> Bytes {
    let batch = checkpoint_record_batch(rows);
    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, batch.schema(), None).expect("writer should be created");
    writer.write(&batch).expect("batch should write");
    writer.close().expect("writer should close");
    Bytes::from(bytes)
}

fn checkpoint_record_batch(rows: &[CheckpointRow]) -> RecordBatch {
    let add_fields = Fields::from(vec![
        Field::new("path", DataType::Utf8, true),
        Field::new("size", DataType::Int64, true),
        Field::new(
            "partitionValues",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]);
    let remove_fields = Fields::from(vec![Field::new("path", DataType::Utf8, true)]);
    let sidecar_fields = Fields::from(vec![Field::new("path", DataType::Utf8, true)]);

    let mut add_builder = StructBuilder::new(
        add_fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                StringBuilder::new(),
                StringBuilder::new(),
            )),
        ],
    );
    let mut remove_builder =
        StructBuilder::new(remove_fields.clone(), vec![Box::new(StringBuilder::new())]);
    let mut sidecar_builder =
        StructBuilder::new(sidecar_fields.clone(), vec![Box::new(StringBuilder::new())]);

    for row in rows {
        match row {
            CheckpointRow::Add(file) => {
                add_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("path builder")
                    .append_value(&file.path);
                add_builder
                    .field_builder::<Int64Builder>(1)
                    .expect("size builder")
                    .append_value(i64::try_from(file.size_bytes).expect("size should fit in i64"));
                let map_builder = add_builder
                    .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(2)
                    .expect("partition map builder");
                for (key, value) in &file.partition_values {
                    map_builder.keys().append_value(key);
                    match value {
                        Some(value) => map_builder.values().append_value(value),
                        None => map_builder.values().append_null(),
                    }
                }
                map_builder
                    .append(true)
                    .expect("partition map entry should append");
                add_builder.append(true);

                remove_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("remove path builder")
                    .append_null();
                remove_builder.append(false);

                sidecar_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("sidecar path builder")
                    .append_null();
                sidecar_builder.append(false);
            }
            CheckpointRow::Sidecar { path } => {
                add_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("path builder")
                    .append_null();
                add_builder
                    .field_builder::<Int64Builder>(1)
                    .expect("size builder")
                    .append_null();
                add_builder
                    .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(2)
                    .expect("partition map builder")
                    .append(false)
                    .expect("null partition map should append");
                add_builder.append(false);

                remove_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("remove path builder")
                    .append_null();
                remove_builder.append(false);

                sidecar_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("sidecar path builder")
                    .append_value(path);
                sidecar_builder.append(true);
            }
        }
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields), true),
            Field::new("remove", DataType::Struct(remove_fields), true),
            Field::new("sidecar", DataType::Struct(sidecar_fields), true),
        ])),
        vec![
            Arc::new(add_builder.finish()),
            Arc::new(remove_builder.finish()),
            Arc::new(sidecar_builder.finish()),
        ],
    )
    .expect("record batch should be created")
}

#[derive(Clone)]
struct InMemoryStorageHandler {
    listed_paths: Vec<String>,
    bytes_by_path: BTreeMap<String, Bytes>,
}

impl InMemoryStorageHandler {
    fn new(listed_paths: Vec<String>, bytes_by_path: BTreeMap<String, Bytes>) -> Self {
        Self {
            listed_paths,
            bytes_by_path,
        }
    }
}

#[async_trait(?Send)]
impl StorageHandler for InMemoryStorageHandler {
    async fn list_paths(&self, _table_uri: &str, prefix: &str) -> Result<Vec<String>, QueryError> {
        Ok(self
            .listed_paths
            .iter()
            .filter(|path| path.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn read_bytes(
        &self,
        _table_uri: &str,
        relative_path: &str,
    ) -> Result<Option<Bytes>, QueryError> {
        Ok(self.bytes_by_path.get(relative_path).cloned())
    }
}

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::builder::{Int64Builder, MapBuilder, MapFieldNames, StringBuilder, StructBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use query_contract::{
    QueryError, QueryErrorCode, ResolvedFileDescriptor, SnapshotResolutionRequest,
};
use serde_json::json;
use tempfile::TempDir;
use wasm_delta_snapshot::{
    BrowserDeltaLogManifest, BrowserDeltaLogObject, BrowserHttpDeltaLogStorageHandler,
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
                stats: None,
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::new(),
                stats: None,
            },
        ]
    );
}

#[tokio::test]
async fn snapshot_prefers_latest_complete_checkpoint_and_sidecars() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let checkpoint_stats =
        r#"{"numRecords":2,"minValues":{"id":2},"maxValues":{"id":5},"nullCount":{"id":0}}"#;
    let replay_stats =
        r#"{"numRecords":1,"minValues":{"id":9},"maxValues":{"id":9},"nullCount":{"id":0}}"#;
    write_json_commit(
        fixture.path(),
        0,
        &[r#"{"add":{"path":"data/a.parquet","size":10,"partitionValues":{"category":"A"}}}"#],
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
        &[CheckpointRow::add_with_stats(
            "data/b.parquet",
            20,
            BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
            checkpoint_stats,
        )],
    );
    write_multipart_checkpoint_part(
        fixture.path(),
        2,
        1,
        2,
        &[CheckpointRow::add(
            "data/ignored.parquet",
            999,
            BTreeMap::new(),
        )],
    );
    write_last_checkpoint(fixture.path(), 2, Some(2));
    write_json_commit(
        fixture.path(),
        2,
        &[r#"{"remove":{"path":"data/a.parquet"}}"#],
    );
    write_json_commit(
        fixture.path(),
        3,
        &[&format!(
            r#"{{"add":{{"path":"data/c.parquet","size":30,"partitionValues":{{"category":"C"}},"stats":{replay_stats:?}}}}}"#
        )],
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
    assert_active_file_facts_match(
        &snapshot.active_files,
        &[
            ResolvedFileDescriptor {
                path: "data/b.parquet".to_string(),
                size_bytes: 20,
                partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                stats: None,
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::from([("category".to_string(), Some("C".to_string()))]),
                stats: None,
            },
        ],
    );

    let snapshot_json = serde_json::to_value(&snapshot).expect("snapshot should serialize");
    assert_eq!(
        snapshot_json["active_files"][0]["stats"],
        json!(checkpoint_stats)
    );
    assert_eq!(
        snapshot_json["active_files"][1]["stats"],
        json!(replay_stats)
    );
}

#[tokio::test]
async fn browser_http_storage_loads_v2_checkpoint_sidecars_and_replays_json() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[r#"{"add":{"path":"data/a.parquet","size":10,"partitionValues":{"category":"A"}}}"#],
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
        &[CheckpointRow::add(
            "data/b.parquet",
            20,
            BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
        )],
    );
    write_last_checkpoint(fixture.path(), 1, None);
    write_json_commit(
        fixture.path(),
        2,
        &[r#"{"add":{"path":"data/c.parquet","size":30,"partitionValues":{"category":"C"}}}"#],
    );

    let server = StaticDirectoryHttpServer::serve(fixture.path());
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            server.object("_delta_log/00000000000000000000.json"),
            server.object(
                "_delta_log/00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.parquet",
            ),
            server.object("_delta_log/_sidecars/part-00001.parquet"),
            server.object("_delta_log/_last_checkpoint"),
            server.object("_delta_log/00000000000000000002.json"),
        ],
    )
    .expect("manifest should validate");

    let resolver = SnapshotResolver::new(
        BrowserHttpDeltaLogStorageHandler::new(manifest),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );
    let snapshot = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: "gs://bucket/table".to_string(),
            snapshot_version: None,
        })
        .await
        .expect("HTTP checkpoint should resolve");

    assert_eq!(snapshot.snapshot_version, 2);
    assert_eq!(
        snapshot
            .active_files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec!["data/b.parquet", "data/c.parquet"]
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
                stats: None,
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("C".to_string()),)
                ]),
                stats: None,
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
                stats: None,
            },
            ResolvedFileDescriptor {
                path: "data/c.parquet".to_string(),
                size_bytes: 30,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("C".to_string()),)
                ]),
                stats: None,
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
            stats: None,
        }]
    );
}

#[tokio::test]
async fn rejects_native_absolute_sidecar_paths_that_escape_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    fs::create_dir_all(delta_log_dir(&table_root).join("_sidecars"))
        .expect("sidecar directory should be created");

    let escaped_sidecar = fixture.path().join("escape.parquet");
    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar(
            escaped_sidecar.display().to_string(),
        )],
    );
    write_last_checkpoint(&table_root, 0, None);
    write_checkpoint_file(
        &remapped_native_absolute_sidecar_path(&table_root, &escaped_sidecar),
        &[CheckpointRow::add(
            "data/escape.parquet",
            10,
            BTreeMap::new(),
        )],
    );

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("native absolute sidecar paths should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("table root"),
        "unexpected error message: {}",
        error.message
    );
}

#[tokio::test]
async fn rejects_sidecar_file_uri_paths_that_escape_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    fs::create_dir_all(delta_log_dir(&table_root).join("_sidecars"))
        .expect("sidecar directory should be created");

    let escaped_sidecar = fixture.path().join("escape.parquet");
    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar(format!(
            "file://{}",
            escaped_sidecar.display()
        ))],
    );
    write_last_checkpoint(&table_root, 0, None);
    write_checkpoint_file(
        &escaped_sidecar,
        &[CheckpointRow::add(
            "data/escape.parquet",
            10,
            BTreeMap::new(),
        )],
    );

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("file URI sidecar paths should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("table root"),
        "unexpected error message: {}",
        error.message
    );
}

#[tokio::test]
async fn rejects_windows_drive_sidecar_paths_that_escape_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    fs::create_dir_all(delta_log_dir(&table_root).join("_sidecars"))
        .expect("sidecar directory should be created");

    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar(r"C:\escape.parquet")],
    );
    write_last_checkpoint(&table_root, 0, None);

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("windows drive sidecar paths should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("table root"),
        "unexpected error message: {}",
        error.message
    );
}

#[tokio::test]
async fn rejects_unc_sidecar_paths_that_escape_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    fs::create_dir_all(delta_log_dir(&table_root).join("_sidecars"))
        .expect("sidecar directory should be created");

    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar(r"\\server\share\escape.parquet")],
    );
    write_last_checkpoint(&table_root, 0, None);

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("UNC sidecar paths should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("table root"),
        "unexpected error message: {}",
        error.message
    );
}

#[tokio::test]
async fn rejects_sidecar_paths_that_escape_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    fs::create_dir_all(&table_root).expect("table root should be created");
    fs::create_dir_all(delta_log_dir(&table_root).join("_sidecars"))
        .expect("sidecar directory should be created");

    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar("../../../escape.parquet")],
    );
    write_last_checkpoint(&table_root, 0, None);
    write_checkpoint_file(
        &fixture.path().join("escape.parquet"),
        &[CheckpointRow::add(
            "data/escape.parquet",
            10,
            BTreeMap::new(),
        )],
    );

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("sidecar traversal should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("sidecar") || error.message.contains("table root"),
        "unexpected error message: {}",
        error.message
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_sidecar_symlinks_within_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    let sidecar_dir = delta_log_dir(&table_root).join("_sidecars");
    fs::create_dir_all(&sidecar_dir).expect("sidecar directory should be created");

    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar("link.parquet")],
    );
    write_last_checkpoint(&table_root, 0, None);
    write_sidecar(
        &table_root,
        "real.parquet",
        &[CheckpointRow::add("data/real.parquet", 10, BTreeMap::new())],
    );
    symlink(
        sidecar_dir.join("real.parquet"),
        sidecar_dir.join("link.parquet"),
    )
    .expect("sidecar symlink should be created");

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("symlinked sidecar paths should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("table root") || error.message.contains("symlink"),
        "unexpected error message: {}",
        error.message
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_sidecar_symlinks_that_escape_the_table_root() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let table_root = fixture.path().join("table");
    let sidecar_dir = delta_log_dir(&table_root).join("_sidecars");
    fs::create_dir_all(&sidecar_dir).expect("sidecar directory should be created");

    write_uuid_checkpoint(
        &table_root,
        0,
        "11111111-1111-1111-1111-111111111111",
        &[CheckpointRow::sidecar("link.parquet")],
    );
    write_last_checkpoint(&table_root, 0, None);

    let escaped_sidecar = fixture.path().join("escape.parquet");
    write_checkpoint_file(
        &escaped_sidecar,
        &[CheckpointRow::add(
            "data/escape.parquet",
            10,
            BTreeMap::new(),
        )],
    );
    symlink(&escaped_sidecar, sidecar_dir.join("link.parquet"))
        .expect("sidecar symlink should be created");

    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let error = resolver
        .resolve_snapshot(SnapshotResolutionRequest {
            table_uri: table_root.display().to_string(),
            snapshot_version: None,
        })
        .await
        .expect_err("sidecar symlink traversal should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(
        error.message.contains("table root"),
        "unexpected error message: {}",
        error.message
    );
}

#[derive(Clone)]
enum CheckpointRow {
    Add {
        file: ResolvedFileDescriptor,
        stats: Option<String>,
    },
    Sidecar {
        path: String,
    },
}

impl CheckpointRow {
    fn add(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
    ) -> Self {
        Self::Add {
            file: ResolvedFileDescriptor {
                path: path.into(),
                size_bytes,
                partition_values,
                stats: None,
            },
            stats: None,
        }
    }

    fn add_with_stats(
        path: impl Into<String>,
        size_bytes: u64,
        partition_values: BTreeMap<String, Option<String>>,
        stats: impl Into<String>,
    ) -> Self {
        Self::Add {
            file: ResolvedFileDescriptor {
                path: path.into(),
                size_bytes,
                partition_values,
                stats: None,
            },
            stats: Some(stats.into()),
        }
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

fn remapped_native_absolute_sidecar_path(root: &Path, absolute_path: &Path) -> PathBuf {
    let mut path = delta_log_dir(root).join("_sidecars");
    for component in absolute_path.components() {
        if let Component::Normal(part) = component {
            path.push(part);
        }
    }
    fs::create_dir_all(path.parent().expect("sidecar parent should exist"))
        .expect("sidecar dir should be created");
    path
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
        Field::new("stats", DataType::Utf8, true),
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
            Box::new(StringBuilder::new()),
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
            CheckpointRow::Add { file, stats } => {
                add_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("path builder")
                    .append_value(&file.path);
                add_builder
                    .field_builder::<Int64Builder>(1)
                    .expect("size builder")
                    .append_value(i64::try_from(file.size_bytes).expect("size should fit in i64"));
                match stats {
                    Some(stats) => add_builder
                        .field_builder::<StringBuilder>(2)
                        .expect("stats builder")
                        .append_value(stats),
                    None => add_builder
                        .field_builder::<StringBuilder>(2)
                        .expect("stats builder")
                        .append_null(),
                }
                let map_builder = add_builder
                    .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(3)
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
                    .field_builder::<StringBuilder>(2)
                    .expect("stats builder")
                    .append_null();
                add_builder
                    .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(3)
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

struct StaticDirectoryHttpServer {
    address: String,
    stop_tx: Option<mpsc::Sender<()>>,
    server: Option<JoinHandle<()>>,
}

impl StaticDirectoryHttpServer {
    fn serve(root: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
        listener
            .set_nonblocking(true)
            .expect("test listener should become nonblocking");
        let address = listener.local_addr().expect("listener addr should resolve");
        let root = root.to_path_buf();
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || serve_directory_requests(listener, root, stop_rx));

        Self {
            address: address.to_string(),
            stop_tx: Some(stop_tx),
            server: Some(server),
        }
    }

    fn object(&self, relative_path: &str) -> BrowserDeltaLogObject {
        BrowserDeltaLogObject::new(relative_path, self.url(relative_path))
    }

    fn url(&self, relative_path: &str) -> String {
        format!("http://{}/{}", self.address, relative_path)
    }
}

impl Drop for StaticDirectoryHttpServer {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(server) = self.server.take() {
            server.join().expect("test server should shut down cleanly");
        }
    }
}

fn serve_directory_requests(listener: TcpListener, root: PathBuf, stop_rx: mpsc::Receiver<()>) {
    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("accepted test stream should become blocking");
                let request_path = read_request_path(&mut stream);
                let Some(path) = confined_http_path(&root, &request_path) else {
                    write_response(&mut stream, "404 Not Found", b"");
                    continue;
                };
                match fs::read(path) {
                    Ok(bytes) => write_response(&mut stream, "200 OK", &bytes),
                    Err(_) => write_response(&mut stream, "404 Not Found", b""),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
}

fn confined_http_path(root: &Path, request_path: &str) -> Option<PathBuf> {
    let relative_path = request_path
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(request_path)
        .strip_prefix('/')?;
    if relative_path.is_empty()
        || Path::new(relative_path).components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return None;
    }
    Some(root.join(relative_path))
}

fn read_request_path(stream: &mut std::net::TcpStream) -> String {
    let mut buffer = [0_u8; 4096];
    let mut request = Vec::new();
    loop {
        let bytes_read = stream
            .read(&mut buffer)
            .expect("request bytes should be readable");
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let text = String::from_utf8(request).expect("request should be valid ASCII");
    let request_line = text.lines().next().expect("request line should exist");
    request_line
        .split_whitespace()
        .nth(1)
        .expect("request path should exist")
        .to_string()
}

fn write_response(stream: &mut std::net::TcpStream, status_line: &str, body: &[u8]) {
    let response = format!(
        "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .expect("response headers should be written");
    stream
        .write_all(body)
        .expect("response body should be written");
}

fn assert_active_file_facts_match(
    actual: &[ResolvedFileDescriptor],
    expected: &[ResolvedFileDescriptor],
) {
    let normalize = |file: &ResolvedFileDescriptor| {
        (
            file.path.clone(),
            file.size_bytes,
            file.partition_values.clone(),
        )
    };

    assert_eq!(
        actual.iter().map(normalize).collect::<Vec<_>>(),
        expected.iter().map(normalize).collect::<Vec<_>>()
    );
}

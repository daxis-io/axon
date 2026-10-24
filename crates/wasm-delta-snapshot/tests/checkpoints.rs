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

use arrow_array::builder::{
    Int32Builder, Int64Builder, MapBuilder, MapFieldNames, StringBuilder, StructBuilder,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use query_contract::{
    CapabilityKey, CapabilityState, QueryError, QueryErrorCode, ResolvedFileDescriptor,
    ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
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

#[tokio::test]
async fn checkpoint_tombstones_do_not_drop_live_deletion_vector_files() {
    // Spark/Databricks checkpoints sort file actions by path with the live add before the
    // same-path tombstones left by earlier deletion-vector updates.
    let fixture = TempDir::new().expect("tempdir should be created");
    write_classic_checkpoint(
        fixture.path(),
        1,
        &[
            CheckpointRow::add_with_deletion_vector(
                "data/p.parquet",
                10,
                TestDeletionVector::uuid("dvB"),
            ),
            CheckpointRow::remove("data/p.parquet", None),
            CheckpointRow::remove("data/p.parquet", Some(TestDeletionVector::uuid("dvA"))),
            CheckpointRow::add("data/q.parquet", 20, BTreeMap::new()),
            CheckpointRow::remove("data/expired.parquet", None),
        ],
    );
    write_last_checkpoint(fixture.path(), 1, None);

    let snapshot = resolve_local_snapshot(fixture.path())
        .await
        .expect("snapshot should resolve");

    assert_eq!(
        active_paths(&snapshot),
        vec!["data/p.parquet", "data/q.parquet"]
    );
    assert_eq!(
        snapshot
            .required_capabilities
            .state(CapabilityKey::DeletionVectors),
        Some(CapabilityState::NativeOnly)
    );
}

#[tokio::test]
async fn v2_checkpoint_tombstones_keep_the_deletion_vector_gate_when_the_flag_is_off() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let live_dv = TestDeletionVector::uuid("dvB");
    let replaced_dv = TestDeletionVector::uuid("dvA");
    write_json_uuid_checkpoint(
        fixture.path(),
        20,
        "11111111-1111-1111-1111-111111111111",
        &[
            r#"{"checkpointMetadata":{"version":20}}"#,
            &deletion_vector_protocol_json(),
            &table_metadata_json("false"),
            &add_json("data/p.parquet", 10, Some(&live_dv), None),
            &remove_json("data/p.parquet", None),
            &remove_json("data/p.parquet", Some(&replaced_dv)),
        ],
    );
    write_last_checkpoint(fixture.path(), 20, None);

    let snapshot = resolve_local_snapshot(fixture.path())
        .await
        .expect("snapshot should resolve");

    assert_eq!(active_paths(&snapshot), vec!["data/p.parquet"]);
    assert_eq!(
        snapshot
            .required_capabilities
            .state(CapabilityKey::DeletionVectors),
        Some(CapabilityState::NativeOnly)
    );
}

#[tokio::test]
async fn sidecar_tombstones_do_not_drop_live_deletion_vector_files() {
    let fixture = TempDir::new().expect("tempdir should be created");
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
            CheckpointRow::add_with_deletion_vector(
                "data/p.parquet",
                10,
                TestDeletionVector::uuid("dvB"),
            ),
            CheckpointRow::remove("data/p.parquet", None),
            CheckpointRow::remove("data/p.parquet", Some(TestDeletionVector::uuid("dvA"))),
        ],
    );
    write_last_checkpoint(fixture.path(), 1, None);

    let snapshot = resolve_local_snapshot(fixture.path())
        .await
        .expect("snapshot should resolve");

    assert_eq!(active_paths(&snapshot), vec!["data/p.parquet"]);
    assert_eq!(
        snapshot
            .required_capabilities
            .state(CapabilityKey::DeletionVectors),
        Some(CapabilityState::NativeOnly)
    );
}

#[tokio::test]
async fn commit_replay_tracks_deletion_vector_updates_by_logical_file() {
    // A commit is a set of actions: the new logical file may be listed before the removal of
    // the one it replaces.
    let fixture = TempDir::new().expect("tempdir should be created");
    let first_dv = TestDeletionVector::uuid("dvA");
    let second_dv = TestDeletionVector::uuid("dvB");
    write_json_commit(
        fixture.path(),
        0,
        &[
            &deletion_vector_protocol_json(),
            &table_metadata_json("false"),
            &add_json("data/p.parquet", 10, None, Some(r#"{"numRecords":3}"#)),
            &add_json("data/q.parquet", 20, None, None),
        ],
    );
    write_json_commit(
        fixture.path(),
        1,
        &[
            &add_json(
                "data/p.parquet",
                10,
                Some(&first_dv),
                Some(r#"{"numRecords":3}"#),
            ),
            &remove_json("data/p.parquet", None),
        ],
    );
    write_json_commit(
        fixture.path(),
        2,
        &[
            &add_json(
                "data/p.parquet",
                10,
                Some(&second_dv),
                Some(r#"{"numRecords":3,"tightBounds":false}"#),
            ),
            &remove_json("data/p.parquet", Some(&first_dv)),
        ],
    );

    let snapshot = resolve_local_snapshot(fixture.path())
        .await
        .expect("snapshot should resolve");

    assert_eq!(
        active_paths(&snapshot),
        vec!["data/p.parquet", "data/q.parquet"]
    );
    assert_eq!(
        snapshot.active_files[0].stats.as_deref(),
        Some(r#"{"numRecords":3,"tightBounds":false}"#)
    );
    assert_eq!(
        snapshot
            .required_capabilities
            .state(CapabilityKey::DeletionVectors),
        Some(CapabilityState::NativeOnly)
    );
}

#[tokio::test]
async fn checkpoint_and_commit_deletion_vector_ids_identify_the_same_logical_file() {
    let fixture = TempDir::new().expect("tempdir should be created");
    let checkpoint_dv = TestDeletionVector::uuid("dvB");
    let inline_dv = TestDeletionVector::inline("inlineR");
    write_classic_checkpoint(
        fixture.path(),
        1,
        &[
            CheckpointRow::add_with_deletion_vector("data/p.parquet", 10, checkpoint_dv.clone()),
            CheckpointRow::add_with_deletion_vector("data/r.parquet", 30, inline_dv.clone()),
        ],
    );
    write_last_checkpoint(fixture.path(), 1, None);
    write_json_commit(
        fixture.path(),
        2,
        &[
            &add_json(
                "data/p.parquet",
                10,
                Some(&TestDeletionVector::uuid("dvC")),
                None,
            ),
            &remove_json("data/p.parquet", Some(&checkpoint_dv)),
            &remove_json("data/r.parquet", Some(&inline_dv)),
        ],
    );

    let snapshot = resolve_local_snapshot(fixture.path())
        .await
        .expect("snapshot should resolve");

    assert_eq!(active_paths(&snapshot), vec!["data/p.parquet"]);
    assert_eq!(
        snapshot
            .required_capabilities
            .state(CapabilityKey::DeletionVectors),
        Some(CapabilityState::NativeOnly)
    );
}

#[tokio::test]
async fn rejects_replay_that_leaves_two_live_files_for_one_path() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[&add_json("data/p.parquet", 10, None, None)],
    );
    write_json_commit(
        fixture.path(),
        1,
        &[&add_json(
            "data/p.parquet",
            10,
            Some(&TestDeletionVector::uuid("dvA")),
            None,
        )],
    );

    let error = resolve_local_snapshot(fixture.path())
        .await
        .expect_err("two live logical files for one path should not resolve");

    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert!(
        error.message.contains("data/p.parquet"),
        "unexpected error message: {}",
        error.message
    );
}

#[tokio::test]
async fn rejects_deletion_vectors_without_a_storage_identity() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_json_commit(
        fixture.path(),
        0,
        &[
            r#"{"add":{"path":"data/p.parquet","size":10,"partitionValues":{},"deletionVector":{"cardinality":2}}}"#,
        ],
    );

    let error = resolve_local_snapshot(fixture.path())
        .await
        .expect_err("a deletion vector without storageType/pathOrInlineDv should not resolve");

    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
}

#[tokio::test]
async fn multipart_checkpoint_ignores_a_leftover_set_with_more_parts() {
    // A failed checkpoint attempt can leave parts of a larger set beside the complete set later
    // written at the same version. Parts of the two sets must never be combined.
    let fixture = TempDir::new().expect("tempdir should be created");
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        2,
        &[(1, "data/a.parquet"), (2, "data/b.parquet")],
    );
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        3,
        &[(1, "data/x.parquet"), (3, "data/y.parquet")],
    );

    for parts in [None, Some(2)] {
        write_last_checkpoint(fixture.path(), 1, parts);
        let snapshot = resolve_local_snapshot(fixture.path())
            .await
            .expect("snapshot should resolve");

        assert_eq!(
            active_paths(&snapshot),
            vec!["data/a.parquet", "data/b.parquet"],
            "_last_checkpoint parts {parts:?}"
        );
    }
}

#[tokio::test]
async fn multipart_checkpoint_ignores_a_leftover_first_part_of_a_larger_set() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        3,
        &[
            (1, "data/a.parquet"),
            (2, "data/b.parquet"),
            (3, "data/c.parquet"),
        ],
    );
    write_multipart_checkpoint_adds(fixture.path(), 1, 4, &[(1, "data/x.parquet")]);

    for parts in [None, Some(3), Some(4)] {
        write_last_checkpoint(fixture.path(), 1, parts);
        let snapshot = resolve_local_snapshot(fixture.path())
            .await
            .expect("snapshot should resolve");

        assert_eq!(
            active_paths(&snapshot),
            vec!["data/a.parquet", "data/b.parquet", "data/c.parquet"],
            "_last_checkpoint parts {parts:?}"
        );
    }
}

#[tokio::test]
async fn multipart_checkpoint_keeps_a_complete_set_beside_an_incomplete_larger_one() {
    // Without the complete set the version would have no checkpoint, and replay would need commit
    // files that log cleanup already removed.
    let fixture = TempDir::new().expect("tempdir should be created");
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        2,
        &[(1, "data/a.parquet"), (2, "data/b.parquet")],
    );
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        3,
        &[(1, "data/x.parquet"), (2, "data/y.parquet")],
    );

    let snapshot = resolve_local_snapshot(fixture.path())
        .await
        .expect("snapshot should resolve");

    assert_eq!(
        active_paths(&snapshot),
        vec!["data/a.parquet", "data/b.parquet"]
    );
}

#[tokio::test]
async fn multipart_checkpoint_selection_follows_the_hint_between_complete_sets() {
    let fixture = TempDir::new().expect("tempdir should be created");
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        2,
        &[(1, "data/a.parquet"), (2, "data/b.parquet")],
    );
    write_multipart_checkpoint_adds(
        fixture.path(),
        1,
        3,
        &[
            (1, "data/c.parquet"),
            (2, "data/d.parquet"),
            (3, "data/e.parquet"),
        ],
    );

    // A hint whose part count matches no complete set falls back to the same deterministic choice
    // as no hint: the set with more parts.
    for (parts, expected) in [
        (Some(2), vec!["data/a.parquet", "data/b.parquet"]),
        (
            Some(3),
            vec!["data/c.parquet", "data/d.parquet", "data/e.parquet"],
        ),
        (
            Some(5),
            vec!["data/c.parquet", "data/d.parquet", "data/e.parquet"],
        ),
        (
            None,
            vec!["data/c.parquet", "data/d.parquet", "data/e.parquet"],
        ),
    ] {
        write_last_checkpoint(fixture.path(), 1, parts);
        let snapshot = resolve_local_snapshot(fixture.path())
            .await
            .expect("snapshot should resolve");

        assert_eq!(
            active_paths(&snapshot),
            expected,
            "_last_checkpoint parts {parts:?}"
        );
    }
}

async fn resolve_local_snapshot(root: &Path) -> Result<ResolvedSnapshotDescriptor, QueryError> {
    SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    )
    .resolve_snapshot(SnapshotResolutionRequest {
        table_uri: root.display().to_string(),
        snapshot_version: None,
    })
    .await
}

fn active_paths(snapshot: &ResolvedSnapshotDescriptor) -> Vec<&str> {
    snapshot
        .active_files
        .iter()
        .map(|file| file.path.as_str())
        .collect()
}

fn deletion_vector_protocol_json() -> String {
    r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#.to_string()
}

fn table_metadata_json(deletion_vectors_enabled: &str) -> String {
    let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"#;
    format!(
        r#"{{"metaData":{{"id":"dv-table","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema}","partitionColumns":[],"configuration":{{"delta.enableDeletionVectors":"{deletion_vectors_enabled}"}}}}}}"#
    )
}

fn add_json(
    path: &str,
    size: u64,
    deletion_vector: Option<&TestDeletionVector>,
    stats: Option<&str>,
) -> String {
    let mut fields = vec![
        format!(r#""path":"{path}""#),
        format!(r#""size":{size}"#),
        r#""partitionValues":{}"#.to_string(),
        r#""dataChange":true"#.to_string(),
    ];
    if let Some(stats) = stats {
        fields.push(format!(r#""stats":{}"#, json!(stats)));
    }
    if let Some(deletion_vector) = deletion_vector {
        fields.push(format!(r#""deletionVector":{}"#, deletion_vector.json()));
    }
    format!(r#"{{"add":{{{}}}}}"#, fields.join(","))
}

fn remove_json(path: &str, deletion_vector: Option<&TestDeletionVector>) -> String {
    let mut fields = vec![
        format!(r#""path":"{path}""#),
        r#""deletionTimestamp":1"#.to_string(),
        r#""dataChange":true"#.to_string(),
    ];
    if let Some(deletion_vector) = deletion_vector {
        fields.push(format!(r#""deletionVector":{}"#, deletion_vector.json()));
    }
    format!(r#"{{"remove":{{{}}}}}"#, fields.join(","))
}

#[derive(Clone)]
enum CheckpointRow {
    Add {
        file: ResolvedFileDescriptor,
        stats: Option<String>,
        deletion_vector: Option<TestDeletionVector>,
    },
    Remove {
        path: String,
        deletion_vector: Option<TestDeletionVector>,
    },
    Sidecar {
        path: String,
    },
}

#[derive(Clone)]
struct TestDeletionVector {
    storage_type: &'static str,
    path_or_inline_dv: &'static str,
    offset: Option<i32>,
}

impl TestDeletionVector {
    fn uuid(path_or_inline_dv: &'static str) -> Self {
        Self {
            storage_type: "u",
            path_or_inline_dv,
            offset: Some(1),
        }
    }

    fn inline(path_or_inline_dv: &'static str) -> Self {
        Self {
            storage_type: "i",
            path_or_inline_dv,
            offset: None,
        }
    }

    fn json(&self) -> String {
        match self.offset {
            Some(offset) => format!(
                r#"{{"storageType":"{}","pathOrInlineDv":"{}","offset":{offset},"sizeInBytes":36,"cardinality":2}}"#,
                self.storage_type, self.path_or_inline_dv
            ),
            None => format!(
                r#"{{"storageType":"{}","pathOrInlineDv":"{}","sizeInBytes":36,"cardinality":2}}"#,
                self.storage_type, self.path_or_inline_dv
            ),
        }
    }
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
            deletion_vector: None,
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
            deletion_vector: None,
        }
    }

    fn add_with_deletion_vector(
        path: impl Into<String>,
        size_bytes: u64,
        deletion_vector: TestDeletionVector,
    ) -> Self {
        Self::Add {
            file: ResolvedFileDescriptor {
                path: path.into(),
                size_bytes,
                partition_values: BTreeMap::new(),
                stats: None,
            },
            stats: None,
            deletion_vector: Some(deletion_vector),
        }
    }

    fn remove(path: impl Into<String>, deletion_vector: Option<TestDeletionVector>) -> Self {
        Self::Remove {
            path: path.into(),
            deletion_vector,
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

fn write_multipart_checkpoint_adds(root: &Path, version: u64, parts: u32, adds: &[(u32, &str)]) {
    for (part, path) in adds {
        write_multipart_checkpoint_part(
            root,
            version,
            *part,
            parts,
            &[CheckpointRow::add(*path, 10, BTreeMap::new())],
        );
    }
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
    let deletion_vector_fields = Fields::from(vec![
        Field::new("storageType", DataType::Utf8, true),
        Field::new("pathOrInlineDv", DataType::Utf8, true),
        Field::new("offset", DataType::Int32, true),
        Field::new("sizeInBytes", DataType::Int32, true),
        Field::new("cardinality", DataType::Int64, true),
    ]);
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
        Field::new(
            "deletionVector",
            DataType::Struct(deletion_vector_fields.clone()),
            true,
        ),
    ]);
    let remove_fields = Fields::from(vec![
        Field::new("path", DataType::Utf8, true),
        Field::new(
            "deletionVector",
            DataType::Struct(deletion_vector_fields.clone()),
            true,
        ),
    ]);
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
            Box::new(StructBuilder::from_fields(
                deletion_vector_fields.clone(),
                0,
            )),
        ],
    );
    let mut remove_builder = StructBuilder::new(
        remove_fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(StructBuilder::from_fields(deletion_vector_fields, 0)),
        ],
    );
    let mut sidecar_builder =
        StructBuilder::new(sidecar_fields.clone(), vec![Box::new(StringBuilder::new())]);

    for row in rows {
        match row {
            CheckpointRow::Add {
                file,
                stats,
                deletion_vector,
            } => {
                append_checkpoint_add(
                    &mut add_builder,
                    Some((file, stats.as_deref(), deletion_vector.as_ref())),
                );
                append_checkpoint_remove(&mut remove_builder, None);
                append_checkpoint_sidecar(&mut sidecar_builder, None);
            }
            CheckpointRow::Remove {
                path,
                deletion_vector,
            } => {
                append_checkpoint_add(&mut add_builder, None);
                append_checkpoint_remove(
                    &mut remove_builder,
                    Some((path, deletion_vector.as_ref())),
                );
                append_checkpoint_sidecar(&mut sidecar_builder, None);
            }
            CheckpointRow::Sidecar { path } => {
                append_checkpoint_add(&mut add_builder, None);
                append_checkpoint_remove(&mut remove_builder, None);
                append_checkpoint_sidecar(&mut sidecar_builder, Some(path));
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

fn append_checkpoint_add(
    builder: &mut StructBuilder,
    add: Option<(
        &ResolvedFileDescriptor,
        Option<&str>,
        Option<&TestDeletionVector>,
    )>,
) {
    let Some((file, stats, deletion_vector)) = add else {
        builder
            .field_builder::<StringBuilder>(0)
            .expect("path builder")
            .append_null();
        builder
            .field_builder::<Int64Builder>(1)
            .expect("size builder")
            .append_null();
        builder
            .field_builder::<StringBuilder>(2)
            .expect("stats builder")
            .append_null();
        builder
            .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(3)
            .expect("partition map builder")
            .append(false)
            .expect("null partition map should append");
        append_checkpoint_deletion_vector(
            builder
                .field_builder::<StructBuilder>(4)
                .expect("deletion vector builder"),
            None,
        );
        builder.append(false);
        return;
    };

    builder
        .field_builder::<StringBuilder>(0)
        .expect("path builder")
        .append_value(&file.path);
    builder
        .field_builder::<Int64Builder>(1)
        .expect("size builder")
        .append_value(i64::try_from(file.size_bytes).expect("size should fit in i64"));
    builder
        .field_builder::<StringBuilder>(2)
        .expect("stats builder")
        .append_option(stats);
    let map_builder = builder
        .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(3)
        .expect("partition map builder");
    for (key, value) in &file.partition_values {
        map_builder.keys().append_value(key);
        map_builder.values().append_option(value.as_deref());
    }
    map_builder
        .append(true)
        .expect("partition map entry should append");
    append_checkpoint_deletion_vector(
        builder
            .field_builder::<StructBuilder>(4)
            .expect("deletion vector builder"),
        deletion_vector,
    );
    builder.append(true);
}

fn append_checkpoint_remove(
    builder: &mut StructBuilder,
    remove: Option<(&String, Option<&TestDeletionVector>)>,
) {
    builder
        .field_builder::<StringBuilder>(0)
        .expect("remove path builder")
        .append_option(remove.map(|(path, _)| path));
    append_checkpoint_deletion_vector(
        builder
            .field_builder::<StructBuilder>(1)
            .expect("remove deletion vector builder"),
        remove.and_then(|(_, deletion_vector)| deletion_vector),
    );
    builder.append(remove.is_some());
}

fn append_checkpoint_sidecar(builder: &mut StructBuilder, path: Option<&String>) {
    builder
        .field_builder::<StringBuilder>(0)
        .expect("sidecar path builder")
        .append_option(path);
    builder.append(path.is_some());
}

fn append_checkpoint_deletion_vector(
    builder: &mut StructBuilder,
    deletion_vector: Option<&TestDeletionVector>,
) {
    builder
        .field_builder::<StringBuilder>(0)
        .expect("storageType builder")
        .append_option(deletion_vector.map(|dv| dv.storage_type));
    builder
        .field_builder::<StringBuilder>(1)
        .expect("pathOrInlineDv builder")
        .append_option(deletion_vector.map(|dv| dv.path_or_inline_dv));
    builder
        .field_builder::<Int32Builder>(2)
        .expect("offset builder")
        .append_option(deletion_vector.and_then(|dv| dv.offset));
    builder
        .field_builder::<Int32Builder>(3)
        .expect("sizeInBytes builder")
        .append_option(deletion_vector.map(|_| 36));
    builder
        .field_builder::<Int64Builder>(4)
        .expect("cardinality builder")
        .append_option(deletion_vector.map(|_| 2));
    builder.append(deletion_vector.is_some());
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

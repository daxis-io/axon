#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use parquet::basic::Compression;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpParquetDatasetDescriptor, CapabilityReport,
    ExecutionTarget, QueryErrorCode, QueryRequest,
};
use wasm_datafusion_session::BrowserDataFusionSession;
use wasm_query_runtime::{BrowserObjectAccessMode, BrowserRuntimeConfig};

#[path = "../../wasm-datafusion-poc/tests/support/mod.rs"]
mod support;

use support::RequestCapturingServer;

static PARQUET_DATASET_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn session_opens_standard_parquet_dataset_without_delta_snapshot_version() {
    runtime().block_on(async {
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: Vec::new(),
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset.clone())
            .await
            .expect("standard Parquet dataset should open");
        let table = session
            .table("events")
            .expect("standard Parquet dataset should be registered");
        assert_eq!(table.descriptor().table_uri, dataset.table_uri);
        assert_eq!(table.descriptor().snapshot_version, 0);
        assert!(table.descriptor().active_files.is_empty());

        let request = QueryRequest::new(
            table.descriptor().table_uri.clone(),
            "SELECT COUNT(*) AS rows FROM events",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("SQL should execute against the standard Parquet dataset");

        assert_eq!(result.runtime_result.row_count, 1);
        assert_eq!(result.response.metrics.files_touched, 0);
    });
}

#[test]
fn session_queries_standard_parquet_dataset_over_loopback_range_reads() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![BrowserHttpFileDescriptor {
                path: "part-000.parquet".to_string(),
                url: server.url(),
                size_bytes: object_size,
                partition_values: BTreeMap::new(),
                stats: None,
            }],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("standard Parquet dataset should open");

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("SQL should execute against the standard Parquet dataset");

        assert_eq!(result.runtime_result.row_count, 2);
        assert_eq!(result.response.metrics.files_touched, 1);
        assert_eq!(result.response.metrics.footer_reads, Some(1));
        assert_eq!(
            result.response.metrics.bootstrap_footer_range_reads,
            Some(2),
            "open should report the trailer and footer payload ranges fetched during bootstrap"
        );
        assert!(
            result
                .response
                .metrics
                .scan_footer_range_reads
                .unwrap_or_default()
                >= 2,
            "SQL should report Parquet metadata ranges fetched by the scan"
        );
        assert!(
            result
                .response
                .metrics
                .scan_data_range_reads
                .unwrap_or_default()
                > 0,
            "SQL should report data-page ranges separately from scan footer ranges"
        );
        assert!(
            result
                .response
                .metrics
                .duplicate_range_reads
                .unwrap_or_default()
                >= 2,
            "open plus SQL should expose repeated trailer/footer ranges before caching exists"
        );
        assert_eq!(
            result.response.metrics.identity_present_range_reads,
            Some(0),
            "missing ETag metadata should be counted as the degraded identity path"
        );
        assert!(
            result
                .response
                .metrics
                .identity_missing_range_reads
                .unwrap_or_default()
                > 0,
            "range metrics should expose identity-missing reads"
        );
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "standard Parquet scans should use browser range I/O"
        );
    });
}

#[test]
fn session_preserves_bootstrapped_etag_for_datafusion_scan_if_range() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let object_etag = "\"part-000-v1\"".to_string();
        let server = RequestCapturingServer::new_with_etag(object, Some(object_etag.clone()));
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![BrowserHttpFileDescriptor {
                path: "part-000.parquet".to_string(),
                url: server.url(),
                size_bytes: object_size,
                partition_values: BTreeMap::new(),
                stats: None,
            }],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("standard Parquet dataset should open");

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("SQL should execute against the standard Parquet dataset");

        assert_eq!(result.runtime_result.row_count, 2);
        assert!(
            server
                .recorded_requests()
                .iter()
                .any(|request| request.headers.get("if-range") == Some(&object_etag)),
            "scan range requests should carry If-Range once bootstrap captured an ETag"
        );
        assert!(
            result
                .response
                .metrics
                .identity_present_range_reads
                .unwrap_or_default()
                > 0,
            "range metrics should expose identity-present reads"
        );
        assert!(
            result
                .response
                .metrics
                .identity_missing_range_reads
                .unwrap_or_default()
                > 0,
            "bootstrap ranges should remain visible as identity-missing reads"
        );
        assert!(
            result
                .response
                .metrics
                .scan_footer_range_reads
                .unwrap_or_default()
                == 0,
            "scan metadata should reuse the bootstrapped Parquet footer once object identity is available"
        );
        assert_eq!(
            result.response.metrics.duplicate_range_reads,
            Some(0),
            "open plus SQL should avoid repeating exact trailer/footer ranges once object identity is available"
        );
        assert_eq!(result.response.metrics.footer_cache_hits, Some(1));
        assert_eq!(result.response.metrics.footer_cache_misses, Some(1));
        assert_eq!(result.response.metrics.footer_range_reads_avoided, Some(2));
        assert_eq!(
            result
                .response
                .metrics
                .footer_cache_degraded_identity_reads,
            Some(1),
            "bootstrap discovers identity after starting in the conservative missing-identity path"
        );
    });
}

#[test]
fn session_treats_weak_etag_as_missing_if_range_identity() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server =
            RequestCapturingServer::new_with_etag(object, Some("W/\"part-000-v1\"".to_string()));
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![BrowserHttpFileDescriptor {
                path: "part-000.parquet".to_string(),
                url: server.url(),
                size_bytes: object_size,
                partition_values: BTreeMap::new(),
                stats: None,
            }],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("standard Parquet dataset should open");

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("SQL should still execute when only weak ETag metadata is available");

        assert_eq!(result.runtime_result.row_count, 2);
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| !request.headers.contains_key("if-range")),
            "weak ETags must not be sent as If-Range validators"
        );
        assert_eq!(
            result.response.metrics.identity_present_range_reads,
            Some(0),
            "weak ETags are not strong object identity for range validation"
        );
        assert!(
            result
                .response
                .metrics
                .identity_missing_range_reads
                .unwrap_or_default()
                > 0
        );
        assert_eq!(result.response.metrics.footer_cache_hits, Some(0));
        assert_eq!(result.response.metrics.footer_cache_misses, Some(2));
        assert_eq!(
            result.response.metrics.footer_cache_degraded_identity_reads,
            Some(2),
            "weak ETags should keep both bootstrap and scan metadata reads on the degraded path"
        );
    });
}

#[test]
fn session_rejects_zstd_parquet_dataset_before_query_execution() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns_and_compression(
            &[(3, 25), (1, 5), (2, 12)],
            Compression::ZSTD(Default::default()),
        );
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![BrowserHttpFileDescriptor {
                path: "region=ap-south/part-00000.zstd.parquet".to_string(),
                url: server.url(),
                size_bytes: object_size,
                partition_values: BTreeMap::new(),
                stats: None,
            }],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        let error = session
            .open_parquet_dataset("events", dataset)
            .await
            .expect_err("ZSTD Parquet datasets should be rejected before query execution");

        assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
        assert!(
            error.message.contains("ZSTD"),
            "error should identify the unsupported compression codec: {}",
            error.message
        );
        assert!(
            error
                .message
                .contains("region=ap-south/part-00000.zstd.parquet"),
            "error should identify the incompatible file: {}",
            error.message
        );
    });
}

fn browser_safe_http_config() -> BrowserRuntimeConfig {
    BrowserRuntimeConfig {
        object_access_mode: BrowserObjectAccessMode::BrowserSafeHttp,
        allow_cloud_credentials: false,
        ..BrowserRuntimeConfig::default()
    }
}

fn parquet_bytes_with_i64_columns(rows: &[(i64, i64)]) -> Vec<u8> {
    parquet_bytes_with_i64_columns_and_compression(rows, Compression::UNCOMPRESSED)
}

fn parquet_bytes_with_i64_columns_and_compression(
    rows: &[(i64, i64)],
    compression: Compression,
) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 value; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .build(),
        ),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();

    let mut row_group = writer
        .next_row_group()
        .expect("row-group writer should construct");
    if let Some(mut column) = row_group
        .next_column()
        .expect("id column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(&ids, None, None)
            .expect("id values should write");
        column.close().expect("id column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("value column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(&values, None, None)
            .expect("value values should write");
        column.close().expect("value column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created")
}

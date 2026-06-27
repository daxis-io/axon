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
    ExecutionTarget, PartitionColumnType, QueryErrorCode, QueryExecutionOptions, QueryRequest,
    QueryRuntimeLimits,
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
                object_etag: None,
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
            object_etag: None,
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
fn session_reuses_prepared_snapshot_for_repeated_sql_when_identity_matches() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server =
            RequestCapturingServer::new_with_etag(object, Some("\"part-000-v1\"".to_string()));
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
                object_etag: None,
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
        session
            .sql("events", &request)
            .await
            .expect("first SQL should prepare the table");
        let result = session
            .sql("events", &request)
            .await
            .expect("second SQL should reuse the prepared table");

        assert_eq!(result.runtime_result.row_count, 2);
        assert_eq!(result.response.metrics.session_reuse_count, Some(1));
        assert_eq!(result.response.metrics.opened_table_reuse_count, Some(1));
        assert_eq!(
            result.response.metrics.bootstrap_footer_range_reads,
            Some(0)
        );
    });
}

#[test]
fn session_runtime_limit_changes_reuse_immutable_prepared_state() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server =
            RequestCapturingServer::new_with_etag(object, Some("\"part-000-v1\"".to_string()));
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
                object_etag: None,
            }],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("standard Parquet dataset should open");

        let base_request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        session
            .sql("events", &base_request)
            .await
            .expect("first SQL should prepare the table");
        let limited_request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        )
        .with_options(QueryExecutionOptions {
            runtime_limits: Some(QueryRuntimeLimits {
                max_result_rows: Some(10),
                max_arrow_ipc_bytes: None,
                max_scan_bytes: None,
            }),
            ..QueryExecutionOptions::default()
        });

        let result = session
            .sql("events", &limited_request)
            .await
            .expect("runtime-limit-only changes should reuse immutable prepared state");

        assert_eq!(result.runtime_result.row_count, 2);
        assert_eq!(result.response.metrics.session_reuse_count, Some(1));
        assert_eq!(
            result.response.metrics.bootstrap_footer_range_reads,
            Some(0)
        );
    });
}

#[test]
fn session_reuses_prepared_state_across_access_envelope_refresh_when_identity_matches() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let first_server = RequestCapturingServer::new_with_etag(
            object.clone(),
            Some("\"part-000-v1\"".to_string()),
        );
        let refreshed_server =
            RequestCapturingServer::new_with_etag(object, Some("\"part-000-v1\"".to_string()));
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");
        let first_dataset = single_identity_dataset(
            "https://example.invalid/datasets/events",
            first_server.url(),
            object_size,
            "\"part-000-v1\"",
        );
        let refreshed_dataset = single_identity_dataset(
            "https://example.invalid/datasets/events",
            refreshed_server.url(),
            object_size,
            "\"part-000-v1\"",
        );

        session
            .open_parquet_dataset("events", first_dataset)
            .await
            .expect("initial dataset should open");
        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        session
            .sql("events", &request)
            .await
            .expect("first SQL should prepare immutable state");

        session
            .open_parquet_dataset("events", refreshed_dataset)
            .await
            .expect("refreshed access envelope should reopen");
        let result = session
            .sql("events", &request)
            .await
            .expect("matching identity should reuse prepared state");

        assert_eq!(result.runtime_result.row_count, 2);
        assert_eq!(result.response.metrics.session_reuse_count, Some(1));
        assert_eq!(
            result.response.metrics.access_envelope_refresh_count,
            Some(1)
        );
        assert_eq!(result.response.metrics.identity_refresh_count, Some(0));
        assert_eq!(
            result.response.metrics.bootstrap_footer_range_reads,
            Some(0)
        );
        assert!(
            refreshed_server.recorded_requests().len() > 0,
            "the refreshed descriptor URL should be used for scan reads"
        );
    });
}

#[test]
fn session_drops_prepared_state_across_reopen_when_strong_etag_drifts() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let first_server = RequestCapturingServer::new_with_etag(
            object.clone(),
            Some("\"part-000-v1\"".to_string()),
        );
        let drifted_server =
            RequestCapturingServer::new_with_etag(object, Some("\"part-000-v2\"".to_string()));
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset(
                "events",
                single_identity_dataset(
                    "https://example.invalid/datasets/events",
                    first_server.url(),
                    object_size,
                    "\"part-000-v1\"",
                ),
            )
            .await
            .expect("initial dataset should open");
        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        session
            .sql("events", &request)
            .await
            .expect("first SQL should prepare immutable state");

        session
            .open_parquet_dataset(
                "events",
                single_identity_dataset(
                    "https://example.invalid/datasets/events",
                    drifted_server.url(),
                    object_size,
                    "\"part-000-v2\"",
                ),
            )
            .await
            .expect("drifted identity descriptor should reopen");
        let result = session
            .sql("events", &request)
            .await
            .expect("drifted identity should rebuild prepared state");

        assert_eq!(result.runtime_result.row_count, 2);
        assert_eq!(result.response.metrics.session_reuse_count, Some(0));
        assert_eq!(result.response.metrics.identity_refresh_count, Some(1));
        assert_eq!(
            result.response.metrics.access_envelope_refresh_count,
            Some(0)
        );
        assert!(
            result
                .response
                .metrics
                .bootstrap_footer_range_reads
                .unwrap_or(0)
                > 0,
            "drifted identity should force footer/bootstrap work"
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
                object_etag: None,
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
fn session_prunes_partitions_before_footer_bootstrap() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let matching_object = parquet_bytes_with_i64_columns(&[(1, 12), (2, 18)]);
        let pruned_object = parquet_bytes_with_i64_columns(&[(3, 30)]);
        let matching_server = RequestCapturingServer::new(matching_object.clone());
        let pruned_server = RequestCapturingServer::new(pruned_object.clone());
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::from([(
                "event_date".to_string(),
                PartitionColumnType::String,
            )]),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![
                BrowserHttpFileDescriptor {
                    path: "event_date=2026-06-25/part-000.parquet".to_string(),
                    url: matching_server.url(),
                    size_bytes: u64::try_from(matching_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::from([(
                        "event_date".to_string(),
                        Some("2026-06-25".to_string()),
                    )]),
                    stats: None,
                    object_etag: None,
                },
                BrowserHttpFileDescriptor {
                    path: "event_date=2026-06-24/part-000.parquet".to_string(),
                    url: pruned_server.url(),
                    size_bytes: u64::try_from(pruned_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::from([(
                        "event_date".to_string(),
                        Some("2026-06-24".to_string()),
                    )]),
                    stats: None,
                    object_etag: None,
                },
            ],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("dataset open should not eagerly bootstrap nonempty footers");
        assert!(matching_server.recorded_requests().is_empty());
        assert!(pruned_server.recorded_requests().is_empty());

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE event_date = '2026-06-25' ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("SQL should execute after prebootstrap partition pruning");

        assert_eq!(result.runtime_result.row_count, 2);
        assert!(!matching_server.recorded_requests().is_empty());
        assert!(
            pruned_server.recorded_requests().is_empty(),
            "nonmatching partition file should not be requested before DataFusion registration"
        );
        assert_eq!(
            result.response.metrics.prebootstrap_candidate_files,
            Some(1)
        );
        assert_eq!(result.response.metrics.prebootstrap_files_pruned, Some(1));
        assert_eq!(result.response.metrics.footer_reads_avoided, Some(1));
        assert_eq!(
            result.response.metrics.prebootstrap_fail_open_count,
            Some(0)
        );
        assert_eq!(result.response.metrics.footer_reads, Some(1));
        assert_eq!(result.response.metrics.files_skipped, 1);
    });
}

#[test]
fn session_prunes_delta_integer_stats_before_footer_bootstrap() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let pruned_object = parquet_bytes_with_i64_columns(&[(1, 5), (2, 7)]);
        let matching_object = parquet_bytes_with_i64_columns(&[(3, 25), (4, 30)]);
        let pruned_server = RequestCapturingServer::new(pruned_object.clone());
        let matching_server = RequestCapturingServer::new(matching_object.clone());
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![
                BrowserHttpFileDescriptor {
                    path: "part-low.parquet".to_string(),
                    url: pruned_server.url(),
                    size_bytes: u64::try_from(pruned_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::new(),
                    stats: Some(delta_i64_stats_json("value", 1, 7)),
                    object_etag: None,
                },
                BrowserHttpFileDescriptor {
                    path: "part-high.parquet".to_string(),
                    url: matching_server.url(),
                    size_bytes: u64::try_from(matching_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::new(),
                    stats: Some(delta_i64_stats_json("value", 25, 30)),
                    object_etag: None,
                },
            ],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("dataset open should not eagerly bootstrap nonempty footers");

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("SQL should execute after prebootstrap Delta stats pruning");

        assert_eq!(result.runtime_result.row_count, 2);
        assert!(matching_server.recorded_requests().len() > 0);
        assert!(
            pruned_server.recorded_requests().is_empty(),
            "file outside Delta min/max should not be requested before registration"
        );
        assert_eq!(
            result.response.metrics.prebootstrap_candidate_files,
            Some(1)
        );
        assert_eq!(result.response.metrics.prebootstrap_files_pruned, Some(1));
        assert_eq!(result.response.metrics.footer_reads_avoided, Some(1));
        assert_eq!(result.response.metrics.footer_reads, Some(1));
        assert_eq!(result.response.metrics.files_skipped, 1);
    });
}

#[test]
fn session_fail_opens_zero_candidate_prebootstrap_pruning() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let first_object = parquet_bytes_with_i64_columns(&[(1, 5)]);
        let second_object = parquet_bytes_with_i64_columns(&[(2, 7)]);
        let first_server = RequestCapturingServer::new(first_object.clone());
        let second_server = RequestCapturingServer::new(second_object.clone());
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::new(),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![
                BrowserHttpFileDescriptor {
                    path: "part-a.parquet".to_string(),
                    url: first_server.url(),
                    size_bytes: u64::try_from(first_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::new(),
                    stats: Some(delta_i64_stats_json("value", 5, 5)),
                    object_etag: None,
                },
                BrowserHttpFileDescriptor {
                    path: "part-b.parquet".to_string(),
                    url: second_server.url(),
                    size_bytes: u64::try_from(second_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::new(),
                    stats: Some(delta_i64_stats_json("value", 7, 7)),
                    object_etag: None,
                },
            ],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("dataset open should not eagerly bootstrap nonempty footers");

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value = 999 ORDER BY id",
            ExecutionTarget::BrowserWasm,
        );
        let result = session.sql("events", &request).await.expect(
            "zero-candidate prebootstrap pruning should fail open and preserve correctness",
        );

        assert_eq!(result.runtime_result.row_count, 0);
        assert!(!first_server.recorded_requests().is_empty());
        assert!(!second_server.recorded_requests().is_empty());
        assert_eq!(
            result.response.metrics.prebootstrap_candidate_files,
            Some(2)
        );
        assert_eq!(result.response.metrics.prebootstrap_files_pruned, Some(0));
        assert_eq!(result.response.metrics.footer_reads_avoided, Some(0));
        assert_eq!(
            result.response.metrics.prebootstrap_fail_open_count,
            Some(1)
        );
        assert_eq!(result.response.metrics.footer_reads, Some(2));
    });
}

#[test]
fn session_fail_opens_unsupported_prebootstrap_shape_to_all_active_files() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let matching_object = parquet_bytes_with_i64_columns(&[(1, 10)]);
        let nonmatching_object = parquet_bytes_with_i64_columns(&[(2, 20)]);
        let matching_server = RequestCapturingServer::new(matching_object.clone());
        let nonmatching_server = RequestCapturingServer::new(nonmatching_object.clone());
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::from([(
                "event_date".to_string(),
                PartitionColumnType::String,
            )]),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![
                BrowserHttpFileDescriptor {
                    path: "event_date=2026-06-25/part-ok.parquet".to_string(),
                    url: matching_server.url(),
                    size_bytes: u64::try_from(matching_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::from([(
                        "event_date".to_string(),
                        Some("2026-06-25".to_string()),
                    )]),
                    stats: None,
                    object_etag: None,
                },
                BrowserHttpFileDescriptor {
                    path: "event_date=2026-06-24/part-other.parquet".to_string(),
                    url: nonmatching_server.url(),
                    size_bytes: u64::try_from(nonmatching_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::from([(
                        "event_date".to_string(),
                        Some("2026-06-24".to_string()),
                    )]),
                    stats: None,
                    object_etag: None,
                },
            ],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("dataset open should not eagerly bootstrap nonempty footers");
        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT DISTINCT id FROM events WHERE event_date = '2026-06-25'",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("unsupported prebootstrap shape should fail open to DataFusion execution");

        assert_eq!(result.runtime_result.row_count, 1);
        assert!(!matching_server.recorded_requests().is_empty());
        assert!(
            !nonmatching_server.recorded_requests().is_empty(),
            "unsupported prebootstrap shapes should bootstrap all active files"
        );
        assert_eq!(
            result.response.metrics.prebootstrap_candidate_files,
            Some(2)
        );
        assert_eq!(result.response.metrics.prebootstrap_files_pruned, Some(0));
        assert_eq!(result.response.metrics.footer_reads_avoided, Some(0));
        assert_eq!(
            result.response.metrics.prebootstrap_fail_open_count,
            Some(0)
        );
        assert_eq!(result.response.metrics.footer_reads, Some(2));
    });
}

#[test]
fn session_fail_opens_missing_malformed_incomplete_or_incompatible_delta_stats() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    for (case_name, stats) in [
        ("missing", None),
        ("malformed", Some("not-json".to_string())),
        (
            "incomplete",
            Some(r#"{"minValues":{"value":1},"nullCount":{"value":0}}"#.to_string()),
        ),
        (
            "type-incompatible",
            Some(
                r#"{"minValues":{"value":"1"},"maxValues":{"value":"9"},"nullCount":{"value":0}}"#
                    .to_string(),
            ),
        ),
    ] {
        runtime().block_on(async {
            let uncertain_object = parquet_bytes_with_i64_columns(&[(1, 1)]);
            let matching_object = parquet_bytes_with_i64_columns(&[(2, 25)]);
            let uncertain_server = RequestCapturingServer::new(uncertain_object.clone());
            let matching_server = RequestCapturingServer::new(matching_object.clone());
            let dataset = BrowserHttpParquetDatasetDescriptor {
                table_uri: format!("https://example.invalid/datasets/events-{case_name}"),
                partition_column_types: BTreeMap::new(),
                browser_compatibility: CapabilityReport::default(),
                required_capabilities: CapabilityReport::default(),
                files: vec![
                    BrowserHttpFileDescriptor {
                        path: format!("part-uncertain-{case_name}.parquet"),
                        url: uncertain_server.url(),
                        size_bytes: u64::try_from(uncertain_object.len())
                            .expect("object size should fit in u64"),
                        partition_values: BTreeMap::new(),
                        stats,
                        object_etag: None,
                    },
                    BrowserHttpFileDescriptor {
                        path: format!("part-matching-{case_name}.parquet"),
                        url: matching_server.url(),
                        size_bytes: u64::try_from(matching_object.len())
                            .expect("object size should fit in u64"),
                        partition_values: BTreeMap::new(),
                        stats: Some(delta_i64_stats_json("value", 25, 25)),
                        object_etag: None,
                    },
                ],
            };
            let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
                .expect("browser DataFusion session should construct");

            session
                .open_parquet_dataset("events", dataset.clone())
                .await
                .expect("dataset open should not eagerly bootstrap nonempty footers");
            let request = QueryRequest::new(
                dataset.table_uri,
                "SELECT id FROM events WHERE value > 10 ORDER BY id",
                ExecutionTarget::BrowserWasm,
            );
            let result = session
                .sql("events", &request)
                .await
                .unwrap_or_else(|error| panic!("{case_name} stats should fail open: {error:?}"));

            assert_eq!(result.runtime_result.row_count, 1, "case {case_name}");
            assert!(
                !uncertain_server.recorded_requests().is_empty(),
                "case {case_name}: uncertain stats file should still be bootstrapped"
            );
            assert!(
                !matching_server.recorded_requests().is_empty(),
                "case {case_name}: matching file should be bootstrapped"
            );
            assert_eq!(
                result.response.metrics.prebootstrap_candidate_files,
                Some(2),
                "case {case_name}"
            );
            assert_eq!(
                result.response.metrics.prebootstrap_files_pruned,
                Some(0),
                "case {case_name}"
            );
            assert_eq!(
                result.response.metrics.footer_reads_avoided,
                Some(0),
                "case {case_name}"
            );
            assert_eq!(
                result.response.metrics.prebootstrap_fail_open_count,
                Some(0),
                "case {case_name}"
            );
        });
    }
}

#[test]
fn session_pruned_noncandidate_unsupported_compression_is_not_bootstrapped() {
    let _guard = PARQUET_DATASET_TEST_LOCK
        .lock()
        .expect("Parquet dataset tests should serialize local HTTP servers");

    runtime().block_on(async {
        let matching_object = parquet_bytes_with_i64_columns(&[(1, 12)]);
        let pruned_zstd_object = parquet_bytes_with_i64_columns_and_compression(
            &[(2, 30)],
            Compression::ZSTD(Default::default()),
        );
        let matching_server = RequestCapturingServer::new(matching_object.clone());
        let pruned_zstd_server = RequestCapturingServer::new(pruned_zstd_object.clone());
        let dataset = BrowserHttpParquetDatasetDescriptor {
            table_uri: "https://example.invalid/datasets/events".to_string(),
            partition_column_types: BTreeMap::from([(
                "event_date".to_string(),
                PartitionColumnType::String,
            )]),
            browser_compatibility: CapabilityReport::default(),
            required_capabilities: CapabilityReport::default(),
            files: vec![
                BrowserHttpFileDescriptor {
                    path: "event_date=2026-06-25/part-ok.parquet".to_string(),
                    url: matching_server.url(),
                    size_bytes: u64::try_from(matching_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::from([(
                        "event_date".to_string(),
                        Some("2026-06-25".to_string()),
                    )]),
                    stats: None,
                    object_etag: None,
                },
                BrowserHttpFileDescriptor {
                    path: "event_date=2026-06-24/part-zstd.parquet".to_string(),
                    url: pruned_zstd_server.url(),
                    size_bytes: u64::try_from(pruned_zstd_object.len())
                        .expect("object size should fit in u64"),
                    partition_values: BTreeMap::from([(
                        "event_date".to_string(),
                        Some("2026-06-24".to_string()),
                    )]),
                    stats: None,
                    object_etag: None,
                },
            ],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("open should not reject nonempty datasets before candidate bootstrap");
        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE event_date = '2026-06-25'",
            ExecutionTarget::BrowserWasm,
        );
        let result = session
            .sql("events", &request)
            .await
            .expect("pruned noncandidate ZSTD file should not force an early rejection");

        assert_eq!(result.runtime_result.row_count, 1);
        assert!(!matching_server.recorded_requests().is_empty());
        assert!(pruned_zstd_server.recorded_requests().is_empty());
        assert_eq!(result.response.metrics.prebootstrap_files_pruned, Some(1));
        assert_eq!(result.response.metrics.footer_reads_avoided, Some(1));
    });
}

#[test]
fn session_rejects_zstd_candidate_before_data_scanning() {
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
                object_etag: None,
            }],
        };
        let mut session = BrowserDataFusionSession::new(browser_safe_http_config(), u64::MAX)
            .expect("browser DataFusion session should construct");

        session
            .open_parquet_dataset("events", dataset)
            .await
            .expect("open should cache nonempty descriptors without eager footer bootstrap");

        let request = QueryRequest::new(
            "https://example.invalid/datasets/events",
            "SELECT id FROM events WHERE value > 10",
            ExecutionTarget::BrowserWasm,
        );
        let error = session
            .sql("events", &request)
            .await
            .expect_err("ZSTD candidate files should fail during pre-scan bootstrap");

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

fn single_identity_dataset(
    table_uri: &str,
    url: String,
    object_size: u64,
    object_etag: &str,
) -> BrowserHttpParquetDatasetDescriptor {
    BrowserHttpParquetDatasetDescriptor {
        table_uri: table_uri.to_string(),
        partition_column_types: BTreeMap::new(),
        browser_compatibility: CapabilityReport::default(),
        required_capabilities: CapabilityReport::default(),
        files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url,
            size_bytes: object_size,
            partition_values: BTreeMap::new(),
            stats: None,
            object_etag: Some(object_etag.to_string()),
        }],
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

fn delta_i64_stats_json(column: &str, min_value: i64, max_value: i64) -> String {
    format!(
        r#"{{"minValues":{{"{column}":{min_value}}},"maxValues":{{"{column}":{max_value}}},"nullCount":{{"{column}":0}}}}"#
    )
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created")
}

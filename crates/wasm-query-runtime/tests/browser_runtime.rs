#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, CapabilityKey, CapabilityState,
    ExecutionTarget, FallbackReason, QueryErrorCode,
};
use wasm_http_object_store::{HttpByteRange, HttpRangeReader};
use wasm_query_runtime::{
    runtime_target, BootstrappedBrowserFile, BootstrappedBrowserSnapshot, BrowserObjectAccessMode,
    BrowserObjectSource, BrowserParquetField, BrowserParquetFileMetadata, BrowserRuntimeConfig,
    BrowserRuntimeSession, MaterializedBrowserFile,
};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

#[test]
fn default_config_constructs_a_browser_runtime_session() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    assert_eq!(runtime_target(), ExecutionTarget::BrowserWasm);
    assert_eq!(session.config(), &BrowserRuntimeConfig::default());
}

#[test]
fn https_object_sources_are_constructible() {
    let source = BrowserObjectSource::from_url("https://example.com/object")
        .expect("https object sources should be supported");

    assert_eq!(source.url(), "https://example.com/object");
}

#[test]
fn materialize_snapshot_preserves_descriptor_metadata_and_file_order() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 7,
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: "z-last.parquet".to_string(),
                url: "https://example.com/z-last.parquet".to_string(),
                size_bytes: 333,
                partition_values: BTreeMap::from([("category".to_string(), Some("z".to_string()))]),
            },
            BrowserHttpFileDescriptor {
                path: "a-first.parquet".to_string(),
                url: "https://example.com/a-first.parquet".to_string(),
                size_bytes: 111,
                partition_values: BTreeMap::from([
                    ("category".to_string(), Some("a".to_string())),
                    ("region".to_string(), None),
                ]),
            },
        ],
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("https descriptors should materialize");

    assert_eq!(materialized.table_uri(), descriptor.table_uri);
    assert_eq!(materialized.snapshot_version(), descriptor.snapshot_version);
    assert_eq!(
        materialized.active_files().len(),
        descriptor.active_files.len()
    );
    for (materialized_file, descriptor_file) in materialized
        .active_files()
        .iter()
        .zip(descriptor.active_files.iter())
    {
        assert_eq!(materialized_file.path(), descriptor_file.path);
        assert_eq!(materialized_file.size_bytes(), descriptor_file.size_bytes);
        assert_eq!(
            materialized_file.partition_values(),
            &descriptor_file.partition_values
        );
        assert_eq!(materialized_file.object_source().url(), descriptor_file.url);
    }
}

#[test]
fn materialize_snapshot_allows_empty_active_file_lists() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/empty_table".to_string(),
        snapshot_version: 3,
        active_files: Vec::new(),
    };

    let materialized = session
        .materialize_snapshot(&descriptor)
        .expect("empty descriptors should materialize");

    assert_eq!(materialized.table_uri(), descriptor.table_uri);
    assert_eq!(materialized.snapshot_version(), descriptor.snapshot_version);
    assert!(materialized.active_files().is_empty());
}

#[test]
fn multi_partition_configs_require_native_fallback() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        target_partitions: 2,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("multi-partition configs should be rejected");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::CapabilityGate {
            capability: CapabilityKey::MultiPartitionExecution,
            required_state: CapabilityState::NativeOnly,
        })
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn non_http_object_access_modes_fail_with_browser_runtime_constraint() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        object_access_mode: BrowserObjectAccessMode::CloudObjectStore,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("cloud object store access should be rejected");

    assert_eq!(error.code, QueryErrorCode::FallbackRequired);
    assert_eq!(
        error.fallback_reason,
        Some(FallbackReason::BrowserRuntimeConstraint)
    );
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn cloud_credentials_are_rejected_as_a_security_policy_violation() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        allow_cloud_credentials: true,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("cloud credentials should be rejected");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn request_timeouts_must_be_positive() {
    let error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero timeouts should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn snapshot_preflight_limits_must_be_positive() {
    let concurrency_error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        snapshot_preflight_max_concurrency: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero snapshot preflight concurrency should be rejected");
    assert_eq!(concurrency_error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(concurrency_error.fallback_reason, None);
    assert_eq!(concurrency_error.target, ExecutionTarget::BrowserWasm);

    let timeout_error = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        snapshot_preflight_timeout_ms: 0,
        ..BrowserRuntimeConfig::default()
    })
    .expect_err("zero snapshot preflight timeout should be rejected");
    assert_eq!(timeout_error.code, QueryErrorCode::InvalidRequest);
    assert_eq!(timeout_error.fallback_reason, None);
    assert_eq!(timeout_error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn non_loopback_plain_http_sources_are_rejected() {
    let error = BrowserObjectSource::from_url("http://0.0.0.0:1/object")
        .expect_err("non-loopback plain HTTP should be rejected");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn unsupported_object_source_schemes_are_rejected() {
    for url in ["file:///tmp/object", "s3://bucket/object"] {
        let error =
            BrowserObjectSource::from_url(url).expect_err("unsupported schemes should be rejected");

        assert_eq!(error.code, QueryErrorCode::InvalidRequest, "url: {url}");
        assert_eq!(error.fallback_reason, None, "url: {url}");
        assert_eq!(error.target, ExecutionTarget::BrowserWasm, "url: {url}");
    }
}

#[test]
fn materialize_snapshot_rejects_duplicate_paths() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let duplicate_path = "category=A/part-000.parquet".to_string();
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 12,
        active_files: vec![
            BrowserHttpFileDescriptor {
                path: duplicate_path.clone(),
                url: "https://example.com/one.parquet".to_string(),
                size_bytes: 128,
                partition_values: BTreeMap::new(),
            },
            BrowserHttpFileDescriptor {
                path: duplicate_path.clone(),
                url: "https://example.com/two.parquet".to_string(),
                size_bytes: 256,
                partition_values: BTreeMap::new(),
            },
        ],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("duplicate paths should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains(&duplicate_path));
}

#[test]
fn materialize_snapshot_rejects_invalid_url_syntax_without_leaking_query_or_fragment() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "https://signed example.test/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("invalid URL syntax should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("https://signed example.test/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn materialize_snapshot_rejects_unsupported_schemes_without_leaking_query_or_fragment() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "ftp://signed.example.test/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("unsupported schemes should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("ftp://signed.example.test/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn materialize_snapshot_rejects_non_loopback_plain_http_urls() {
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 1,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://example.com/object?sig=super-secret#fragment".to_string(),
            size_bytes: 128,
            partition_values: BTreeMap::new(),
        }],
    };

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("non-loopback plain HTTP should fail");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("http://example.com/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn sessions_can_probe_loopback_http_sources_in_host_tests_through_an_injected_range_reader() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"abcdefghij"));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session =
        BrowserRuntimeSession::with_reader(BrowserRuntimeConfig::default(), HttpRangeReader::new())
            .expect("default config should be supported");

    let result = runtime()
        .block_on(session.probe(
            &source,
            HttpByteRange::Bounded {
                offset: 2,
                length: 4,
            },
        ))
        .expect("probe should succeed");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=2-5".to_string()));
    assert_eq!(result.metadata.url, url);
    assert_eq!(result.metadata.size_bytes, Some(10));
    assert_eq!(result.bytes.as_ref(), b"cdef");
}

#[test]
fn materialize_snapshot_rejects_loopback_http_even_in_host_tests() {
    let descriptor = BrowserHttpSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 9,
        active_files: vec![BrowserHttpFileDescriptor {
            path: "part-000.parquet".to_string(),
            url: "http://127.0.0.1:8787/object?sig=super-secret#fragment".to_string(),
            size_bytes: 10,
            partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
        }],
    };
    let session =
        BrowserRuntimeSession::with_reader(BrowserRuntimeConfig::default(), HttpRangeReader::new())
            .expect("default config should be supported");

    let error = session
        .materialize_snapshot(&descriptor)
        .expect_err("browser-facing descriptors should remain HTTPS-only");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("http://127.0.0.1:8787/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn new_sessions_apply_request_timeouts_to_probe_requests() {
    let (url, server) = spawn_stalling_server(Duration::from_millis(250));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 25,
        ..BrowserRuntimeConfig::default()
    })
    .expect("short timeouts should be supported");

    let error = runtime()
        .block_on(session.probe(&source, HttpByteRange::Full))
        .expect_err("stalled probes should time out");

    server
        .join()
        .expect("stalling test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_applies_request_timeouts_to_footer_reads() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let trailer = object[object.len() - 8..].to_vec();
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let (url, requests, server) =
        spawn_stalling_footer_read_server(Duration::from_millis(250), trailer, object.len());
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 25,
        ..BrowserRuntimeConfig::default()
    })
    .expect("short timeouts should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("stalled footer reads should time out");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn probe_preserves_http_range_reader_errors() {
    let (url, _, server) = spawn_test_server(|_| TestResponse {
        status_line: "401 Unauthorized",
        headers: vec![("Content-Length".to_string(), "0".to_string())],
        body: Vec::new(),
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.probe(&source, HttpByteRange::Full))
        .expect_err("unauthorized probes should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_bootstraps_raw_footer_bytes_from_loopback_http() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let (url, requests, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object)
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let result = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect("footer bootstrap should succeed");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(
        result.object_size_bytes(),
        (prefix.len() + footer.len() + 8) as u64
    );
    assert_eq!(result.footer_length_bytes(), footer.len() as u32);
    assert_eq!(result.footer_bytes(), footer);
}

#[test]
fn read_parquet_footer_for_file_bootstraps_raw_footer_bytes_from_loopback_http() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        object.len() as u64,
        BTreeMap::new(),
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests"),
    );

    let result = runtime()
        .block_on(session.read_parquet_footer_for_file(&file))
        .expect("file-driven footer bootstrap should succeed");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(result.object_size_bytes(), file.size_bytes());
    assert_eq!(result.footer_length_bytes(), footer.len() as u32);
    assert_eq!(result.footer_bytes(), footer);
}

#[test]
fn read_parquet_footer_for_file_rejects_descriptor_size_mismatches() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        object.len() as u64 + 1,
        BTreeMap::new(),
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests"),
    );

    let error = runtime()
        .block_on(session.read_parquet_footer_for_file(&file))
        .expect_err("descriptor size mismatches should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains(file.path()));
}

#[test]
fn read_parquet_metadata_for_file_rejects_malformed_footer_metadata() {
    let footer = b"not-a-valid-parquet-footer";
    let object = parquet_like_object(b"row-group-bytes", footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");
    let file = MaterializedBrowserFile::new(
        "part-000.parquet",
        object.len() as u64,
        BTreeMap::new(),
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests"),
    );

    let error = runtime()
        .block_on(session.read_parquet_metadata_for_file(&file))
        .expect_err("malformed footer metadata should fail");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert!(error.message.contains(file.path()));
}

#[test]
fn validate_uniform_schema_rejects_mixed_file_field_layouts() {
    let snapshot = BootstrappedBrowserSnapshot {
        table_uri: "gs://axon-fixtures/mixed".to_string(),
        snapshot_version: 5,
        active_files: vec![
            BootstrappedBrowserFile {
                path: "part-000.parquet".to_string(),
                size_bytes: 100,
                partition_values: BTreeMap::new(),
                metadata: BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![BrowserParquetField {
                        name: "id".to_string(),
                        physical_type: "INT32".to_string(),
                        logical_type: None,
                        converted_type: None,
                        repetition: "REQUIRED".to_string(),
                        nullable: false,
                        max_definition_level: 0,
                        max_repetition_level: 0,
                        type_length: None,
                        precision: None,
                        scale: None,
                    }],
                },
            },
            BootstrappedBrowserFile {
                path: "part-001.parquet".to_string(),
                size_bytes: 120,
                partition_values: BTreeMap::new(),
                metadata: BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![BrowserParquetField {
                        name: "id".to_string(),
                        physical_type: "INT64".to_string(),
                        logical_type: None,
                        converted_type: None,
                        repetition: "REQUIRED".to_string(),
                        nullable: false,
                        max_definition_level: 0,
                        max_repetition_level: 0,
                        type_length: None,
                        precision: None,
                        scale: None,
                    }],
                },
            },
        ],
    };

    let error = snapshot
        .validate_uniform_schema()
        .expect_err("mixed schemas should fail validation");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("part-001.parquet"));
}

#[test]
fn validate_uniform_schema_rejects_mixed_logical_types_with_the_same_physical_type() {
    let snapshot = BootstrappedBrowserSnapshot {
        table_uri: "gs://axon-fixtures/mixed-logical".to_string(),
        snapshot_version: 6,
        active_files: vec![
            BootstrappedBrowserFile {
                path: "part-000.parquet".to_string(),
                size_bytes: 100,
                partition_values: BTreeMap::from([("category".to_string(), Some("A".to_string()))]),
                metadata: BrowserParquetFileMetadata {
                    object_size_bytes: 100,
                    footer_length_bytes: 32,
                    row_group_count: 1,
                    row_count: 3,
                    fields: vec![BrowserParquetField {
                        name: "name".to_string(),
                        physical_type: "BYTE_ARRAY".to_string(),
                        logical_type: Some("String".to_string()),
                        converted_type: Some("UTF8".to_string()),
                        repetition: "OPTIONAL".to_string(),
                        nullable: true,
                        max_definition_level: 1,
                        max_repetition_level: 0,
                        type_length: None,
                        precision: None,
                        scale: None,
                    }],
                },
            },
            BootstrappedBrowserFile {
                path: "part-001.parquet".to_string(),
                size_bytes: 120,
                partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                metadata: BrowserParquetFileMetadata {
                    object_size_bytes: 120,
                    footer_length_bytes: 40,
                    row_group_count: 1,
                    row_count: 4,
                    fields: vec![BrowserParquetField {
                        name: "name".to_string(),
                        physical_type: "BYTE_ARRAY".to_string(),
                        logical_type: None,
                        converted_type: None,
                        repetition: "OPTIONAL".to_string(),
                        nullable: true,
                        max_definition_level: 1,
                        max_repetition_level: 0,
                        type_length: None,
                        precision: None,
                        scale: None,
                    }],
                },
            },
        ],
    };

    let error = snapshot
        .validate_uniform_schema()
        .expect_err("logical-type mismatches should fail validation");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("part-001.parquet"));
}

#[test]
fn summarize_reports_sorted_partition_columns() {
    let summary = BootstrappedBrowserSnapshot {
        table_uri: "gs://axon-fixtures/partitioned".to_string(),
        snapshot_version: 7,
        active_files: vec![BootstrappedBrowserFile {
            path: "part-000.parquet".to_string(),
            size_bytes: 100,
            partition_values: BTreeMap::from([
                ("region".to_string(), Some("us-east-1".to_string())),
                ("category".to_string(), Some("A".to_string())),
            ]),
            metadata: BrowserParquetFileMetadata {
                object_size_bytes: 100,
                footer_length_bytes: 32,
                row_group_count: 1,
                row_count: 3,
                fields: vec![BrowserParquetField {
                    name: "id".to_string(),
                    physical_type: "INT32".to_string(),
                    logical_type: None,
                    converted_type: None,
                    repetition: "REQUIRED".to_string(),
                    nullable: false,
                    max_definition_level: 0,
                    max_repetition_level: 0,
                    type_length: None,
                    precision: None,
                    scale: None,
                }],
            },
        }],
    }
    .summarize()
    .expect("uniform snapshots should summarize");

    assert_eq!(
        summary.schema.partition_columns,
        vec!["category".to_string(), "region".to_string()]
    );
}

#[test]
fn bootstrap_snapshot_metadata_enforces_the_snapshot_preflight_deadline() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let object_for_server = object.clone();
    let (url, _, server) = spawn_multi_request_server(2, move |request, _| {
        thread::sleep(Duration::from_millis(70));
        full_or_ranged_response(request, &object_for_server)
    });
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig {
        request_timeout_ms: 200,
        snapshot_preflight_timeout_ms: 100,
        snapshot_preflight_max_concurrency: 1,
        ..BrowserRuntimeConfig::default()
    })
    .expect("preflight timeout configuration should be supported");
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let snapshot = wasm_query_runtime::MaterializedBrowserSnapshot::new(
        "gs://axon-fixtures/sample_table",
        9,
        vec![MaterializedBrowserFile::new(
            "part-000.parquet",
            object.len() as u64,
            BTreeMap::new(),
            source,
        )],
    )
    .expect("duplicate-free snapshots should construct");

    let error = runtime()
        .block_on(session.bootstrap_snapshot_metadata(&snapshot))
        .expect_err("snapshot preflight should fail once the batch deadline is exceeded");

    server.join().expect("test server should shut down cleanly");
    assert_eq!(error.code, QueryErrorCode::ExecutionFailed);
    assert!(error.message.contains("snapshot preflight"));
}

#[test]
fn read_parquet_footer_rejects_objects_smaller_than_the_parquet_trailer() {
    let (url, requests, server) =
        spawn_test_server(|request| full_or_ranged_response(request, b"small"));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("objects shorter than 8 bytes should fail");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_missing_trailing_magic() {
    let footer = b"serialized-parquet-footer";
    let mut object = b"row-group-bytes".to_vec();
    object.extend_from_slice(footer);
    object.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    object.extend_from_slice(b"NOPE");
    let (url, requests, server) =
        spawn_test_server(move |request| full_or_ranged_response(request, &object));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("footer bootstrap should reject missing trailing magic");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_zero_length_footers() {
    let mut object = b"row-group-bytes".to_vec();
    object.extend_from_slice(&0_u32.to_le_bytes());
    object.extend_from_slice(PARQUET_MAGIC);
    let (url, requests, server) =
        spawn_test_server(move |request| full_or_ranged_response(request, &object));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("zero-length footers should fail");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_footer_lengths_that_point_outside_the_object() {
    let mut object = b"row-group-bytes".to_vec();
    object.extend_from_slice(b"tiny");
    object.extend_from_slice(&64_u32.to_le_bytes());
    object.extend_from_slice(PARQUET_MAGIC);
    let (url, requests, server) =
        spawn_test_server(move |request| full_or_ranged_response(request, &object));
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("footer ranges that precede byte zero should fail");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_oversized_footers_before_fetching_them() {
    let declared_footer_length = 20_u32 * 1024 * 1024;
    let object_size = u64::from(declared_footer_length) + 8;
    let trailer = {
        let mut trailer = declared_footer_length.to_le_bytes().to_vec();
        trailer.extend_from_slice(PARQUET_MAGIC);
        trailer
    };
    let (url, requests, server) = spawn_test_server(move |request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
        TestResponse {
            status_line: "206 Partial Content",
            headers: vec![
                ("Content-Length".to_string(), trailer.len().to_string()),
                (
                    "Content-Range".to_string(),
                    format!(
                        "bytes {}-{}/{}",
                        object_size - 8,
                        object_size - 1,
                        object_size
                    ),
                ),
            ],
            body: trailer.clone(),
        }
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("oversized footers should fail before the second fetch");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_footer_reads_when_object_size_changes_between_requests() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let trailer = object[object.len() - 8..].to_vec();
    let object_len = object.len() as u64;
    let changed_object_len = object_len + 1;
    let expected_range_in_server = expected_range.clone();
    let (url, requests, server) =
        spawn_multi_request_server(2, move |request, index| match index {
            0 => {
                assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                    ],
                    body: trailer.clone(),
                }
            }
            1 => {
                assert_eq!(
                    request.headers.get("range"),
                    Some(&expected_range_in_server)
                );
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), footer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!(
                                "bytes {footer_offset}-{}",
                                footer_offset + footer.len() as u64 - 1
                            ) + &format!("/{changed_object_len}"),
                        ),
                    ],
                    body: footer.to_vec(),
                }
            }
            _ => unreachable!("only two requests are expected"),
        });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("object-size changes between footer reads should fail");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_rejects_footer_reads_when_entity_tag_changes_between_requests() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let trailer = object[object.len() - 8..].to_vec();
    let object_len = object.len();
    let expected_range_in_server = expected_range.clone();
    let (url, requests, server) =
        spawn_multi_request_server(2, move |request, index| match index {
            0 => {
                assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                        ("ETag".to_string(), "\"v1\"".to_string()),
                    ],
                    body: trailer.clone(),
                }
            }
            1 => {
                assert_eq!(
                    request.headers.get("range"),
                    Some(&expected_range_in_server)
                );
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), footer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!(
                                "bytes {footer_offset}-{}",
                                footer_offset + footer.len() as u64 - 1
                            ) + &format!("/{}", object_len),
                        ),
                        ("ETag".to_string(), "\"v2\"".to_string()),
                    ],
                    body: footer.to_vec(),
                }
            }
            _ => unreachable!("only two requests are expected"),
        });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("entity-tag changes between footer reads should fail");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_preserves_http_range_reader_errors_from_the_trailer_read() {
    let (url, requests, server) = spawn_test_server(|request| {
        assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
        TestResponse {
            status_line: "401 Unauthorized",
            headers: vec![("Content-Length".to_string(), "0".to_string())],
            body: Vec::new(),
        }
    });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("trailer-read failures should surface");

    let request = finish_request(server, requests);
    assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
    assert_eq!(error.code, QueryErrorCode::AccessDenied);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

#[test]
fn read_parquet_footer_preserves_http_range_reader_errors_from_the_footer_read() {
    let footer = b"serialized-parquet-footer";
    let prefix = b"row-group-bytes";
    let object = parquet_like_object(prefix, footer);
    let footer_offset = prefix.len() as u64;
    let expected_range = format!(
        "bytes={footer_offset}-{}",
        footer_offset + footer.len() as u64 - 1
    );
    let trailer = object[object.len() - 8..].to_vec();
    let object_len = object.len();
    let expected_range_in_server = expected_range.clone();
    let (url, requests, server) =
        spawn_multi_request_server(2, move |request, index| match index {
            0 => {
                assert_eq!(request.headers.get("range"), Some(&"bytes=-8".to_string()));
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                    ],
                    body: trailer.clone(),
                }
            }
            1 => {
                assert_eq!(
                    request.headers.get("range"),
                    Some(&expected_range_in_server)
                );
                TestResponse {
                    status_line: "200 OK",
                    headers: vec![("Content-Length".to_string(), footer.len().to_string())],
                    body: footer.to_vec(),
                }
            }
            _ => unreachable!("only two requests are expected"),
        });
    let source =
        BrowserObjectSource::from_url(&url).expect("loopback HTTP should be allowed in host tests");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default config should be supported");

    let error = runtime()
        .block_on(session.read_parquet_footer(&source))
        .expect_err("footer-read failures should surface");

    let requests = finish_requests(server, requests, 2);
    assert_eq!(
        requests[0].headers.get("range"),
        Some(&"bytes=-8".to_string())
    );
    assert_eq!(requests[1].headers.get("range"), Some(&expected_range));
    assert_eq!(error.code, QueryErrorCode::ObjectStoreProtocol);
    assert_eq!(error.fallback_reason, None);
    assert_eq!(error.target, ExecutionTarget::BrowserWasm);
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created for tests")
}

#[derive(Debug)]
struct CapturedRequest {
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn spawn_test_server<F>(handler: F) -> (String, Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: FnOnce(&CapturedRequest) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test client should connect");
        let request = read_request(&mut stream);
        let response = handler(&request);
        write_response(&mut stream, response);
        let _ = request_tx.send(request);
    });

    (url, request_rx, server)
}

fn spawn_stalling_server(delay: Duration) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test client should connect");
        let mut buffer = [0_u8; 4096];
        let _ = stream.read(&mut buffer);
        thread::sleep(delay);
    });

    (url, server)
}

fn spawn_stalling_footer_read_server(
    delay: Duration,
    trailer: Vec<u8>,
    object_len: usize,
) -> (String, Receiver<CapturedRequest>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        {
            let (mut trailer_stream, _) = listener.accept().expect("test client should connect");
            let trailer_request = read_request(&mut trailer_stream);
            write_response(
                &mut trailer_stream,
                TestResponse {
                    status_line: "206 Partial Content",
                    headers: vec![
                        ("Content-Length".to_string(), trailer.len().to_string()),
                        (
                            "Content-Range".to_string(),
                            format!("bytes {}-{}/{}", object_len - 8, object_len - 1, object_len),
                        ),
                    ],
                    body: trailer,
                },
            );
            let _ = request_tx.send(trailer_request);
        }

        let (mut footer_stream, _) = listener.accept().expect("test client should connect");
        let footer_request = read_request(&mut footer_stream);
        let _ = request_tx.send(footer_request);
        thread::sleep(delay);
    });

    (url, request_rx, server)
}

fn finish_request(server: JoinHandle<()>, requests: Receiver<CapturedRequest>) -> CapturedRequest {
    server.join().expect("test server should shut down cleanly");
    requests
        .recv()
        .expect("test should receive the captured request")
}

fn finish_requests(
    server: JoinHandle<()>,
    requests: Receiver<CapturedRequest>,
    expected_count: usize,
) -> Vec<CapturedRequest> {
    server.join().expect("test server should shut down cleanly");
    (0..expected_count)
        .map(|_| {
            requests
                .recv()
                .expect("test should receive the captured request")
        })
        .collect()
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
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
    let mut lines = text.split("\r\n");
    let _request_line = lines.next().expect("request line should be present");
    let mut headers = BTreeMap::new();

    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .expect("header line should contain a colon");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    CapturedRequest { headers }
}

fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
    if let Err(error) = try_write_response(stream, response) {
        if !is_expected_client_disconnect(&error) {
            panic!("response should be writable: {error}");
        }
    }
}

fn try_write_response(
    stream: &mut std::net::TcpStream,
    response: TestResponse,
) -> std::io::Result<()> {
    write!(stream, "HTTP/1.1 {}\r\n", response.status_line)?;
    for (header, value) in response.headers {
        write!(stream, "{header}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    stream.write_all(&response.body)?;
    stream.flush()
}

fn is_expected_client_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::NotConnected
    )
}

fn spawn_multi_request_server<F>(
    request_count: usize,
    handler: F,
) -> (String, Receiver<CapturedRequest>, JoinHandle<()>)
where
    F: Fn(&CapturedRequest, usize) -> TestResponse + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
    let address = listener.local_addr().expect("listener addr should resolve");
    let url = format!("http://{address}/object");
    let (request_tx, request_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        for index in 0..request_count {
            let (mut stream, _) = listener.accept().expect("test client should connect");
            let request = read_request(&mut stream);
            let response = handler(&request, index);
            write_response(&mut stream, response);
            let _ = request_tx.send(request);
        }
    });

    (url, request_rx, server)
}

fn full_or_ranged_response(request: &CapturedRequest, body: &[u8]) -> TestResponse {
    let Some(range_header) = request.headers.get("range") else {
        return TestResponse {
            status_line: "200 OK",
            headers: vec![("Content-Length".to_string(), body.len().to_string())],
            body: body.to_vec(),
        };
    };

    let (start, end) = resolve_range(range_header, body.len());
    if start > end || end >= body.len() {
        return TestResponse {
            status_line: "416 Range Not Satisfiable",
            headers: vec![
                ("Content-Length".to_string(), "0".to_string()),
                (
                    "Content-Range".to_string(),
                    format!("bytes */{}", body.len()),
                ),
            ],
            body: Vec::new(),
        };
    }

    let ranged = body[start..=end].to_vec();
    TestResponse {
        status_line: "206 Partial Content",
        headers: vec![
            ("Content-Length".to_string(), ranged.len().to_string()),
            (
                "Content-Range".to_string(),
                format!("bytes {start}-{end}/{}", body.len()),
            ),
        ],
        body: ranged,
    }
}

fn resolve_range(range_header: &str, object_len: usize) -> (usize, usize) {
    let range = range_header
        .strip_prefix("bytes=")
        .expect("test server expects byte ranges");

    if let Some(suffix) = range.strip_prefix('-') {
        let suffix_len = suffix.parse::<usize>().expect("suffix length should parse");
        let start = object_len.saturating_sub(suffix_len);
        return (start, object_len.saturating_sub(1));
    }

    let (start, end) = range
        .split_once('-')
        .expect("range should contain a start/end separator");
    let start = start.parse::<usize>().expect("range start should parse");

    if end.is_empty() {
        return (start, object_len.saturating_sub(1));
    }

    let end = end.parse::<usize>().expect("range end should parse");
    (start, end)
}

fn parquet_like_object(prefix: &[u8], footer: &[u8]) -> Vec<u8> {
    let mut object = prefix.to_vec();
    object.extend_from_slice(footer);
    object.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    object.extend_from_slice(PARQUET_MAGIC);
    object
}

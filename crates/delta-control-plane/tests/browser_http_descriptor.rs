mod support;

use std::collections::BTreeMap;

use delta_control_plane::{attach_browser_http_urls, resolve_snapshot};
use query_contract::{
    QueryErrorCode, ResolvedFileDescriptor, ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};
use support::TestTableFixture;
use wasm_query_runtime::{BrowserObjectSource, BrowserRuntimeConfig, BrowserRuntimeSession};

#[test]
fn attach_browser_http_urls_maps_latest_snapshot_files_with_stable_order() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?X-Goog-Signature=super-secret#fragment")
    });

    let browser_snapshot =
        attach_browser_http_urls(resolved_snapshot.clone(), &object_urls_by_path)
            .expect("browser http urls should attach");

    assert_eq!(browser_snapshot.table_uri, fixture.table_uri);
    assert_eq!(browser_snapshot.snapshot_version, 1);
    assert_eq!(
        browser_snapshot.active_files.len(),
        resolved_snapshot.active_files.len()
    );
    for (browser_file, resolved_file) in browser_snapshot
        .active_files
        .iter()
        .zip(resolved_snapshot.active_files.iter())
    {
        assert_eq!(browser_file.path, resolved_file.path);
        assert_eq!(browser_file.size_bytes, resolved_file.size_bytes);
        assert_eq!(
            browser_file.partition_values,
            resolved_file.partition_values
        );
        assert_eq!(browser_file.url, object_urls_by_path[&resolved_file.path]);
    }
}

#[test]
fn attach_browser_http_urls_preserves_historical_snapshot_version_and_file_set() {
    let fixture = TestTableFixture::create_multi_version();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: Some(1),
    })
    .expect("historical snapshot should resolve");
    let object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/historical/{path}?sig=history")
    });

    let browser_snapshot =
        attach_browser_http_urls(resolved_snapshot.clone(), &object_urls_by_path)
            .expect("browser http urls should attach");

    assert_eq!(browser_snapshot.table_uri, fixture.table_uri);
    assert_eq!(browser_snapshot.snapshot_version, 1);
    assert_eq!(
        browser_snapshot.active_files.len(),
        resolved_snapshot.active_files.len()
    );
    assert_eq!(browser_snapshot.active_files.len(), 1);
    assert_eq!(
        browser_snapshot.active_files[0].path,
        resolved_snapshot.active_files[0].path
    );
    assert_eq!(
        browser_snapshot.active_files[0].partition_values,
        resolved_snapshot.active_files[0].partition_values
    );
}

#[test]
fn attach_browser_http_urls_rejects_missing_active_file_urls() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let missing_path = resolved_snapshot.active_files[0].path.clone();
    let mut object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?sig=secret")
    });
    object_urls_by_path.remove(&missing_path);

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("missing active file urls should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains(&missing_path));
}

#[test]
fn attach_browser_http_urls_rejects_unexpected_extra_paths() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let mut object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?sig=secret")
    });
    let unexpected_path = "category=Z/part-999.parquet".to_string();
    object_urls_by_path.insert(
        unexpected_path.clone(),
        "https://signed.example.test/category=Z/part-999.parquet?sig=extra".to_string(),
    );

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("unexpected extra paths should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains(&unexpected_path));
}

#[test]
fn attach_browser_http_urls_rejects_invalid_url_syntax_without_leaking_query_or_fragment() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let target_path = resolved_snapshot.active_files[0].path.clone();
    let mut object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?sig=secret")
    });
    object_urls_by_path.insert(
        target_path.clone(),
        "https://signed example.test/object?sig=super-secret#fragment".to_string(),
    );

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("invalid URL syntax should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("https://signed example.test/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn attach_browser_http_urls_rejects_unsupported_schemes_without_leaking_query_or_fragment() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let target_path = resolved_snapshot.active_files[0].path.clone();
    let mut object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?sig=secret")
    });
    object_urls_by_path.insert(
        target_path,
        "ftp://signed.example.test/object?sig=super-secret#fragment".to_string(),
    );

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("unsupported schemes should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("ftp://signed.example.test/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn attach_browser_http_urls_rejects_non_loopback_plain_http_urls() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let target_path = resolved_snapshot.active_files[0].path.clone();
    let mut object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?sig=secret")
    });
    object_urls_by_path.insert(
        target_path,
        "http://example.com/object?sig=super-secret#fragment".to_string(),
    );

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("non-loopback plain HTTP should fail");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("http://example.com/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn attach_browser_http_urls_produces_https_urls_accepted_by_browser_object_source() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/browser/{path}?sig=secret")
    });

    let browser_snapshot = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect("browser http urls should attach");

    for file in browser_snapshot.active_files {
        let source =
            BrowserObjectSource::from_url(&file.url).expect("https urls should construct sources");
        assert_eq!(source.url(), file.url);
    }
}

#[test]
fn browser_http_descriptors_materialize_into_runtime_owned_object_sources() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/runtime/{path}?sig=secret")
    });
    let browser_snapshot =
        attach_browser_http_urls(resolved_snapshot.clone(), &object_urls_by_path)
            .expect("browser http urls should attach");
    let session = BrowserRuntimeSession::new(BrowserRuntimeConfig::default())
        .expect("default browser runtime config should be supported");

    let materialized = session
        .materialize_snapshot(&browser_snapshot)
        .expect("attached browser descriptors should materialize");

    assert_eq!(materialized.table_uri(), browser_snapshot.table_uri);
    assert_eq!(
        materialized.snapshot_version(),
        browser_snapshot.snapshot_version
    );
    assert_eq!(
        materialized.active_files().len(),
        browser_snapshot.active_files.len()
    );
    for (materialized_file, browser_file) in materialized
        .active_files()
        .iter()
        .zip(browser_snapshot.active_files.iter())
    {
        assert_eq!(materialized_file.path(), browser_file.path);
        assert_eq!(materialized_file.size_bytes(), browser_file.size_bytes);
        assert_eq!(
            materialized_file.partition_values(),
            &browser_file.partition_values
        );
        assert_eq!(materialized_file.object_source().url(), browser_file.url);
    }
}

#[test]
fn attach_browser_http_urls_rejects_loopback_plain_http_urls_for_browser_contracts() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");
    let target_path = resolved_snapshot.active_files[0].path.clone();
    let mut object_urls_by_path = build_url_map(&resolved_snapshot, |path| {
        format!("https://signed.example.test/{path}?sig=secret")
    });
    object_urls_by_path.insert(
        target_path,
        "http://127.0.0.1:8787/browser/object?sig=super-secret#fragment".to_string(),
    );

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("loopback plain HTTP should fail for browser-facing descriptors");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error
        .message
        .contains("http://127.0.0.1:8787/browser/object"));
    assert!(!error.message.contains("super-secret"));
    assert!(!error.message.contains("fragment"));
}

#[test]
fn attach_browser_http_urls_rejects_duplicate_resolved_paths() {
    let duplicate_path = "category=A/part-000.parquet".to_string();
    let resolved_snapshot = ResolvedSnapshotDescriptor {
        table_uri: "gs://axon-fixtures/sample_table".to_string(),
        snapshot_version: 12,
        active_files: vec![
            ResolvedFileDescriptor {
                path: duplicate_path.clone(),
                size_bytes: 128,
                partition_values: Default::default(),
            },
            ResolvedFileDescriptor {
                path: duplicate_path.clone(),
                size_bytes: 256,
                partition_values: Default::default(),
            },
        ],
    };
    let object_urls_by_path = BTreeMap::from([(
        duplicate_path.clone(),
        "https://signed.example.test/object".to_string(),
    )]);

    let error = attach_browser_http_urls(resolved_snapshot, &object_urls_by_path)
        .expect_err("duplicate resolved paths should fail one-to-one coverage checks");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains(&duplicate_path));
}

fn build_url_map<F>(
    resolved_snapshot: &query_contract::ResolvedSnapshotDescriptor,
    build_url: F,
) -> BTreeMap<String, String>
where
    F: Fn(&str) -> String,
{
    resolved_snapshot
        .active_files
        .iter()
        .map(|file| (file.path.clone(), build_url(&file.path)))
        .collect()
}

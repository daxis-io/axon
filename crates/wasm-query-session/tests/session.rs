#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;

#[path = "../../delta-control-plane/tests/support/mod.rs"]
mod support;

use delta_control_plane::resolve_snapshot;
use query_contract::{
    BrowserHttpFileDescriptor, BrowserHttpSnapshotDescriptor, ExecutionTarget, QueryErrorCode,
    QueryRequest, ResolvedSnapshotDescriptor, SnapshotResolutionRequest,
};
use support::{LoopbackObjectServer, TestTableFixture};
use wasm_query_runtime::{
    BrowserObjectSource, BrowserRuntimeConfig, MaterializedBrowserFile, MaterializedBrowserSnapshot,
};
use wasm_query_session::BrowserQuerySession;

#[test]
fn session_reuses_bootstrapped_snapshot_across_repeated_queries() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let materialized_file_count = materialized.active_files().len();
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");

    session
        .cache_table("events", materialized, None)
        .expect("session should cache runtime-owned snapshot state");

    let narrow_request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table WHERE category = 'A' ORDER BY id LIMIT 2",
    );
    let first_result = runtime()
        .block_on(session.sql("events", &narrow_request))
        .expect("first query should bootstrap and execute against the loopback snapshot");
    assert_eq!(first_result.runtime_result.row_count, 2);

    let cached_bootstrapped = session
        .table("events")
        .expect("table should remain cached")
        .bootstrapped_snapshot()
        .cloned()
        .expect("first query should populate cached bootstrap state");
    assert!(
        cached_bootstrapped.active_files().len() < materialized_file_count,
        "first-query bootstrap should stay pruned to the query-relevant files"
    );
    let narrow_plan = session
        .build_execution_plan("events", &narrow_request)
        .expect("cached bootstrap state should support repeated narrow planning");
    assert_eq!(narrow_plan.limit(), Some(2));
    assert_eq!(
        narrow_plan.scan().candidate_file_count(),
        u64::try_from(cached_bootstrapped.active_files().len())
            .expect("candidate file counts should fit into u64")
    );

    drop(server);

    let repeated_narrow_plan = session
        .build_execution_plan("events", &narrow_request)
        .expect("repeated planning should reuse the cached request-scoped bootstrap");
    assert_eq!(repeated_narrow_plan, narrow_plan);

    let broad_request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table ORDER BY id LIMIT 6",
    );
    let broad_error = session
        .build_execution_plan("events", &broad_request)
        .expect_err("broader follow-up planning must not reuse an incompatible cached bootstrap");
    assert_eq!(broad_error.code, QueryErrorCode::ExecutionFailed);

    assert_eq!(
        session
            .table("events")
            .expect("table should remain cached")
            .bootstrapped_snapshot(),
        Some(&cached_bootstrapped)
    );
}

#[test]
fn session_evicts_cached_tables_when_memory_budget_is_exceeded() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let materialized_bytes = materialized_bytes(&materialized);
    let mut session =
        BrowserQuerySession::new(BrowserRuntimeConfig::default(), materialized_bytes * 2)
            .expect("default browser runtime config should be supported");
    let narrow_request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table WHERE category = 'A' ORDER BY id LIMIT 2",
    );

    session
        .cache_table("events_a", materialized.clone(), None)
        .expect("first table should cache");
    assert!(session.contains_table("events_a"));
    assert_eq!(session.table_count(), 1);

    session
        .cache_table("events_b", materialized, None)
        .expect("second table should fit exactly at the materialized-only budget");
    assert_eq!(session.table_count(), 2);
    assert_eq!(session.cached_bytes(), materialized_bytes * 2);

    let query_result = runtime()
        .block_on(session.sql("events_b", &narrow_request))
        .expect("bootstrapping the active table should re-enforce the cache budget");
    assert_eq!(query_result.runtime_result.row_count, 2);

    assert!(!session.contains_table("events_a"));
    assert!(session.contains_table("events_b"));
    assert_eq!(session.table_count(), 1);
    assert!(session.cached_bytes() > materialized_bytes);
    assert!(session
        .table("events_b")
        .expect("remaining table should stay cached")
        .bootstrapped_snapshot()
        .is_some());
}

#[test]
fn disposing_a_table_releases_cached_snapshot_state() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let materialized_bytes = materialized_bytes(&materialized);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");
    let bootstrapped = runtime()
        .block_on(session.runtime().bootstrap_snapshot_metadata(&materialized))
        .expect("loopback snapshot metadata bootstrap should succeed");

    session
        .cache_table("events", materialized, Some(bootstrapped))
        .expect("table should cache");
    let cached_bytes = session.cached_bytes();
    assert!(cached_bytes > materialized_bytes);

    let removed = session
        .remove_table("events")
        .expect("disposing an open table should return its cached state");

    assert_eq!(
        removed.materialized_snapshot().table_uri(),
        fixture.table_uri.as_str()
    );
    assert!(removed.bootstrapped_snapshot().is_some());
    assert_eq!(session.cached_bytes(), 0);
    assert_eq!(session.table_count(), 0);
    assert!(!session.contains_table("events"));
}

#[test]
fn open_table_rejects_loopback_http_descriptors_even_in_host_tests() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let descriptor = loopback_descriptor(&resolved_snapshot, &server);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");

    let error = session
        .open_table("events", descriptor)
        .expect_err("browser-facing descriptors should remain HTTPS-only");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
    assert!(error.message.contains("must use HTTPS"));
}

#[test]
fn open_table_caches_https_descriptor_without_bootstrapping() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let descriptor = https_descriptor(&resolved_snapshot);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");

    session
        .open_table("events", descriptor.clone())
        .expect("https browser descriptors should materialize without I/O");

    assert_eq!(
        session
            .table("events")
            .expect("opened table should be cached")
            .descriptor(),
        Some(&descriptor)
    );
    assert!(session
        .table("events")
        .expect("opened table should be cached")
        .bootstrapped_snapshot()
        .is_none());
    assert_eq!(session.cached_bytes(), descriptor_bytes(&descriptor));
    assert!(session.dispose_table("events"));
    assert_eq!(session.table_count(), 0);
}

#[test]
fn session_sql_returns_explain_string_when_requested() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");
    session
        .cache_table("events", materialized, None)
        .expect("session should cache snapshot");

    let mut request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table WHERE category = 'A' LIMIT 2",
    );
    request.options.include_explain = true;

    let result = runtime()
        .block_on(session.sql("events", &request))
        .expect("query should execute");

    let explain = result
        .response
        .explain
        .as_deref()
        .expect("explain should be populated");
    assert!(
        explain.contains("BrowserExecutionPlan"),
        "explain text: {explain}"
    );
    assert!(explain.contains("table_uri"), "explain text: {explain}");
}

#[test]
fn session_sql_omits_explain_by_default() {
    let fixture = TestTableFixture::create_partitioned();
    let resolved_snapshot = resolve_latest_snapshot(&fixture);
    let server = LoopbackObjectServer::from_fixture_paths(
        &fixture,
        resolved_snapshot
            .active_files
            .iter()
            .map(|file| file.path.clone()),
    );
    let materialized = materialize_loopback_snapshot(&resolved_snapshot, &server);
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("default browser runtime config should be supported");
    session
        .cache_table("events", materialized, None)
        .expect("session should cache snapshot");

    let request = query_request(
        &resolved_snapshot.table_uri,
        resolved_snapshot.snapshot_version,
        "SELECT id FROM axon_table LIMIT 1",
    );

    let result = runtime()
        .block_on(session.sql("events", &request))
        .expect("query should execute");

    assert!(
        result.response.explain.is_none(),
        "explain must be None when include_explain is false; got: {:?}",
        result.response.explain
    );
}

fn resolve_latest_snapshot(fixture: &TestTableFixture) -> ResolvedSnapshotDescriptor {
    resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve")
}

fn loopback_descriptor(
    resolved_snapshot: &ResolvedSnapshotDescriptor,
    server: &LoopbackObjectServer,
) -> BrowserHttpSnapshotDescriptor {
    let object_urls_by_path = resolved_snapshot
        .active_files
        .iter()
        .map(|file| (file.path.clone(), server.url_for_path(&file.path)))
        .collect::<BTreeMap<_, _>>();

    BrowserHttpSnapshotDescriptor {
        table_uri: resolved_snapshot.table_uri.clone(),
        snapshot_version: resolved_snapshot.snapshot_version,
        partition_column_types: resolved_snapshot.partition_column_types.clone(),
        browser_compatibility: resolved_snapshot.browser_compatibility.clone(),
        required_capabilities: resolved_snapshot.required_capabilities.clone(),
        active_files: resolved_snapshot
            .active_files
            .iter()
            .map(|file| BrowserHttpFileDescriptor {
                url: object_urls_by_path
                    .get(&file.path)
                    .cloned()
                    .expect("loopback URL should exist for each resolved file"),
                path: file.path.clone(),
                size_bytes: file.size_bytes,
                partition_values: file.partition_values.clone(),
                stats: file.stats.clone(),
            })
            .collect(),
    }
}

fn https_descriptor(
    resolved_snapshot: &ResolvedSnapshotDescriptor,
) -> BrowserHttpSnapshotDescriptor {
    BrowserHttpSnapshotDescriptor {
        table_uri: resolved_snapshot.table_uri.clone(),
        snapshot_version: resolved_snapshot.snapshot_version,
        partition_column_types: resolved_snapshot.partition_column_types.clone(),
        browser_compatibility: resolved_snapshot.browser_compatibility.clone(),
        required_capabilities: resolved_snapshot.required_capabilities.clone(),
        active_files: resolved_snapshot
            .active_files
            .iter()
            .map(|file| BrowserHttpFileDescriptor {
                url: format!("https://example.invalid/{}", file.path),
                path: file.path.clone(),
                size_bytes: file.size_bytes,
                partition_values: file.partition_values.clone(),
                stats: file.stats.clone(),
            })
            .collect(),
    }
}

fn materialize_loopback_snapshot(
    resolved_snapshot: &ResolvedSnapshotDescriptor,
    server: &LoopbackObjectServer,
) -> MaterializedBrowserSnapshot {
    let files = resolved_snapshot
        .active_files
        .iter()
        .map(|file| {
            MaterializedBrowserFile::new(
                file.path.clone(),
                file.size_bytes,
                file.partition_values.clone(),
                BrowserObjectSource::from_url(server.url_for_path(&file.path))
                    .expect("loopback HTTP should be allowed in host tests"),
            )
        })
        .collect::<Vec<_>>();

    MaterializedBrowserSnapshot::new_with_partition_column_types(
        resolved_snapshot.table_uri.clone(),
        resolved_snapshot.snapshot_version,
        files,
        resolved_snapshot.partition_column_types.clone(),
    )
    .expect("duplicate-free snapshots should construct")
}

fn materialized_bytes(snapshot: &MaterializedBrowserSnapshot) -> u64 {
    snapshot
        .active_files()
        .iter()
        .map(|file| file.size_bytes())
        .sum()
}

fn descriptor_bytes(descriptor: &BrowserHttpSnapshotDescriptor) -> u64 {
    descriptor
        .active_files
        .iter()
        .map(|file| file.size_bytes)
        .sum()
}

fn query_request(table_uri: &str, snapshot_version: i64, sql: &str) -> QueryRequest {
    let mut request = QueryRequest::new(table_uri, sql, ExecutionTarget::BrowserWasm);
    request.snapshot_version = Some(snapshot_version);
    request
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime should be created")
}

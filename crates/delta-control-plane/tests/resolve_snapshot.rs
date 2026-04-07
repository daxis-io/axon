mod support;

use std::path::PathBuf;

use delta_control_plane::{resolve_snapshot, resolve_snapshot_with_policy, SnapshotAccessPolicy};
use deltalake::kernel::{DataType, MetadataValue, PrimitiveType, StructField};
use deltalake::table::config::TableProperty;
use query_contract::{CapabilityKey, CapabilityState, QueryErrorCode, SnapshotResolutionRequest};
use serde_json::json;
use support::TestTableFixture;
use tempfile::TempDir;

#[test]
fn resolve_snapshot_returns_latest_descriptor_with_sorted_active_files() {
    let fixture = TestTableFixture::create_partitioned();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("latest snapshot should resolve");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
    assert_eq!(descriptor.snapshot_version, 1);
    assert_eq!(
        descriptor.partition_column_types,
        std::collections::BTreeMap::from([(
            "category".to_string(),
            query_contract::PartitionColumnType::String,
        )])
    );
    assert_eq!(
        descriptor.browser_compatibility,
        descriptor.required_capabilities
    );
    assert!(descriptor.required_capabilities.capabilities.is_empty());
    assert_eq!(descriptor.active_files, fixture.expected_active_files(None));
}

#[test]
fn resolve_snapshot_rejects_unknown_protocol_features() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![],
    );
    fixture.overwrite_initial_protocol(unknown_protocol_feature_protocol());

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect_err("unknown protocol features must hard fail during trusted snapshot resolution");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(error.message.contains("mysteryFeature"));
}

#[test]
fn resolve_snapshot_reports_change_data_feed_as_native_only_browser_compatibility() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![(
            TableProperty::EnableChangeDataFeed.as_ref().to_string(),
            Some("true".to_string()),
        )],
    );

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("change data feed tables should still resolve into a metadata descriptor");

    assert_eq!(
        descriptor
            .browser_compatibility
            .state(CapabilityKey::ChangeDataFeed),
        Some(CapabilityState::NativeOnly)
    );
    assert_eq!(
        descriptor.required_capabilities,
        descriptor.browser_compatibility
    );
}

#[test]
fn resolve_snapshot_rejects_append_only_tables_as_terminal_browser_unsupported() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![("delta.appendOnly".to_string(), Some("true".to_string()))],
    );

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("append-only tables are outside the browser compatibility matrix");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(error.message.contains("appendOnly"));
}

#[test]
fn resolve_snapshot_rejects_legacy_writer_terminal_features() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![legacy_terminal_feature_field()],
        vec![],
    );
    fixture.overwrite_initial_metadata_configuration(vec![(
        "delta.constraints.id_positive".to_string(),
        Some("id > 0".to_string()),
    )]);
    fixture.overwrite_initial_protocol(json!({
        "minReaderVersion": 1,
        "minWriterVersion": 4,
    }));

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("legacy writer feature inference should still reject terminal browser states");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(error.message.contains("checkConstraints"));
    assert!(error.message.contains("generatedColumns"));
    assert!(error.message.contains("invariants"));
}

#[test]
fn resolve_snapshot_allows_known_but_disabled_v7_features() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![],
    );
    fixture.overwrite_initial_protocol(json!({
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["typeWidening"],
        "writerFeatures": ["typeWidening", "rowTracking", "inCommitTimestamp", "icebergCompatV1"],
    }));

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("known but disabled property-gated features should not hard fail");

    assert!(descriptor.required_capabilities.capabilities.is_empty());
}

#[test]
fn resolve_snapshot_classifies_known_protocol_features_before_unknown_feature_failures() {
    let fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![],
    );
    fixture.overwrite_initial_protocol(json!({
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["catalogManaged"],
        "writerFeatures": ["catalogManaged"],
    }));

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("known unsupported features should not fall through as unknown");

    assert_eq!(error.code, QueryErrorCode::UnsupportedFeature);
    assert!(error.message.contains("catalogManaged"));
    assert!(!error.message.contains("unknown Delta protocol"));
}

#[test]
fn resolve_snapshot_honors_explicit_snapshot_version() {
    let fixture = TestTableFixture::create_multi_version();
    let table_uri = fixture.table_uri.clone();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri,
        snapshot_version: Some(1),
    })
    .expect("historical snapshot should resolve");

    assert_eq!(descriptor.snapshot_version, 1);
    assert_eq!(
        descriptor.active_files,
        fixture.expected_active_files(Some(1))
    );
}

#[test]
fn resolve_snapshot_accepts_raw_local_paths() {
    let fixture = TestTableFixture::create_partitioned();
    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.raw_table_path(),
        snapshot_version: None,
    })
    .expect("raw local paths should resolve");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
    assert_eq!(descriptor.active_files, fixture.expected_active_files(None));
}

#[test]
fn resolve_snapshot_accepts_file_urls() {
    let fixture = TestTableFixture::create_partitioned();
    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("file urls should resolve");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
    assert_eq!(descriptor.active_files, fixture.expected_active_files(None));
}

#[test]
fn resolve_snapshot_trims_whitespace_around_table_uri() {
    let fixture = TestTableFixture::create_partitioned();
    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: format!("  {}  ", fixture.raw_table_path()),
        snapshot_version: None,
    })
    .expect("whitespace-trimmed raw paths should resolve");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
    assert_eq!(descriptor.active_files, fixture.expected_active_files(None));
}

#[test]
fn resolve_snapshot_rejects_negative_snapshot_versions() {
    let fixture = TestTableFixture::create_multi_version();

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: Some(-1),
    })
    .expect_err("negative versions should be rejected");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("snapshot_version"));
}

#[test]
fn resolve_snapshot_rejects_unknown_snapshot_versions() {
    let fixture = TestTableFixture::create_multi_version();

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: Some(99),
    })
    .expect_err("unknown versions should fail deterministically");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("snapshot"));
}

#[test]
fn resolve_snapshot_rejects_invalid_table_locations() {
    let missing_table = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("does-not-exist")
        .display()
        .to_string();

    let error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: missing_table,
        snapshot_version: None,
    })
    .expect_err("invalid table locations should fail");

    assert_eq!(error.code, QueryErrorCode::InvalidRequest);
    assert!(error.message.contains("table"));
}

#[test]
fn resolve_snapshot_with_policy_allows_listed_tables() {
    let fixture = TestTableFixture::create_partitioned();
    let mut policy = SnapshotAccessPolicy::default();
    policy
        .allow_table(&format!("  {}  ", fixture.raw_table_path()))
        .expect("allow rule should accept equivalent local table locators");

    let descriptor = resolve_snapshot_with_policy(
        SnapshotResolutionRequest {
            table_uri: fixture.table_uri.clone(),
            snapshot_version: None,
        },
        &policy,
    )
    .expect("listed tables should resolve");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
    assert_eq!(descriptor.active_files, fixture.expected_active_files(None));
}

#[test]
fn resolve_snapshot_with_policy_rejects_unlisted_tables_when_allow_list_is_present() {
    let allowed_fixture = TestTableFixture::create_partitioned();
    let requested_fixture = TestTableFixture::create_multi_version();
    let mut policy = SnapshotAccessPolicy::default();
    policy
        .allow_table(&allowed_fixture.table_uri)
        .expect("allow rule should accept normalized table uris");

    let error = resolve_snapshot_with_policy(
        SnapshotResolutionRequest {
            table_uri: requested_fixture.table_uri,
            snapshot_version: None,
        },
        &policy,
    )
    .expect_err("unlisted tables should be denied when allow rules exist");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
}

#[test]
fn resolve_snapshot_with_policy_denies_equivalent_locator_forms_before_snapshot_io() {
    let fixture = TestTableFixture::create_partitioned();
    let mut policy = SnapshotAccessPolicy::default();
    policy
        .deny_table(&fixture.raw_table_path())
        .expect("deny rule should accept raw local table paths");

    let error = resolve_snapshot_with_policy(
        SnapshotResolutionRequest {
            table_uri: format!("  {}  ", fixture.table_uri.trim_end_matches('/')),
            snapshot_version: None,
        },
        &policy,
    )
    .expect_err("denied tables should be rejected before any snapshot resolution happens");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
}

#[test]
fn resolve_snapshot_with_policy_denies_equivalent_remote_locator_forms_before_snapshot_io() {
    let mut policy = SnapshotAccessPolicy::default();
    policy
        .deny_table("memory://bucket/table")
        .expect("deny rule should accept remote table uris");

    let error = resolve_snapshot_with_policy(
        SnapshotResolutionRequest {
            table_uri: "memory://bucket//table".to_string(),
            snapshot_version: None,
        },
        &policy,
    )
    .expect_err("equivalent remote locators should be denied before storage resolution");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
}

#[test]
fn resolve_snapshot_with_policy_prefers_denies_over_allow_rules() {
    let fixture = TestTableFixture::create_partitioned();
    let mut policy = SnapshotAccessPolicy::default();
    policy
        .allow_table(&fixture.table_uri)
        .expect("allow rule should be accepted");
    policy
        .deny_table(&fixture.raw_table_path())
        .expect("deny rule should accept equivalent local table locators");

    let error = resolve_snapshot_with_policy(
        SnapshotResolutionRequest {
            table_uri: fixture.table_uri,
            snapshot_version: None,
        },
        &policy,
    )
    .expect_err("deny rules should win over allow rules");

    assert_eq!(error.code, QueryErrorCode::SecurityPolicyViolation);
}

fn unknown_protocol_feature_protocol() -> serde_json::Value {
    json!({
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["mysteryFeature"],
        "writerFeatures": ["mysteryFeature"],
    })
}

#[test]
fn resolve_snapshot_without_policy_remains_permissive() {
    let fixture = TestTableFixture::create_partitioned();

    let descriptor = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("compatibility wrapper should remain allow-all");

    assert_eq!(descriptor.table_uri, fixture.table_uri);
}

#[test]
fn denied_invalid_table_locations_return_security_policy_violation_before_storage_errors() {
    let non_delta_dir = TempDir::new().expect("tempdir should be created");
    let non_delta_table_uri = non_delta_dir.path().display().to_string();
    let mut policy = SnapshotAccessPolicy::default();
    policy
        .deny_table(&non_delta_table_uri)
        .expect("deny rule should accept an existing directory");

    let denied_error = resolve_snapshot_with_policy(
        SnapshotResolutionRequest {
            table_uri: non_delta_table_uri.clone(),
            snapshot_version: None,
        },
        &policy,
    )
    .expect_err("denied locations should fail before delta snapshot loading");
    let permissive_error = resolve_snapshot(SnapshotResolutionRequest {
        table_uri: non_delta_table_uri,
        snapshot_version: None,
    })
    .expect_err("the same location should otherwise surface its storage error");

    assert_eq!(denied_error.code, QueryErrorCode::SecurityPolicyViolation);
    assert_eq!(permissive_error.code, QueryErrorCode::InvalidRequest);
}

fn legacy_terminal_feature_field() -> StructField {
    StructField::new(
        "id".to_string(),
        DataType::Primitive(PrimitiveType::Integer),
        false,
    )
    .with_metadata([
        (
            "delta.generationExpression",
            MetadataValue::String("id".to_string()),
        ),
        (
            "delta.invariants",
            MetadataValue::String("{\"expression\":{\"expression\":\"id > 0\"}}".to_string()),
        ),
    ])
}

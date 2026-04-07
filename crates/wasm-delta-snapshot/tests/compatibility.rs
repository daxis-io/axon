#[path = "../../delta-control-plane/tests/support/mod.rs"]
mod support;

use delta_control_plane::resolve_snapshot as resolve_control_plane_snapshot;
use deltalake::kernel::{DataType, MetadataValue, PrimitiveType, StructField};
use query_contract::{CapabilityKey, CapabilityState, QueryErrorCode, SnapshotResolutionRequest};
use serde_json::json;
use support::TestTableFixture;
use wasm_delta_snapshot::{
    DefaultJsonHandler, DefaultParquetHandler, LocalFileStorageHandler, SnapshotResolver,
};

#[test]
fn unsupported_protocol_features_classify_as_native_only_or_terminal() {
    let native_only_fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![(
            "delta.enableChangeDataFeed".to_string(),
            Some("true".to_string()),
        )],
    );
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_snapshot = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: native_only_fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect("native-only fixtures should still resolve into a metadata descriptor");
    let native_snapshot = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: native_only_fixture.table_uri.clone(),
        snapshot_version: None,
    })
    .expect("trusted control-plane should resolve native-only fixtures");

    assert_eq!(
        browser_snapshot
            .browser_compatibility
            .state(CapabilityKey::ChangeDataFeed),
        Some(CapabilityState::NativeOnly)
    );
    assert_eq!(
        browser_snapshot.browser_compatibility,
        native_snapshot.browser_compatibility
    );

    let terminal_fixture = TestTableFixture::create_with_columns_and_configuration(
        vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        )],
        vec![("delta.appendOnly".to_string(), Some("true".to_string()))],
    );

    let browser_error = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: terminal_fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect_err("out-of-matrix Delta features should hard fail as terminal unsupported");
    let native_error = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: terminal_fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("trusted control-plane should agree on terminal unsupported features");

    assert_eq!(browser_error.code, QueryErrorCode::UnsupportedFeature);
    assert_eq!(native_error.code, QueryErrorCode::UnsupportedFeature);
    assert!(browser_error.message.contains("appendOnly"));
    assert!(native_error.message.contains("appendOnly"));
}

#[test]
fn legacy_writer_features_still_classify_with_terminal_browser_routing() {
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
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_error = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect_err("legacy writer feature inference should still classify terminal features");
    let native_error = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("trusted control-plane should agree on legacy writer feature inference");

    assert_eq!(browser_error.code, QueryErrorCode::UnsupportedFeature);
    assert_eq!(native_error.code, QueryErrorCode::UnsupportedFeature);
    assert!(browser_error.message.contains("checkConstraints"));
    assert!(browser_error.message.contains("generatedColumns"));
    assert!(browser_error.message.contains("invariants"));
    assert!(native_error.message.contains("checkConstraints"));
    assert!(native_error.message.contains("generatedColumns"));
    assert!(native_error.message.contains("invariants"));
}

#[test]
fn known_but_disabled_v7_features_do_not_hard_fail_browser_routing() {
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
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_snapshot = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect("disabled property-gated features should not hard fail browser routing");
    let native_snapshot = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect("trusted control-plane should agree on disabled property-gated features");

    assert_eq!(
        browser_snapshot.browser_compatibility,
        native_snapshot.browser_compatibility
    );
    assert!(browser_snapshot
        .required_capabilities
        .capabilities
        .is_empty());
    assert!(native_snapshot
        .required_capabilities
        .capabilities
        .is_empty());
}

#[test]
fn known_protocol_features_do_not_fall_back_to_unknown_feature_failures() {
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
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_error = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect_err(
            "known but unsupported features should classify before unknown-feature failures",
        );
    let native_error = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("trusted control-plane should classify known unsupported features");

    assert_eq!(browser_error.code, QueryErrorCode::UnsupportedFeature);
    assert_eq!(native_error.code, QueryErrorCode::UnsupportedFeature);
    assert!(browser_error.message.contains("catalogManaged"));
    assert!(native_error.message.contains("catalogManaged"));
    assert!(!browser_error.message.contains("unknown Delta protocol"));
    assert!(!native_error.message.contains("unknown Delta protocol"));
}

#[test]
fn unknown_protocol_features_hard_fail_in_browser_and_control_plane() {
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
        "readerFeatures": ["mysteryFeature"],
        "writerFeatures": ["mysteryFeature"],
    }));
    let resolver = SnapshotResolver::new(
        LocalFileStorageHandler::default(),
        DefaultJsonHandler::default(),
        DefaultParquetHandler::default(),
    );

    let browser_error = tokio::runtime::Runtime::new()
        .expect("runtime should be created")
        .block_on(async {
            resolver
                .resolve_snapshot(SnapshotResolutionRequest {
                    table_uri: fixture.raw_table_path(),
                    snapshot_version: None,
                })
                .await
        })
        .expect_err("unknown protocol features must hard fail in browser snapshot resolution");
    let native_error = resolve_control_plane_snapshot(SnapshotResolutionRequest {
        table_uri: fixture.table_uri,
        snapshot_version: None,
    })
    .expect_err("unknown protocol features must hard fail in the trusted control-plane");

    assert_eq!(browser_error.code, QueryErrorCode::UnsupportedFeature);
    assert_eq!(native_error.code, QueryErrorCode::UnsupportedFeature);
    assert!(browser_error.message.contains("mysteryFeature"));
    assert!(native_error.message.contains("mysteryFeature"));
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

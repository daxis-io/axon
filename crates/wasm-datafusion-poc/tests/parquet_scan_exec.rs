#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow_array::{
    cast::AsArray, Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DoubleType, FloatType, Int32Type, Int64Type,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::{
    BrokeredDeltaAccessMode, BrokeredDeltaReadPlan, BrokeredObjectAccess, BrokeredPolicyAuthority,
    DirectExternalEngineReadSupport, ObjectGrantBatchSignRequest, ObjectGrantBatchSignResponse,
    ObjectGrantHeadRequest, ObjectGrantListRequest, ObjectGrantListResponse, ObjectGrantObject,
    ObjectGrantRangeRequest, ObjectGrantSignedUrl, PartitionColumnType, PolicyAuthorityKind,
    QueryError, QueryErrorCode, ResolvedFileDescriptor, ResolvedSnapshotDescriptor,
    SnapshotResolutionRequest,
};
use wasm_datafusion_poc::{
    DeltaActiveFile, DeltaTableDescriptor, DeltaTableFieldDataType, DeltaTableSchema,
    DeltaTableSchemaField, WasmDataFusionEngine,
};
use wasm_delta_snapshot::{
    BrokeredDeltaLogStorageHandler, DefaultJsonHandler, DefaultParquetHandler, SnapshotResolver,
};
use wasm_http_object_store::{BrokerFuture, BrokeredObjectStore, ObjectGrantBrokerClient};

mod support;
use support::RequestCapturingServer;

static PARQUET_SCAN_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn delta_descriptor_scan_streams_parquet_batches_through_datafusion() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, value \
             FROM events \
             WHERE category = 'B' AND value > 10 \
             ORDER BY id",
            )
            .await
            .expect("DataFusion should execute SQL over browser Parquet scan batches");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![12, 25]);
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "DataFusion scan should delegate Parquet object reads to browser range I/O"
        );
    });
}

#[test]
fn delta_descriptor_scan_preserves_rows_for_count_star() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches("SELECT COUNT(*) AS rows FROM events")
            .await
            .expect("DataFusion should count rows from browser Parquet scan batches");

        assert_eq!(schema.field(0).name(), "rows");
        assert_eq!(int64_column_values(&batches, 0), vec![3]);
    });
}

#[test]
fn delta_descriptor_scan_aligns_normalized_parquet_names_to_projected_schema() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                mixed_case_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT \"Id\", \"Value\" \
             FROM events \
             WHERE \"Category\" = 'B' AND \"Value\" > 10 \
             ORDER BY \"Id\"",
            )
            .await
            .expect("DataFusion should align normalized Parquet output to projected schema");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["Id", "Value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![12, 25]);
    });
}

#[test]
fn delta_descriptor_scan_streams_unannotated_byte_array_columns_as_binary() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_binary_payload(&[
            (1, vec![0, 1, 2, 3]),
            (2, vec![b'a', b'x', b'o', b'n']),
        ]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                binary_payload_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register a binary Parquet field");

        let (schema, batches) = engine
            .sql_to_record_batches("SELECT id, payload FROM events ORDER BY id")
            .await
            .expect("DataFusion should execute SQL over unannotated BYTE_ARRAY columns");

        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2]);
        assert_eq!(
            binary_column_values(&batches, 1),
            vec![vec![0, 1, 2, 3], vec![b'a', b'x', b'o', b'n']]
        );
    });
}

#[test]
fn delta_descriptor_scan_streams_json_byte_array_columns_as_utf8() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_annotated_byte_array_payload(
            "JSON",
            &[
                (1, br#"{"source":"axon"}"#.to_vec()),
                (2, br#"{"source":"delta"}"#.to_vec()),
            ],
        );
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                utf8_payload_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register a JSON Parquet field as UTF8");

        let (schema, batches) = engine
            .sql_to_record_batches("SELECT id, payload FROM events ORDER BY id")
            .await
            .expect("DataFusion should execute SQL over JSON BYTE_ARRAY columns");

        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2]);
        assert_eq!(
            utf8_column_values(&batches, 1),
            vec![
                r#"{"source":"axon"}"#.to_string(),
                r#"{"source":"delta"}"#.to_string()
            ]
        );
    });
}

#[test]
fn delta_descriptor_scan_streams_enum_byte_array_columns_as_binary() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_annotated_byte_array_payload(
            "ENUM",
            &[(1, b"bronze".to_vec()), (2, b"silver".to_vec())],
        );
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                binary_payload_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register an ENUM Parquet field as binary");

        let (schema, batches) = engine
            .sql_to_record_batches("SELECT id, payload FROM events ORDER BY id")
            .await
            .expect("DataFusion should execute SQL over ENUM BYTE_ARRAY columns");

        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2]);
        assert_eq!(
            binary_column_values(&batches, 1),
            vec![b"bronze".to_vec(), b"silver".to_vec()]
        );
    });
}

#[test]
fn delta_descriptor_scan_streams_raw_wkb_like_geometry_geography_bytes_as_binary() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        // These are opaque WKB-like payloads. This test only proves raw bytes
        // survive as Arrow Binary, not geospatial functions or predicates.
        let geometry_one = vec![1, 1, 0, 0, 0];
        let geometry_two = vec![1, 2, 0, 0, 0];
        let geography_one = vec![2, 1, 0, 0, 0];
        let geography_two = vec![2, 2, 0, 0, 0];
        let object = parquet_bytes_with_wkb_like_payloads(&[
            (1, geometry_one.clone(), geography_one.clone()),
            (2, geometry_two.clone(), geography_two.clone()),
        ]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                raw_wkb_like_payload_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register GEOMETRY/GEOGRAPHY payloads as binary");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, geometry_payload, geography_payload FROM events ORDER BY id",
            )
            .await
            .expect("DataFusion should execute SQL over raw binary GEOMETRY/GEOGRAPHY payloads");

        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
        assert_eq!(schema.field(2).data_type(), &DataType::Binary);
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2]);
        assert_eq!(
            binary_column_values(&batches, 1),
            vec![geometry_one, geometry_two]
        );
        assert_eq!(
            binary_column_values(&batches, 2),
            vec![geography_one, geography_two]
        );
    });
}

#[test]
fn browser_readable_required_and_optional_utf8_columns_scan_through_datafusion() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_required_and_optional_utf8(&[
            (1, "alpha", Some("stable")),
            (2, "beta", None),
            (3, "alpha", Some("stable")),
        ]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                utf8_required_optional_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register required and optional UTF8 fields");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, required_name, optional_label FROM events ORDER BY id",
            )
            .await
            .expect("DataFusion should execute SQL over required and optional UTF8 columns");

        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert!(schema.field(2).is_nullable());
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2, 3]);
        assert_eq!(
            utf8_column_values(&batches, 1),
            vec!["alpha".to_string(), "beta".to_string(), "alpha".to_string()]
        );
        assert_eq!(
            optional_utf8_column_values(&batches, 2),
            vec![Some("stable".to_string()), None, Some("stable".to_string())]
        );
    });
}

#[test]
fn browser_readable_primitive_columns_scan_with_optional_nulls_through_datafusion() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_optional_primitives(&[
            (1, Some(true), Some(11), Some(101), Some(1.5), Some(10.25)),
            (2, None, None, None, None, None),
            (3, Some(false), Some(33), Some(303), Some(3.5), Some(30.75)),
        ]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                primitive_optional_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register browser-readable primitive fields");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, maybe_bool, maybe_i32, maybe_i64, maybe_f32, maybe_f64 \
                 FROM events ORDER BY id",
            )
            .await
            .expect("DataFusion should execute SQL over browser-readable primitive columns");

        assert_eq!(schema.field(1).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(2).data_type(), &DataType::Int32);
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
        assert_eq!(schema.field(4).data_type(), &DataType::Float32);
        assert_eq!(schema.field(5).data_type(), &DataType::Float64);
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2, 3]);
        assert_eq!(
            optional_bool_column_values(&batches, 1),
            vec![Some(true), None, Some(false)]
        );
        assert_eq!(
            optional_int32_column_values(&batches, 2),
            vec![Some(11), None, Some(33)]
        );
        assert_eq!(
            optional_int64_column_values(&batches, 3),
            vec![Some(101), None, Some(303)]
        );
        assert_eq!(
            optional_float32_column_values(&batches, 4),
            vec![Some(1.5), None, Some(3.5)]
        );
        assert_eq!(
            optional_float64_column_values(&batches, 5),
            vec![Some(10.25), None, Some(30.75)]
        );
    });
}

#[test]
fn browser_readable_binary_json_byte_array_scan_with_optional_nulls() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_optional_binary_and_json(&[
            (
                1,
                Some(vec![0, 1, 2, 3]),
                Some(br#"{"source":"axon"}"#.to_vec()),
            ),
            (2, None, None),
            (
                3,
                Some(vec![b'a', b'x', b'o', b'n']),
                Some(br#"{"source":"delta"}"#.to_vec()),
            ),
        ]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(delta_descriptor(
                "events",
                binary_json_optional_descriptor_schema(),
                server.url(),
                object_size,
            ))
            .await
            .expect("Delta descriptor should register binary and JSON BYTE_ARRAY fields");

        let (schema, batches) = engine
            .sql_to_record_batches("SELECT id, payload, json_payload FROM events ORDER BY id")
            .await
            .expect("DataFusion should execute SQL over binary and JSON BYTE_ARRAY columns");

        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert!(schema.field(2).is_nullable());
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2, 3]);
        assert_eq!(
            optional_binary_column_values(&batches, 1),
            vec![
                Some(vec![0, 1, 2, 3]),
                None,
                Some(vec![b'a', b'x', b'o', b'n'])
            ]
        );
        assert_eq!(
            optional_utf8_column_values(&batches, 2),
            vec![
                Some(r#"{"source":"axon"}"#.to_string()),
                None,
                Some(r#"{"source":"delta"}"#.to_string())
            ]
        );
    });
}

#[test]
fn brokered_delta_read_plan_opens_datafusion_table_and_returns_fixture_rows() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let plan = brokered_delta_plan();
        let resolved_snapshot = ResolvedSnapshotDescriptor {
            table_uri: plan.table_root.clone(),
            snapshot_version: 11,
            partition_column_types: BTreeMap::from([(
                "category".to_string(),
                PartitionColumnType::String,
            )]),
            browser_compatibility: Default::default(),
            required_capabilities: Default::default(),
            active_files: vec![ResolvedFileDescriptor {
                path: "category=B/part-000.parquet".to_string(),
                size_bytes: object_size,
                partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
                stats: Some(r#"{"numRecords":3}"#.to_string()),
            }],
        };
        let snapshot = plan
            .to_browser_http_snapshot_descriptor(
                resolved_snapshot,
                &BTreeMap::from([("category=B/part-000.parquet".to_string(), server.url())]),
            )
            .expect("brokered Delta read plan should adapt to the browser descriptor");
        let descriptor = DeltaTableDescriptor::from_browser_http_snapshot(
            "events",
            &snapshot,
            brokered_delta_datafusion_schema(),
        )
        .expect("browser descriptor should convert to a DataFusion table descriptor");
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(descriptor)
            .await
            .expect("brokered descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, value \
             FROM events \
             WHERE category = 'B' \
             ORDER BY id",
            )
            .await
            .expect("DataFusion should query rows opened through the brokered plan path");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![1, 2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![5, 12, 25]);
    });
}

#[test]
fn brokered_delta_runtime_replays_log_signs_files_and_queries_datafusion() {
    let _guard = PARQUET_SCAN_TEST_LOCK
        .lock()
        .expect("Parquet scan tests should serialize local HTTP servers");
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_columns(&[(3, 25), (1, 5), (2, 12)]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let plan = brokered_delta_plan();
        let commit = delta_log_commit_for_data_file(object_size);
        let client = FakeBrokerClient::with_state(FakeBrokerState {
            objects: BTreeMap::from([(
                "_delta_log/00000000000000000000.json".to_string(),
                Bytes::from(commit),
            )]),
            signed_urls: BTreeMap::from([(
                "category=B/part-000.parquet".to_string(),
                server.url(),
            )]),
            ..FakeBrokerState::default()
        });
        let object_store = BrokeredObjectStore::new(
            plan.grant_id.clone(),
            BrokeredObjectAccess {
                list: true,
                head: true,
                get: true,
                range_get: true,
                batch_sign: false,
                proxy_range: true,
            },
            client.clone(),
        );
        let resolver = SnapshotResolver::new(
            BrokeredDeltaLogStorageHandler::new(plan.table_root.clone(), object_store),
            DefaultJsonHandler::default(),
            DefaultParquetHandler::default(),
        );

        let resolved_snapshot = resolver
            .resolve_snapshot(SnapshotResolutionRequest {
                table_uri: plan.table_root.clone(),
                snapshot_version: None,
            })
            .await
            .expect("brokered object grant should reconstruct the Delta snapshot");
        let signed_urls = client
            .batch_sign(
                &plan.grant_id,
                ObjectGrantBatchSignRequest {
                    paths: resolved_snapshot
                        .active_files
                        .iter()
                        .map(|file| file.path.clone())
                        .collect(),
                },
            )
            .await
            .expect("broker should sign active data files")
            .signed_urls
            .into_iter()
            .map(|signed| (signed.path, signed.url))
            .collect::<BTreeMap<_, _>>();
        let snapshot = plan
            .to_browser_http_snapshot_descriptor(resolved_snapshot, &signed_urls)
            .expect("brokered Delta read plan should adapt to the browser descriptor");
        let descriptor = DeltaTableDescriptor::from_browser_http_snapshot(
            "events",
            &snapshot,
            brokered_delta_datafusion_schema(),
        )
        .expect("browser descriptor should convert to a DataFusion table descriptor");
        let mut engine = WasmDataFusionEngine::new();

        engine
            .open_delta_table(descriptor)
            .await
            .expect("brokered descriptor should register as a DataFusion table");

        let (schema, batches) = engine
            .sql_to_record_batches(
                "SELECT id, value \
             FROM events \
             WHERE category = 'B' AND value > 10 \
             ORDER BY id",
            )
            .await
            .expect("DataFusion should query rows opened through the brokered runtime path");

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
        assert_eq!(int64_column_values(&batches, 0), vec![2, 3]);
        assert_eq!(int64_column_values(&batches, 1), vec![12, 25]);
        assert_eq!(
            client.state().list_calls,
            vec![("grant-456".to_string(), "_delta_log".to_string())]
        );
        assert!(
            client
                .state()
                .proxy_range_calls
                .iter()
                .any(|(_, request)| request.path == "_delta_log/00000000000000000000.json"),
            "snapshot replay should read Delta log bytes through the broker proxy"
        );
        assert_eq!(
            client.state().batch_sign_calls,
            vec![(
                "grant-456".to_string(),
                vec!["category=B/part-000.parquet".to_string()]
            )]
        );
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "DataFusion scan should read signed Parquet URLs through browser range I/O"
        );
    });
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("test runtime should construct")
}

fn delta_descriptor(
    table_name: &str,
    schema: SchemaRef,
    url: String,
    size_bytes: u64,
) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: table_name.to_string(),
        table_version: 11,
        schema,
        partition_columns: vec!["category".to_string()],
        partition_column_types: BTreeMap::from([(
            "category".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![DeltaActiveFile {
            path: "category=B/part-000.parquet".to_string(),
            url,
            size_bytes,
            partition_values: BTreeMap::from([("category".to_string(), Some("B".to_string()))]),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":3}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn mixed_case_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("Value", DataType::Int64, false),
        Field::new("Category", DataType::Utf8, true),
    ]))
}

fn binary_payload_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn utf8_payload_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn raw_wkb_like_payload_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("geometry_payload", DataType::Binary, false),
        Field::new("geography_payload", DataType::Binary, false),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn utf8_required_optional_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("required_name", DataType::Utf8, false),
        Field::new("optional_label", DataType::Utf8, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn primitive_optional_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("maybe_bool", DataType::Boolean, true),
        Field::new("maybe_i32", DataType::Int32, true),
        Field::new("maybe_i64", DataType::Int64, true),
        Field::new("maybe_f32", DataType::Float32, true),
        Field::new("maybe_f64", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn binary_json_optional_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, true),
        Field::new("json_payload", DataType::Utf8, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn brokered_delta_datafusion_schema() -> DeltaTableSchema {
    DeltaTableSchema::new(vec![
        DeltaTableSchemaField::new("id", DeltaTableFieldDataType::Int64, false),
        DeltaTableSchemaField::new("value", DeltaTableFieldDataType::Int64, false),
        DeltaTableSchemaField::new("category", DeltaTableFieldDataType::Utf8, true),
    ])
}

fn brokered_delta_plan() -> BrokeredDeltaReadPlan {
    BrokeredDeltaReadPlan {
        table_id: "tbl-123".to_string(),
        full_name: "main.sales.orders".to_string(),
        table_root: "s3://prod-bucket/tables/orders".to_string(),
        grant_id: "grant-456".to_string(),
        expires_at_epoch_ms: 1_800_000_000_000,
        delta_access_mode: BrokeredDeltaAccessMode::DeltaLog,
        policy_authority: BrokeredPolicyAuthority {
            authority: PolicyAuthorityKind::UnityCatalog,
            direct_external_engine_read: DirectExternalEngineReadSupport::Confirmed,
        },
        object_access: BrokeredObjectAccess {
            list: true,
            head: true,
            get: true,
            range_get: true,
            batch_sign: true,
            proxy_range: true,
        },
    }
}

fn delta_log_commit_for_data_file(size_bytes: u64) -> String {
    format!(
        "{}{}{}",
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
        "{\"metaData\":{\"id\":\"test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\
         \"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},{\\\"name\\\":\\\"value\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},{\\\"name\\\":\\\"category\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\
         \"partitionColumns\":[\"category\"],\"configuration\":{}}}\n",
        format!(
            "{{\"add\":{{\"path\":\"category=B/part-000.parquet\",\"size\":{size_bytes},\"partitionValues\":{{\"category\":\"B\"}},\"stats\":\"{{\\\"numRecords\\\":3}}\"}}}}\n"
        )
    )
}

fn parquet_bytes_with_i64_columns(rows: &[(i64, i64)]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 value; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
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

fn parquet_bytes_with_binary_payload(rows: &[(i64, Vec<u8>)]) -> Vec<u8> {
    parquet_bytes_with_byte_array_payload("REQUIRED BYTE_ARRAY payload", rows)
}

fn parquet_bytes_with_annotated_byte_array_payload(
    annotation: &str,
    rows: &[(i64, Vec<u8>)],
) -> Vec<u8> {
    parquet_bytes_with_byte_array_payload(
        &format!("REQUIRED BYTE_ARRAY payload ({annotation})"),
        rows,
    )
}

fn parquet_bytes_with_byte_array_payload(
    payload_column_schema: &str,
    rows: &[(i64, Vec<u8>)],
) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type(&format!(
            "message schema {{ REQUIRED INT64 id; {payload_column_schema}; }}"
        ))
        .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let payloads = rows
        .iter()
        .map(|(_, payload)| ByteArray::from(payload.clone()))
        .collect::<Vec<_>>();

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
        .expect("payload column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&payloads, None, None)
            .expect("payload values should write");
        column.close().expect("payload column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

fn parquet_bytes_with_wkb_like_payloads(rows: &[(i64, Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type(
            "message schema { \
             REQUIRED INT64 id; \
             REQUIRED BYTE_ARRAY geometry_payload (GEOMETRY); \
             REQUIRED BYTE_ARRAY geography_payload (GEOGRAPHY); \
             }",
        )
        .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let geometries = rows
        .iter()
        .map(|(_, geometry, _)| ByteArray::from(geometry.clone()))
        .collect::<Vec<_>>();
    let geographies = rows
        .iter()
        .map(|(_, _, geography)| ByteArray::from(geography.clone()))
        .collect::<Vec<_>>();

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
        .expect("geometry_payload column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&geometries, None, None)
            .expect("geometry_payload values should write");
        column
            .close()
            .expect("geometry_payload column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("geography_payload column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&geographies, None, None)
            .expect("geography_payload values should write");
        column
            .close()
            .expect("geography_payload column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

fn parquet_bytes_with_required_and_optional_utf8(rows: &[(i64, &str, Option<&str>)]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type(
            "message schema { \
             REQUIRED INT64 id; \
             REQUIRED BYTE_ARRAY required_name (UTF8); \
             OPTIONAL BYTE_ARRAY optional_label (UTF8); \
             }",
        )
        .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let required_names = rows
        .iter()
        .map(|(_, required_name, _)| ByteArray::from(required_name.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    let optional_labels = rows
        .iter()
        .filter_map(|(_, _, optional_label)| {
            optional_label.map(|value| ByteArray::from(value.as_bytes().to_vec()))
        })
        .collect::<Vec<_>>();
    let optional_label_def_levels = rows
        .iter()
        .map(|(_, _, optional_label)| i16::from(optional_label.is_some()))
        .collect::<Vec<_>>();

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
        .expect("required_name column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&required_names, None, None)
            .expect("required_name values should write");
        column
            .close()
            .expect("required_name column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("optional_label column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&optional_labels, Some(&optional_label_def_levels), None)
            .expect("optional_label values should write");
        column
            .close()
            .expect("optional_label column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

type OptionalPrimitiveRow = (
    i64,
    Option<bool>,
    Option<i32>,
    Option<i64>,
    Option<f32>,
    Option<f64>,
);

fn parquet_bytes_with_optional_primitives(rows: &[OptionalPrimitiveRow]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type(
            "message schema { \
             REQUIRED INT64 id; \
             OPTIONAL BOOLEAN maybe_bool; \
             OPTIONAL INT32 maybe_i32; \
             OPTIONAL INT64 maybe_i64; \
             OPTIONAL FLOAT maybe_f32; \
             OPTIONAL DOUBLE maybe_f64; \
             }",
        )
        .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let bools = rows.iter().filter_map(|row| row.1).collect::<Vec<_>>();
    let bool_def_levels = rows
        .iter()
        .map(|row| i16::from(row.1.is_some()))
        .collect::<Vec<_>>();
    let i32s = rows.iter().filter_map(|row| row.2).collect::<Vec<_>>();
    let i32_def_levels = rows
        .iter()
        .map(|row| i16::from(row.2.is_some()))
        .collect::<Vec<_>>();
    let i64s = rows.iter().filter_map(|row| row.3).collect::<Vec<_>>();
    let i64_def_levels = rows
        .iter()
        .map(|row| i16::from(row.3.is_some()))
        .collect::<Vec<_>>();
    let f32s = rows.iter().filter_map(|row| row.4).collect::<Vec<_>>();
    let f32_def_levels = rows
        .iter()
        .map(|row| i16::from(row.4.is_some()))
        .collect::<Vec<_>>();
    let f64s = rows.iter().filter_map(|row| row.5).collect::<Vec<_>>();
    let f64_def_levels = rows
        .iter()
        .map(|row| i16::from(row.5.is_some()))
        .collect::<Vec<_>>();

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
        .expect("maybe_bool column writer should be returned")
    {
        column
            .typed::<BoolType>()
            .write_batch(&bools, Some(&bool_def_levels), None)
            .expect("maybe_bool values should write");
        column
            .close()
            .expect("maybe_bool column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("maybe_i32 column writer should be returned")
    {
        column
            .typed::<Int32Type>()
            .write_batch(&i32s, Some(&i32_def_levels), None)
            .expect("maybe_i32 values should write");
        column
            .close()
            .expect("maybe_i32 column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("maybe_i64 column writer should be returned")
    {
        column
            .typed::<Int64Type>()
            .write_batch(&i64s, Some(&i64_def_levels), None)
            .expect("maybe_i64 values should write");
        column
            .close()
            .expect("maybe_i64 column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("maybe_f32 column writer should be returned")
    {
        column
            .typed::<FloatType>()
            .write_batch(&f32s, Some(&f32_def_levels), None)
            .expect("maybe_f32 values should write");
        column
            .close()
            .expect("maybe_f32 column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("maybe_f64 column writer should be returned")
    {
        column
            .typed::<DoubleType>()
            .write_batch(&f64s, Some(&f64_def_levels), None)
            .expect("maybe_f64 values should write");
        column
            .close()
            .expect("maybe_f64 column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

fn parquet_bytes_with_optional_binary_and_json(
    rows: &[(i64, Option<Vec<u8>>, Option<Vec<u8>>)],
) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type(
            "message schema { \
             REQUIRED INT64 id; \
             OPTIONAL BYTE_ARRAY payload; \
             OPTIONAL BYTE_ARRAY json_payload (JSON); \
             }",
        )
        .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let payloads = rows
        .iter()
        .filter_map(|(_, payload, _)| payload.clone().map(ByteArray::from))
        .collect::<Vec<_>>();
    let payload_def_levels = rows
        .iter()
        .map(|(_, payload, _)| i16::from(payload.is_some()))
        .collect::<Vec<_>>();
    let json_payloads = rows
        .iter()
        .filter_map(|(_, _, json_payload)| json_payload.clone().map(ByteArray::from))
        .collect::<Vec<_>>();
    let json_payload_def_levels = rows
        .iter()
        .map(|(_, _, json_payload)| i16::from(json_payload.is_some()))
        .collect::<Vec<_>>();

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
        .expect("payload column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&payloads, Some(&payload_def_levels), None)
            .expect("payload values should write");
        column.close().expect("payload column writer should close");
    }
    if let Some(mut column) = row_group
        .next_column()
        .expect("json_payload column writer should be returned")
    {
        column
            .typed::<ByteArrayType>()
            .write_batch(&json_payloads, Some(&json_payload_def_levels), None)
            .expect("json_payload values should write");
        column
            .close()
            .expect("json_payload column writer should close");
    }
    row_group.close().expect("row-group writer should close");
    writer.close().expect("file writer should close");
    bytes
}

fn int64_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column_index)
                .as_primitive::<arrow_array::types::Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn utf8_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column should be an Arrow StringArray");
            (0..array.len())
                .map(|index| array.value(index).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn binary_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<Vec<u8>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("column should be an Arrow BinaryArray");
            (0..array.len())
                .map(|index| array.value(index).to_vec())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_utf8_column_values(
    batches: &[RecordBatch],
    column_index: usize,
) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column should be an Arrow StringArray");
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index).to_string())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_binary_column_values(
    batches: &[RecordBatch],
    column_index: usize,
) -> Vec<Option<Vec<u8>>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("column should be an Arrow BinaryArray");
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index).to_vec())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_bool_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<Option<bool>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("column should be an Arrow BooleanArray");
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_int32_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("column should be an Arrow Int32Array");
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_int64_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_primitive::<arrow_array::types::Int64Type>();
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_float32_column_values(
    batches: &[RecordBatch],
    column_index: usize,
) -> Vec<Option<f32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("column should be an Arrow Float32Array");
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_float64_column_values(
    batches: &[RecordBatch],
    column_index: usize,
) -> Vec<Option<f64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("column should be an Arrow Float64Array");
            (0..array.len())
                .map(|index| {
                    if array.is_null(index) {
                        None
                    } else {
                        Some(array.value(index))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct FakeBrokerClient {
    state: Arc<Mutex<FakeBrokerState>>,
}

#[derive(Debug, Default)]
struct FakeBrokerState {
    objects: BTreeMap<String, Bytes>,
    signed_urls: BTreeMap<String, String>,
    list_calls: Vec<(String, String)>,
    head_calls: Vec<(String, String)>,
    batch_sign_calls: Vec<(String, Vec<String>)>,
    proxy_range_calls: Vec<(String, ObjectGrantRangeRequest)>,
}

impl FakeBrokerClient {
    fn with_state(state: FakeBrokerState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeBrokerState> {
        self.state.lock().expect("fake broker state lock")
    }
}

impl ObjectGrantBrokerClient for FakeBrokerClient {
    fn list<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantListRequest,
    ) -> BrokerFuture<'a, ObjectGrantListResponse> {
        Box::pin(async move {
            let mut state = self.state();
            state
                .list_calls
                .push((grant_id.to_string(), request.prefix.clone()));
            Ok(ObjectGrantListResponse {
                objects: state
                    .objects
                    .iter()
                    .filter(|(path, _)| path.starts_with(&request.prefix))
                    .map(|(path, bytes)| ObjectGrantObject {
                        path: path.clone(),
                        size_bytes: bytes.len() as u64,
                        etag: None,
                    })
                    .collect(),
            })
        })
    }

    fn head<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantHeadRequest,
    ) -> BrokerFuture<'a, ObjectGrantObject> {
        Box::pin(async move {
            let mut state = self.state();
            state
                .head_calls
                .push((grant_id.to_string(), request.path.clone()));
            state
                .objects
                .get(&request.path)
                .map(|bytes| ObjectGrantObject {
                    path: request.path,
                    size_bytes: bytes.len() as u64,
                    etag: None,
                })
                .ok_or_else(|| {
                    QueryError::new(
                        QueryErrorCode::ObjectNotFound,
                        "fake broker object was missing",
                        query_contract::ExecutionTarget::BrowserWasm,
                    )
                })
        })
    }

    fn batch_sign<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantBatchSignRequest,
    ) -> BrokerFuture<'a, ObjectGrantBatchSignResponse> {
        Box::pin(async move {
            let mut state = self.state();
            state
                .batch_sign_calls
                .push((grant_id.to_string(), request.paths.clone()));
            Ok(ObjectGrantBatchSignResponse {
                signed_urls: request
                    .paths
                    .iter()
                    .map(|path| ObjectGrantSignedUrl {
                        path: path.clone(),
                        url: state
                            .signed_urls
                            .get(path)
                            .cloned()
                            .expect("fake broker should have a signed URL for active files"),
                        expires_at_epoch_ms: 1_800_000_000_000,
                    })
                    .collect(),
            })
        })
    }

    fn proxy_range<'a>(
        &'a self,
        grant_id: &'a str,
        request: ObjectGrantRangeRequest,
    ) -> BrokerFuture<'a, Bytes> {
        Box::pin(async move {
            let mut state = self.state();
            state
                .proxy_range_calls
                .push((grant_id.to_string(), request.clone()));
            let bytes = state.objects.get(&request.path).cloned().ok_or_else(|| {
                QueryError::new(
                    QueryErrorCode::ObjectNotFound,
                    "fake broker range object was missing",
                    query_contract::ExecutionTarget::BrowserWasm,
                )
            })?;
            Ok(bytes.slice(request.start as usize..request.end as usize))
        })
    }
}

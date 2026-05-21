#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{cast::AsArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{ScanArgs, TableProvider};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use query_contract::PartitionColumnType;
use wasm_datafusion_poc::{
    AxonDeltaTableProvider, AxonParquetScanExec, DeltaActiveFile, DeltaTableDescriptor,
};

#[tokio::test]
async fn scan_trace_records_projected_columns_and_limit_from_datafusion() {
    let ctx = context_with_events();
    let plan = physical_plan(&ctx, "SELECT id FROM events LIMIT 2").await;
    let scan = axon_scan(&plan);
    let trace = scan.pushdown_trace();

    assert_eq!(trace.projected_columns, vec!["id"]);
    assert_eq!(trace.limit, Some(2));
    assert_eq!(trace.files_skipped, 0);
    assert_eq!(trace.row_groups_skipped, 0);
    assert_eq!(trace.bytes_fetched, 0);
    assert_eq!(trace.rows_emitted, 0);
}

#[tokio::test]
async fn pushed_limit_bounds_rows_emitted_by_scan() {
    let ctx = context_with_single_partition_events();
    let plan = physical_plan(&ctx, "SELECT id FROM events LIMIT 1").await;
    let scan = axon_scan(&plan);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let trace = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![1]);
    assert_eq!(trace.limit, Some(1));
    assert_eq!(trace.rows_emitted, 1);
}

#[tokio::test]
async fn residual_filter_prevents_scan_level_limit_pushdown() {
    let ctx = context_with_single_partition_events();
    let plan = physical_plan(&ctx, "SELECT id FROM events WHERE value = 25 LIMIT 1").await;
    let scan = axon_scan(&plan);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let trace = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![2]);
    assert_eq!(trace.limit, None);
    assert_eq!(trace.projected_columns, vec!["id", "value"]);
    assert_eq!(trace.inexact_filters, Vec::<String>::new());
    assert_eq!(trace.rows_emitted, 3);
}

#[tokio::test]
async fn direct_residual_scan_does_not_push_limit_into_scan() {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    let provider = AxonDeltaTableProvider::with_record_batch_partitions(
        delta_descriptor(Arc::clone(&schema)),
        controlled_partitions(schema),
    );
    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("value"))),
        Operator::Gt,
        Box::new(Expr::Literal(ScalarValue::Int32(Some(10)), None)),
    ));
    let projection = [0_usize, 1_usize];
    let filters = [filter];
    let scan_result = provider
        .scan_with_args(
            &ctx.state(),
            ScanArgs::default()
                .with_projection(Some(&projection))
                .with_filters(Some(&filters))
                .with_limit(Some(1)),
        )
        .await
        .expect("scan_with_args should build a scan plan");
    let plan = Arc::clone(scan_result.plan());
    let scan = axon_scan(&plan);
    let trace = scan.pushdown_trace();

    assert_eq!(trace.limit, None);
    assert_eq!(trace.projected_columns, vec!["id", "value"]);
    assert_eq!(trace.inexact_filters, vec!["value > Int32(10)"]);
}

#[tokio::test]
async fn exact_partition_filter_prunes_files_in_scan_trace() {
    let ctx = context_with_events();
    let plan = physical_plan(
        &ctx,
        "SELECT id FROM events WHERE event_date = '2026-01-02'",
    )
    .await;
    let scan = axon_scan(&plan);
    let trace = scan.pushdown_trace();

    assert_eq!(trace.projected_columns, vec!["id"]);
    assert_eq!(trace.files_total, 3);
    assert_eq!(trace.files_planned, 1);
    assert_eq!(trace.files_skipped, 2);
    assert_eq!(
        trace.planned_file_paths,
        vec!["event_date=2026-01-02/part-001.parquet"]
    );
}

#[tokio::test]
async fn exact_partition_prune_keeps_value_filter_as_residual() {
    let ctx = context_with_events();
    let plan = physical_plan(
        &ctx,
        "SELECT id \
         FROM events \
         WHERE event_date = '2026-01-02' AND value > 10 \
         ORDER BY id",
    )
    .await;
    let scan = axon_scan(&plan);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let trace = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![4]);
    assert_eq!(trace.projected_columns, vec!["id", "value"]);
    assert_eq!(trace.files_planned, 1);
    assert_eq!(trace.files_skipped, 2);
    assert_eq!(
        trace.planned_file_paths,
        vec!["event_date=2026-01-02/part-001.parquet"]
    );
    assert_eq!(
        trace.exact_filters,
        vec!["event_date = Utf8(\"2026-01-02\")"]
    );
    assert_eq!(trace.inexact_filters, Vec::<String>::new());
    assert_eq!(trace.rows_emitted, 2);
}

#[tokio::test]
async fn file_stats_prune_keeps_row_predicate_as_residual() {
    let ctx = context_with_value_stats_events();
    let plan = physical_plan(
        &ctx,
        "SELECT id \
         FROM events \
         WHERE value > 10 AND id != 4 \
         ORDER BY id",
    )
    .await;
    let scan = axon_scan(&plan);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let trace = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![5]);
    assert_eq!(trace.projected_columns, vec!["id", "value"]);
    assert_eq!(trace.files_total, 3);
    assert_eq!(trace.files_planned, 2);
    assert_eq!(trace.files_skipped, 1);
    assert_eq!(
        trace.planned_file_paths,
        vec!["stats/part-001.parquet", "stats/part-002.parquet",]
    );
    assert_eq!(trace.exact_filters, Vec::<String>::new());
    assert_eq!(trace.inexact_filters, vec!["value > Int32(10)"]);
}

#[tokio::test]
async fn null_equality_partition_filter_is_not_exact_pushdown() {
    let provider = AxonDeltaTableProvider::with_record_batch_partitions(
        nullable_delta_descriptor(),
        nullable_partitions(nullable_descriptor_schema()),
    );
    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("event_date"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Null, None)),
    ));

    let pushdown = provider
        .supports_filters_pushdown(&[&filter])
        .expect("pushdown support should be classified");

    assert_eq!(pushdown, vec![TableProviderFilterPushDown::Unsupported]);
}

#[tokio::test]
async fn supports_filters_pushdown_distinguishes_exact_inexact_and_unsupported() {
    let schema = descriptor_schema();
    let provider = AxonDeltaTableProvider::with_record_batch_partitions(
        delta_descriptor(Arc::clone(&schema)),
        controlled_partitions(schema),
    );
    let event_date_is_null = Expr::IsNull(Box::new(Expr::Column(Column::new_unqualified(
        "event_date",
    ))));
    let event_date_is_not_null = Expr::IsNotNull(Box::new(Expr::Column(Column::new_unqualified(
        "event_date",
    ))));
    let value_in = datafusion::prelude::col("value").in_list(
        vec![
            datafusion::prelude::lit(7_i32),
            datafusion::prelude::lit(25_i32),
        ],
        false,
    );
    let partition_and_residual = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::new_unqualified("event_date"))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("2026-01-02".to_string())),
                None,
            )),
        ))),
        Operator::And,
        Box::new(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::new_unqualified("value"))),
            Operator::Gt,
            Box::new(Expr::Literal(ScalarValue::Int32(Some(10)), None)),
        ))),
    ));

    let pushdown = provider
        .supports_filters_pushdown(&[
            &event_date_is_null,
            &event_date_is_not_null,
            &value_in,
            &partition_and_residual,
        ])
        .expect("pushdown support should classify mixed filters");

    assert_eq!(
        pushdown,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Unsupported,
            TableProviderFilterPushDown::Unsupported,
            TableProviderFilterPushDown::Inexact,
        ]
    );
}

#[tokio::test]
async fn supports_filters_pushdown_marks_stats_and_row_group_pruning_inexact() {
    let schema = descriptor_schema();
    let stats_provider = AxonDeltaTableProvider::with_record_batch_partitions(
        value_stats_delta_descriptor(Arc::clone(&schema)),
        value_stats_partitions(schema),
    );
    let value_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("value"))),
        Operator::Gt,
        Box::new(Expr::Literal(ScalarValue::Int32(Some(10)), None)),
    ));

    let stats_pushdown = stats_provider
        .supports_filters_pushdown(&[&value_filter])
        .expect("stats pushdown support should classify");
    assert_eq!(stats_pushdown, vec![TableProviderFilterPushDown::Inexact]);

    let parquet_provider = AxonDeltaTableProvider::new(parquet_delta_descriptor(
        "http://127.0.0.1/object.parquet".to_string(),
        1024,
    ));
    let id_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("id"))),
        Operator::GtEq,
        Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
    ));

    let row_group_pushdown = parquet_provider
        .supports_filters_pushdown(&[&id_filter])
        .expect("row-group pushdown support should classify");
    assert_eq!(
        row_group_pushdown,
        vec![TableProviderFilterPushDown::Inexact]
    );
}

#[tokio::test]
async fn null_partition_comparisons_stay_conservative_and_correct() {
    let ctx = context_with_nullable_events();

    let is_null_plan = physical_plan(&ctx, "SELECT id FROM events WHERE event_date IS NULL").await;
    let is_null_scan = axon_scan(&is_null_plan);
    let is_null_batches = collect(Arc::clone(&is_null_plan), ctx.task_ctx())
        .await
        .expect("IS NULL query should execute");
    let is_null_trace = is_null_scan.pushdown_trace();

    assert_eq!(int32_column_values(&is_null_batches, 0), vec![1]);
    assert_eq!(is_null_trace.files_planned, 1);
    assert_eq!(is_null_trace.files_skipped, 1);
    assert_eq!(is_null_trace.exact_filters, vec!["event_date IS NULL"]);

    let is_not_null_plan =
        physical_plan(&ctx, "SELECT id FROM events WHERE event_date IS NOT NULL").await;
    let is_not_null_scan = axon_scan(&is_not_null_plan);
    let is_not_null_batches = collect(Arc::clone(&is_not_null_plan), ctx.task_ctx())
        .await
        .expect("IS NOT NULL query should execute");
    let is_not_null_trace = is_not_null_scan.pushdown_trace();

    assert_eq!(int32_column_values(&is_not_null_batches, 0), vec![2]);
    assert_eq!(is_not_null_trace.files_planned, 2);
    assert_eq!(is_not_null_trace.files_skipped, 0);
    assert_eq!(is_not_null_trace.inexact_filters, Vec::<String>::new());

    let provider = AxonDeltaTableProvider::with_record_batch_partitions(
        nullable_delta_descriptor(),
        nullable_partitions(nullable_descriptor_schema()),
    );
    let null_equality = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("event_date"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Null, None)),
    ));

    let pushdown = provider
        .supports_filters_pushdown(&[&null_equality])
        .expect("pushdown support should classify = NULL");
    assert_eq!(pushdown, vec![TableProviderFilterPushDown::Unsupported]);
}

#[tokio::test]
async fn scan_with_args_delegates_projection_filters_and_limit_to_scan_path() {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    let provider = AxonDeltaTableProvider::with_record_batch_partitions(
        delta_descriptor(Arc::clone(&schema)),
        controlled_partitions(schema),
    );
    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("event_date"))),
        Operator::Eq,
        Box::new(Expr::Literal(
            ScalarValue::Utf8(Some("2026-01-02".to_string())),
            None,
        )),
    ));
    let projection = [0_usize];
    let filters = [filter];
    let scan_result = provider
        .scan_with_args(
            &ctx.state(),
            ScanArgs::default()
                .with_projection(Some(&projection))
                .with_filters(Some(&filters))
                .with_limit(Some(1)),
        )
        .await
        .expect("scan_with_args should build a scan plan");
    let plan = Arc::clone(scan_result.plan());
    let scan = axon_scan(&plan);
    let trace = scan.pushdown_trace();

    assert_eq!(trace.projected_columns, vec!["id"]);
    assert_eq!(trace.limit, Some(1));
    assert_eq!(
        trace.exact_filters,
        vec!["event_date = Utf8(\"2026-01-02\")"]
    );
    assert_eq!(
        trace.planned_file_paths,
        vec!["event_date=2026-01-02/part-001.parquet"]
    );
}

#[tokio::test]
async fn partition_and_nonpartition_in_lists_keep_residuals_correct() {
    let ctx = context_with_events();
    let partition_in_plan = physical_plan(
        &ctx,
        "SELECT id \
         FROM events \
         WHERE event_date IN ('2026-01-01', '2026-01-03') AND value > 20 \
         ORDER BY id",
    )
    .await;
    let partition_in_scan = axon_scan(&partition_in_plan);
    let partition_in_batches = collect(Arc::clone(&partition_in_plan), ctx.task_ctx())
        .await
        .expect("partition IN query should execute");
    let partition_in_trace = partition_in_scan.pushdown_trace();

    assert_eq!(int32_column_values(&partition_in_batches, 0), vec![2, 5]);
    assert_eq!(partition_in_trace.files_planned, 2);
    assert_eq!(partition_in_trace.files_skipped, 1);
    assert_eq!(
        partition_in_trace.exact_filters,
        vec!["event_date = Utf8(\"2026-01-01\") OR event_date = Utf8(\"2026-01-03\")"]
    );
    assert_eq!(partition_in_trace.inexact_filters, Vec::<String>::new());

    let nonpartition_in_plan = physical_plan(
        &ctx,
        "SELECT id FROM events WHERE value IN (7, 25) ORDER BY id LIMIT 1",
    )
    .await;
    let nonpartition_in_scan = axon_scan(&nonpartition_in_plan);
    let nonpartition_in_batches = collect(Arc::clone(&nonpartition_in_plan), ctx.task_ctx())
        .await
        .expect("non-partition IN query should execute");
    let nonpartition_in_trace = nonpartition_in_scan.pushdown_trace();

    assert_eq!(int32_column_values(&nonpartition_in_batches, 0), vec![2]);
    assert_eq!(nonpartition_in_trace.limit, None);
    assert_eq!(nonpartition_in_trace.files_planned, 3);
    assert_eq!(nonpartition_in_trace.inexact_filters, Vec::<String>::new());
}

#[tokio::test]
async fn inexact_residual_filter_stays_above_partition_pruned_scan() {
    let ctx = context_with_events();
    let df = ctx
        .sql(
            "SELECT id \
             FROM events \
             WHERE event_date = '2026-01-02' AND value > 10 \
             ORDER BY id",
        )
        .await
        .expect("SQL should plan");
    let plan = df
        .create_physical_plan()
        .await
        .expect("physical plan should build");
    let scan = axon_scan(&plan);
    let before = scan.pushdown_trace();

    assert_eq!(before.files_skipped, 2);
    assert_eq!(before.projected_columns, vec!["id", "value"]);

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("query should execute");
    let after = scan.pushdown_trace();

    assert_eq!(int32_column_values(&batches, 0), vec![4]);
    assert_eq!(after.files_skipped, 2);
    assert_eq!(after.rows_emitted, 2);
    assert_eq!(after.bytes_fetched, 0);
    assert_eq!(after.row_groups_skipped, 0);
}

#[test]
fn parquet_row_group_prune_keeps_row_predicate_as_residual() {
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_row_groups(&[&[1_i64, 2, 3], &[10_i64, 11, 12]]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let ctx = SessionContext::new();

        ctx.register_table(
            "events",
            Arc::new(AxonDeltaTableProvider::new(parquet_delta_descriptor(
                server.url(),
                object_size,
            ))),
        )
        .expect("table should register");

        let plan = physical_plan(
            &ctx,
            "SELECT id FROM events WHERE id >= 10 AND id <> 11 ORDER BY id",
        )
        .await;
        let scan = axon_scan(&plan);
        let batches = collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("query should execute");
        let trace = scan.pushdown_trace();

        assert_eq!(int64_column_values(&batches, 0), vec![10, 12]);
        assert_eq!(trace.projected_columns, vec!["id"]);
        assert_eq!(trace.files_planned, 1);
        assert_eq!(trace.files_skipped, 0);
        assert_eq!(trace.row_groups_touched, 1);
        assert_eq!(trace.row_groups_skipped, 1);
        assert_eq!(trace.inexact_filters, vec!["id >= Int64(10)"]);
        assert_eq!(trace.rows_emitted, 3);
    });
}

#[test]
fn parquet_row_group_pruning_updates_scan_trace_and_keeps_residual_correctness() {
    test_runtime().block_on(async {
        let object = parquet_bytes_with_i64_row_groups(&[&[1_i64, 2, 3], &[10_i64, 11, 12]]);
        let object_size = u64::try_from(object.len()).expect("object size should fit in u64");
        let server = RequestCapturingServer::new(object);
        let ctx = SessionContext::new();

        ctx.register_table(
            "events",
            Arc::new(AxonDeltaTableProvider::new(parquet_delta_descriptor(
                server.url(),
                object_size,
            ))),
        )
        .expect("table should register");

        let plan = physical_plan(&ctx, "SELECT id FROM events WHERE id >= 10 ORDER BY id").await;
        let scan = axon_scan(&plan);
        let batches = collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("query should execute");
        let trace = scan.pushdown_trace();

        assert_eq!(int64_column_values(&batches, 0), vec![10, 11, 12]);
        assert_eq!(trace.row_groups_skipped, 1);
        assert!(trace.bytes_fetched > 0);
        assert_eq!(trace.rows_emitted, 3);
        assert!(
            server
                .recorded_requests()
                .iter()
                .all(|request| request.headers.contains_key("range")),
            "Parquet scan should use browser range I/O"
        );
    });
}

async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .expect("SQL should plan")
        .create_physical_plan()
        .await
        .expect("physical plan should build")
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("test runtime should construct")
}

fn context_with_events() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    ctx.register_table(
        "events",
        Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
            delta_descriptor(Arc::clone(&schema)),
            controlled_partitions(schema),
        )),
    )
    .expect("table should register");
    ctx
}

fn context_with_single_partition_events() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    ctx.register_table(
        "events",
        Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
            DeltaTableDescriptor {
                table_name: "events".to_string(),
                table_version: 12,
                schema: Arc::clone(&schema),
                partition_columns: Vec::new(),
                partition_column_types: BTreeMap::new(),
                active_files: vec![active_file("part-000.parquet", "", 3)],
            },
            vec![vec![record_batch(
                schema,
                &[1, 2, 3],
                &[5, 25, 7],
                &["", "", ""],
            )]],
        )),
    )
    .expect("table should register");
    ctx
}

fn context_with_nullable_events() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table(
        "events",
        Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
            nullable_delta_descriptor(),
            nullable_partitions(nullable_descriptor_schema()),
        )),
    )
    .expect("table should register");
    ctx
}

fn context_with_value_stats_events() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = descriptor_schema();
    ctx.register_table(
        "events",
        Arc::new(AxonDeltaTableProvider::with_record_batch_partitions(
            value_stats_delta_descriptor(Arc::clone(&schema)),
            value_stats_partitions(schema),
        )),
    )
    .expect("table should register");
    ctx
}

fn axon_scan(plan: &Arc<dyn ExecutionPlan>) -> &AxonParquetScanExec {
    if let Some(scan) = plan.as_any().downcast_ref::<AxonParquetScanExec>() {
        return scan;
    }

    plan.children()
        .into_iter()
        .find_map(|child| axon_scan_optional(child.as_ref()))
        .expect("expected AxonParquetScanExec in physical plan")
}

fn axon_scan_optional(plan: &dyn ExecutionPlan) -> Option<&AxonParquetScanExec> {
    if let Some(scan) = plan.as_any().downcast_ref::<AxonParquetScanExec>() {
        return Some(scan);
    }

    plan.children()
        .into_iter()
        .find_map(|child| axon_scan_optional(child.as_ref()))
}

fn controlled_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![record_batch(
            Arc::clone(&schema),
            &[1, 2],
            &[5, 25],
            &["2026-01-01", "2026-01-01"],
        )],
        vec![record_batch(
            Arc::clone(&schema),
            &[3, 4],
            &[7, 15],
            &["2026-01-02", "2026-01-02"],
        )],
        vec![record_batch(schema, &[5], &[50], &["2026-01-03"])],
    ]
}

fn value_stats_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![record_batch(
            Arc::clone(&schema),
            &[1, 2],
            &[5, 7],
            &["", ""],
        )],
        vec![record_batch(
            Arc::clone(&schema),
            &[3, 4],
            &[10, 15],
            &["", ""],
        )],
        vec![record_batch(schema, &[5], &[50], &[""])],
    ]
}

fn record_batch(
    schema: SchemaRef,
    ids: &[i32],
    values: &[i32],
    event_dates: &[&str],
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(StringArray::from(event_dates.to_vec())),
        ],
    )
    .expect("record batch should construct")
}

fn delta_descriptor(schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 12,
        schema,
        partition_columns: vec!["event_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![
            active_file("event_date=2026-01-01/part-000.parquet", "2026-01-01", 2),
            active_file("event_date=2026-01-02/part-001.parquet", "2026-01-02", 2),
            active_file("event_date=2026-01-03/part-002.parquet", "2026-01-03", 1),
        ],
    }
}

fn nullable_delta_descriptor() -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 13,
        schema: nullable_descriptor_schema(),
        partition_columns: vec!["event_date".to_string()],
        partition_column_types: BTreeMap::from([(
            "event_date".to_string(),
            PartitionColumnType::String,
        )]),
        active_files: vec![
            DeltaActiveFile {
                path: "event_date=null/part-000.parquet".to_string(),
                url: "https://example.test/table/event_date=null/part-000.parquet".to_string(),
                size_bytes: 1024,
                partition_values: BTreeMap::from([("event_date".to_string(), None)]),
                object_etag: None,
                stats_json: Some(r#"{"numRecords":1}"#.to_string()),
                deletion_vector: None,
            },
            active_file("event_date=2026-01-02/part-001.parquet", "2026-01-02", 1),
        ],
    }
}

fn value_stats_delta_descriptor(schema: SchemaRef) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 15,
        schema,
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![
            stats_active_file("stats/part-000.parquet", 2, 1, 2, 5, 7),
            stats_active_file("stats/part-001.parquet", 2, 3, 4, 10, 15),
            stats_active_file("stats/part-002.parquet", 1, 5, 5, 50, 50),
        ],
    }
}

fn nullable_partitions(schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    vec![
        vec![nullable_record_batch(
            Arc::clone(&schema),
            &[1],
            &[5],
            &[None],
        )],
        vec![nullable_record_batch(
            schema,
            &[2],
            &[15],
            &[Some("2026-01-02")],
        )],
    ]
}

fn parquet_delta_descriptor(url: String, size_bytes: u64) -> DeltaTableDescriptor {
    DeltaTableDescriptor {
        table_name: "events".to_string(),
        table_version: 14,
        schema: parquet_descriptor_schema(),
        partition_columns: Vec::new(),
        partition_column_types: BTreeMap::new(),
        active_files: vec![DeltaActiveFile {
            path: "part-000.parquet".to_string(),
            url,
            size_bytes,
            partition_values: BTreeMap::new(),
            object_etag: None,
            stats_json: Some(r#"{"numRecords":6}"#.to_string()),
            deletion_vector: None,
        }],
    }
}

fn active_file(path: &str, event_date: &str, rows: u64) -> DeltaActiveFile {
    DeltaActiveFile {
        path: path.to_string(),
        url: format!("https://example.test/table/{path}"),
        size_bytes: 1024,
        partition_values: BTreeMap::from([(
            "event_date".to_string(),
            Some(event_date.to_string()),
        )]),
        object_etag: None,
        stats_json: Some(format!(r#"{{"numRecords":{rows}}}"#)),
        deletion_vector: None,
    }
}

fn stats_active_file(
    path: &str,
    rows: u64,
    min_id: i64,
    max_id: i64,
    min_value: i64,
    max_value: i64,
) -> DeltaActiveFile {
    DeltaActiveFile {
        path: path.to_string(),
        url: format!("https://example.test/table/{path}"),
        size_bytes: 1024,
        partition_values: BTreeMap::new(),
        object_etag: None,
        stats_json: Some(format!(
            r#"{{"numRecords":{rows},"minValues":{{"id":{min_id},"value":{min_value}}},"maxValues":{{"id":{max_id},"value":{max_value}}},"nullCount":{{"id":0,"value":0}}}}"#
        )),
        deletion_vector: None,
    }
}

fn nullable_record_batch(
    schema: SchemaRef,
    ids: &[i32],
    values: &[i32],
    event_dates: &[Option<&str>],
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(StringArray::from(event_dates.to_vec())),
        ],
    )
    .expect("record batch should construct")
}

fn descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("event_date", DataType::Utf8, true),
    ]))
}

fn nullable_descriptor_schema() -> SchemaRef {
    descriptor_schema()
}

fn parquet_descriptor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn int32_column_values(batches: &[RecordBatch], column_index: usize) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column_index)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
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

fn parquet_bytes_with_i64_row_groups(row_groups: &[&[i64]]) -> Vec<u8> {
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; }")
            .expect("parquet schema should parse"),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .expect("parquet writer should construct");

    for values in row_groups {
        let mut row_group = writer
            .next_row_group()
            .expect("row-group writer should construct");
        if let Some(mut column) = row_group
            .next_column()
            .expect("id column writer should be returned")
        {
            column
                .typed::<Int64Type>()
                .write_batch(values, None, None)
                .expect("id values should write");
            column.close().expect("id column writer should close");
        }
        row_group.close().expect("row-group writer should close");
    }

    writer.close().expect("file writer should close");
    bytes
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    headers: BTreeMap<String, String>,
}

struct TestResponse {
    status_line: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 512];
    loop {
        let read = stream
            .read(&mut chunk)
            .expect("test server should read request bytes");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buffer);
    let headers = request
        .lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();

    CapturedRequest { headers }
}

fn write_response(stream: &mut std::net::TcpStream, response: TestResponse) {
    write!(stream, "HTTP/1.1 {}\r\n", response.status_line).expect("status line should write");
    write!(stream, "Connection: close\r\n").expect("connection header should write");
    for (header, value) in response.headers {
        write!(stream, "{header}: {value}\r\n").expect("header should write");
    }
    write!(stream, "\r\n").expect("header terminator should write");
    stream
        .write_all(&response.body)
        .expect("response body should write");
    stream.flush().expect("response should flush");
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
        .expect("range should include '-' separator");
    let start = start.parse::<usize>().expect("range start should parse");
    if end.is_empty() {
        return (start, object_len.saturating_sub(1));
    }
    let end = end.parse::<usize>().expect("range end should parse");
    (start, end.min(object_len.saturating_sub(1)))
}

struct RequestCapturingServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    thread: Option<JoinHandle<()>>,
}

impl RequestCapturingServer {
    fn new(body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port should bind");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let address = listener.local_addr().expect("listener addr should resolve");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_thread = Arc::clone(&requests);
        let body = Arc::new(body);

        let thread = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if stop_for_thread.load(Ordering::SeqCst) {
                            break;
                        }
                        stream
                            .set_nonblocking(false)
                            .expect("accepted streams should allow blocking reads");
                        let body = Arc::clone(&body);
                        let requests = Arc::clone(&requests_for_thread);
                        thread::spawn(move || {
                            let request = read_request(&mut stream);
                            requests
                                .lock()
                                .expect("recorded requests should be writable")
                                .push(request.clone());
                            write_response(&mut stream, full_or_ranged_response(&request, &body));
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("test server accept should succeed: {error}"),
                }
            }
        });

        Self {
            address,
            stop,
            requests,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/object", self.address)
    }

    fn recorded_requests(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .expect("recorded requests should be readable")
            .clone()
    }
}

impl Drop for RequestCapturingServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("test server should shut down cleanly");
        }
    }
}

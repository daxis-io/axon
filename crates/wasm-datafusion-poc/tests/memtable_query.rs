use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::test]
async fn datafusion_queries_registered_record_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "B"])),
        ],
    )
    .expect("batch should construct");

    let result = wasm_datafusion_poc::query_record_batch(
        "SELECT id, category FROM axon_table WHERE id > 1 ORDER BY id",
        batch,
    )
    .await
    .expect("DataFusion query should run");

    assert_eq!(result.row_count, 2);
    assert_eq!(result.column_names, vec!["id", "category"]);
}

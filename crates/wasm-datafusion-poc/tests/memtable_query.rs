use arrow_array::{cast::AsArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::test]
async fn datafusion_queries_registered_record_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![5, 12, 25, 20])),
            Arc::new(StringArray::from(vec!["A", "B", "B", "C"])),
        ],
    )
    .expect("batch should construct");

    let batches = wasm_datafusion_poc::query_record_batches(wasm_datafusion_poc::SMOKE_SQL, batch)
        .await
        .expect("DataFusion query should run");
    let result = batches.first().expect("query should return one batch");

    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.schema().field(0).name(), "id");
    assert_eq!(result.schema().field(1).name(), "value");
    assert_eq!(int32_values(result, 0), vec![2, 3]);
    assert_eq!(int32_values(result, 1), vec![12, 25]);
}

fn int32_values(batch: &RecordBatch, column_index: usize) -> Vec<i32> {
    batch
        .column(column_index)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values()
        .to_vec()
}

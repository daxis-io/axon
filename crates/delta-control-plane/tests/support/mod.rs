#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deltalake::arrow::array::{Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::DeltaTable;
use tempfile::TempDir;

pub struct TestTableFixture {
    _tempdir: TempDir,
    pub table_uri: String,
}

impl TestTableFixture {
    pub fn create_multi_version() -> Self {
        let fixture = Self::create_with_partition_columns(vec![]);

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&fixture.table_uri)
                        .expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                let table = table
                    .write(vec![fixture_batch(
                        &[1, 2, 3],
                        &["A", "B", "A"],
                        &[10, 20, 30],
                    )])
                    .await
                    .expect("first batch should be written");

                table
                    .write(vec![fixture_batch(
                        &[4, 5, 6],
                        &["B", "B", "C"],
                        &[40, 50, 60],
                    )])
                    .await
                    .expect("second batch should be written");
            });

        fixture
    }

    pub fn create_partitioned() -> Self {
        let fixture = Self::create_with_partition_columns(vec!["category"]);

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&fixture.table_uri)
                        .expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                table
                    .write(vec![fixture_batch(
                        &[1, 2, 3, 4, 5, 6],
                        &["A", "B", "A", "B", "B", "C"],
                        &[10, 20, 30, 40, 50, 60],
                    )])
                    .await
                    .expect("partitioned batch should be written");
            });

        fixture
    }

    fn create_with_partition_columns(partition_columns: Vec<&str>) -> Self {
        let tempdir = TempDir::new().expect("tempdir should be created");
        let table_uri = deltalake::ensure_table_uri(tempdir.path().to_string_lossy())
            .expect("table uri should be normalized")
            .to_string();

        tokio::runtime::Runtime::new()
            .expect("runtime should be created")
            .block_on(async {
                let table = DeltaTable::try_from_url(
                    deltalake::ensure_table_uri(&table_uri).expect("table uri should parse"),
                )
                .await
                .expect("table handle should be created");

                table
                    .create()
                    .with_columns(default_table_columns())
                    .with_partition_columns(partition_columns)
                    .with_table_name("axon_fixture")
                    .await
                    .expect("table should be created");
            });

        Self {
            _tempdir: tempdir,
            table_uri,
        }
    }

    pub fn data_file_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        collect_matching_paths(self._tempdir.path(), &mut paths, |path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        });
        paths.sort();
        paths
    }
}

fn default_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "category".to_string(),
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "value".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
    ]
}

fn fixture_batch(ids: &[i32], categories: &[&str], values: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("category", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Int32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(categories.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )
    .expect("fixture batch should be created")
}

fn collect_matching_paths<F>(root: &Path, matches: &mut Vec<PathBuf>, predicate: F)
where
    F: Fn(&Path) -> bool + Copy,
{
    for entry in fs::read_dir(root).expect("directory should be readable") {
        let entry = entry.expect("directory entry should load");
        let path = entry.path();
        if path.is_dir() {
            collect_matching_paths(&path, matches, predicate);
        } else if predicate(&path) {
            matches.push(path);
        }
    }
}

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
use deltalake::parquet::file::serialized_reader::ReadOptionsBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{checkpoints, DeltaTable};
use serde::Serialize;
use sha2::{Digest, Sha256};

const BROWSER_TABLE_URI: &str = "gs://axon-sandbox/prod-like-events";
const PAGE_INDEX_AB_SEED: u64 = 0xA80D_1D3E_58C0_2026;
const PAGE_INDEX_AB_ROWS: usize = 65_536;
const PAGE_INDEX_AB_PAGE_ROWS: usize = 1_024;
const PAGE_INDEX_AB_MATCH_START: i64 = 63_488;

#[derive(Debug, Serialize)]
struct FixtureManifest {
    name: &'static str,
    table_uri: &'static str,
    expected_latest_version: i64,
    checkpoint_version: i64,
    generated_steps: Vec<GeneratedStep>,
    objects: Vec<ManifestObject>,
    data_files: Vec<DataFileInventory>,
}

#[derive(Debug, Serialize)]
struct GeneratedStep {
    version: i64,
    label: &'static str,
    detail: &'static str,
}

#[derive(Debug, Serialize)]
struct ManifestObject {
    relative_path: String,
    url_path: String,
    kind: ObjectKind,
    size_bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObjectKind {
    CommitJson,
    CheckpointParquet,
    LastCheckpoint,
}

#[derive(Debug, Serialize)]
struct DataFileInventory {
    relative_path: String,
    url_path: String,
    size_bytes: u64,
    partition_values: BTreeMap<String, String>,
}

#[derive(Debug)]
struct PageIndexAbFixture {
    bytes: Vec<u8>,
    manifest: PageIndexAbManifest,
}

#[derive(Debug, Serialize)]
struct PageIndexAbManifest {
    schema_version: u32,
    fixture_revision: &'static str,
    url_path: &'static str,
    size_bytes: u64,
    seed: String,
    row_count: u64,
    row_group_count: u64,
    page_row_count_limit: u64,
    predicate: &'static str,
    expected_row_count: u64,
    expected_event_id_sum: i64,
    expected_payload_length_sum: u64,
    expected_pages_selected: u64,
    expected_pages_skipped: u64,
    footer_extent: ByteExtentManifest,
    column_index_extents: Vec<ColumnExtentManifest>,
    offset_index_extents: Vec<ColumnExtentManifest>,
    data_page_extents: Vec<DataPageExtentManifest>,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct ByteExtentManifest {
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ColumnExtentManifest {
    column: String,
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Debug, Serialize)]
struct DataPageExtentManifest {
    column: String,
    page_index: u64,
    first_row_index: u64,
    row_count: u64,
    offset_bytes: u64,
    length_bytes: u64,
    predicate_match: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let output_root = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("public/fixtures/prod-like"));
    generate_fixture(&output_root).await
}

async fn generate_fixture(output_root: &Path) -> Result<(), Box<dyn Error>> {
    if output_root.exists() {
        fs::remove_dir_all(output_root)?;
    }
    fs::create_dir_all(output_root)?;
    let table_root = output_root.join("table");

    let table_url = deltalake::ensure_table_uri(table_root.to_string_lossy())?;
    let table = DeltaTable::try_from_url(table_url.clone()).await?;
    let table = table
        .create()
        .with_columns(table_columns())
        .with_partition_columns(vec!["category"])
        .with_table_name("axon_prod_like_fixture")
        .with_configuration(vec![
            (
                "delta.checkpoint.writeStatsAsJson".to_string(),
                Some("true".to_string()),
            ),
            (
                "delta.checkpoint.writeStatsAsStruct".to_string(),
                Some("false".to_string()),
            ),
        ])
        .await?;

    let table = table
        .write(vec![fixture_batch(
            &[1, 2, 3, 4],
            &["A", "A", "B", "B"],
            &[10, 20, 30, 40],
        )])
        .await?;
    let table = table
        .write(vec![fixture_batch(&[5, 6], &["C", "C"], &[50, 60])])
        .with_save_mode(SaveMode::Append)
        .await?;

    checkpoints::create_checkpoint(&table, None).await?;

    table
        .write(vec![fixture_batch(
            &[7, 8, 9, 10],
            &["B", "B", "D", "D"],
            &[70, 80, 90, 100],
        )])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    let manifest = FixtureManifest {
        name: "Prod-like generated Delta table",
        table_uri: BROWSER_TABLE_URI,
        expected_latest_version: 3,
        checkpoint_version: 2,
        generated_steps: vec![
            GeneratedStep {
                version: 0,
                label: "create table",
                detail: "Create schema and partition metadata for category-partitioned events.",
            },
            GeneratedStep {
                version: 1,
                label: "append A/B",
                detail: "Write initial partitioned Parquet data files.",
            },
            GeneratedStep {
                version: 2,
                label: "append C + checkpoint",
                detail: "Append another partition and write a checkpoint parquet file.",
            },
            GeneratedStep {
                version: 3,
                label: "overwrite B/D",
                detail: "Replay removes old active files and adds the latest B/D data files.",
            },
        ],
        objects: collect_manifest_objects(output_root, &table_root)?,
        data_files: collect_data_files(output_root, &table_root)?,
    };
    let manifest_path = output_root.join("delta-log-manifest.json");
    fs::write(manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    let page_index_fixture = build_page_index_ab_fixture()?;
    let page_index_root = output_root.join("page-index-ab");
    fs::create_dir_all(&page_index_root)?;
    fs::write(
        page_index_root.join("event-id.parquet"),
        &page_index_fixture.bytes,
    )?;
    fs::write(
        page_index_root.join("manifest.json"),
        serde_json::to_vec_pretty(&page_index_fixture.manifest)?,
    )?;
    Ok(())
}

fn table_columns() -> Vec<StructField> {
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
    .expect("fixture batch should be valid")
}

fn build_page_index_ab_fixture() -> Result<PageIndexAbFixture, Box<dyn Error>> {
    let event_ids = (0..PAGE_INDEX_AB_ROWS)
        .map(|value| i64::try_from(value).expect("fixture event id should fit i64"))
        .collect::<Vec<_>>();
    let payloads = event_ids
        .iter()
        .map(|event_id| {
            let fragment = format!("{:016x}{:016x}", PAGE_INDEX_AB_SEED, event_id);
            fragment.repeat(4)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("event_id", ArrowDataType::Int64, false),
        Field::new("payload", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(event_ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_created_by("axon page-index byte-savings A/B fixture".to_string())
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(PAGE_INDEX_AB_ROWS))
        .set_data_page_row_count_limit(PAGE_INDEX_AB_PAGE_ROWS)
        .set_write_batch_size(PAGE_INDEX_AB_PAGE_ROWS)
        .set_dictionary_enabled(false)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    let expected_row_count = u64::try_from(PAGE_INDEX_AB_ROWS)
        .expect("fixture rows should fit u64")
        - u64::try_from(PAGE_INDEX_AB_MATCH_START).expect("match start should fit u64");
    let last_event_id =
        i64::try_from(PAGE_INDEX_AB_ROWS - 1).expect("last fixture event id should fit i64");
    let expected_event_id_sum =
        (PAGE_INDEX_AB_MATCH_START + last_event_id) * i64::try_from(expected_row_count)? / 2;
    let expected_payload_length_sum = expected_row_count * 128;
    let expected_pages_selected = expected_row_count
        / u64::try_from(PAGE_INDEX_AB_PAGE_ROWS).expect("page rows should fit u64");
    let total_pages = u64::try_from(PAGE_INDEX_AB_ROWS / PAGE_INDEX_AB_PAGE_ROWS)
        .expect("page count should fit u64");
    let size_bytes = u64::try_from(bytes.len())?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader =
        SerializedFileReader::new_with_options(bytes::Bytes::copy_from_slice(&bytes), options)?;
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);
    let column_index_extents = row_group
        .columns()
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            Ok(ColumnExtentManifest {
                column: metadata
                    .file_metadata()
                    .schema_descr()
                    .column(column_index)
                    .path()
                    .string(),
                offset_bytes: u64::try_from(
                    column
                        .column_index_offset()
                        .ok_or("page-index fixture column index offset was absent")?,
                )?,
                length_bytes: u64::try_from(
                    column
                        .column_index_length()
                        .ok_or("page-index fixture column index length was absent")?,
                )?,
            })
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let offset_index_extents = row_group
        .columns()
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            Ok(ColumnExtentManifest {
                column: metadata
                    .file_metadata()
                    .schema_descr()
                    .column(column_index)
                    .path()
                    .string(),
                offset_bytes: u64::try_from(
                    column
                        .offset_index_offset()
                        .ok_or("page-index fixture offset index offset was absent")?,
                )?,
                length_bytes: u64::try_from(
                    column
                        .offset_index_length()
                        .ok_or("page-index fixture offset index length was absent")?,
                )?,
            })
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let offset_indexes = metadata
        .offset_index()
        .ok_or("page-index fixture decoded offset indexes were absent")?;
    let mut data_page_extents = Vec::new();
    for (column_position, offset_index) in offset_indexes[0].iter().enumerate() {
        let column = metadata
            .file_metadata()
            .schema_descr()
            .column(column_position)
            .path()
            .string();
        let locations = offset_index.page_locations();
        for (page_index, location) in locations.iter().enumerate() {
            let first_row_index = u64::try_from(location.first_row_index)?;
            let next_row_index = locations
                .get(page_index + 1)
                .map(|next| u64::try_from(next.first_row_index))
                .transpose()?
                .unwrap_or(u64::try_from(PAGE_INDEX_AB_ROWS)?);
            let row_count = next_row_index
                .checked_sub(first_row_index)
                .ok_or("page-index fixture page rows were not monotonic")?;
            data_page_extents.push(DataPageExtentManifest {
                column: column.clone(),
                page_index: u64::try_from(page_index)?,
                first_row_index,
                row_count,
                offset_bytes: u64::try_from(location.offset)?,
                length_bytes: u64::try_from(location.compressed_page_size)?,
                predicate_match: next_row_index > u64::try_from(PAGE_INDEX_AB_MATCH_START)?,
            });
        }
    }
    let footer_length_offset = bytes
        .len()
        .checked_sub(8)
        .ok_or("page-index fixture was shorter than a parquet trailer")?;
    let footer_metadata_length = u32::from_le_bytes(
        bytes[footer_length_offset..footer_length_offset + 4]
            .try_into()
            .expect("parquet footer length should occupy four bytes"),
    );
    let footer_length_bytes = u64::from(footer_metadata_length) + 8;
    let footer_extent = ByteExtentManifest {
        offset_bytes: size_bytes
            .checked_sub(footer_length_bytes)
            .ok_or("page-index fixture footer exceeded object size")?,
        length_bytes: footer_length_bytes,
    };

    Ok(PageIndexAbFixture {
        bytes,
        manifest: PageIndexAbManifest {
            schema_version: 1,
            fixture_revision: "local-page-index-ab-v1",
            url_path: "/fixtures/prod-like/page-index-ab/event-id.parquet",
            size_bytes,
            seed: PAGE_INDEX_AB_SEED.to_string(),
            row_count: u64::try_from(PAGE_INDEX_AB_ROWS)?,
            row_group_count: 1,
            page_row_count_limit: u64::try_from(PAGE_INDEX_AB_PAGE_ROWS)?,
            predicate: "event_id >= 63488",
            expected_row_count,
            expected_event_id_sum,
            expected_payload_length_sum,
            expected_pages_selected,
            expected_pages_skipped: total_pages - expected_pages_selected,
            footer_extent,
            column_index_extents,
            offset_index_extents,
            data_page_extents,
            sha256,
        },
    })
}

fn collect_manifest_objects(
    output_root: &Path,
    table_root: &Path,
) -> Result<Vec<ManifestObject>, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(&table_root.join("_delta_log"), &mut files)?;
    files.sort();
    Ok(files
        .into_iter()
        .filter_map(|path| {
            let relative_path = relative_to(table_root, &path).ok()?;
            let kind = classify_log_object(&relative_path)?;
            let size_bytes = fs::metadata(&path).ok()?.len();
            Some(ManifestObject {
                url_path: public_url_path(output_root, &path).ok()?,
                relative_path,
                kind,
                size_bytes,
            })
        })
        .collect())
}

fn collect_data_files(
    output_root: &Path,
    table_root: &Path,
) -> Result<Vec<DataFileInventory>, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(table_root, &mut files)?;
    files.sort();
    files
        .into_iter()
        .filter(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension == "parquet")
                && !path.components().any(|component| {
                    matches!(component, Component::Normal(value) if value == "_delta_log")
                })
        })
        .map(|path| {
            let relative_path = relative_to(table_root, &path)?;
            Ok(DataFileInventory {
                url_path: public_url_path(output_root, &path)?,
                size_bytes: fs::metadata(&path)?.len(),
                partition_values: partition_values_from_path(&relative_path),
                relative_path,
            })
        })
        .collect()
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

fn classify_log_object(relative_path: &str) -> Option<ObjectKind> {
    if relative_path == "_delta_log/_last_checkpoint" {
        Some(ObjectKind::LastCheckpoint)
    } else if relative_path.ends_with(".checkpoint.parquet") {
        Some(ObjectKind::CheckpointParquet)
    } else if relative_path.ends_with(".json") {
        Some(ObjectKind::CommitJson)
    } else {
        None
    }
}

fn relative_to(root: &Path, path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(path
        .strip_prefix(root)?
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn public_url_path(output_root: &Path, path: &Path) -> Result<String, Box<dyn Error>> {
    let relative_path = path.strip_prefix(output_root)?;
    Ok(format!(
        "/fixtures/prod-like/{}",
        relative_path
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    ))
}

fn partition_values_from_path(relative_path: &str) -> BTreeMap<String, String> {
    relative_path
        .split('/')
        .filter_map(|component| component.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
    use deltalake::parquet::file::serialized_reader::ReadOptionsBuilder;

    #[test]
    fn page_index_ab_fixture_has_independent_indexes_and_selective_pages() {
        let fixture =
            build_page_index_ab_fixture().expect("page-index A/B fixture should generate");
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let path = env::temp_dir().join(format!(
            "axon-page-index-ab-fixture-test-{}.parquet",
            std::process::id()
        ));
        fs::write(&path, &fixture.bytes).expect("fixture bytes should write");
        let file = fs::File::open(&path).expect("fixture file should reopen");
        let reader = SerializedFileReader::new_with_options(file, options)
            .expect("page-index A/B fixture should decode");
        fs::remove_file(path).expect("temporary fixture should be removed");
        let metadata = reader.metadata();

        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 65_536);
        let column_index = metadata
            .column_index()
            .expect("fixture should contain column indexes");
        let offset_index = metadata
            .offset_index()
            .expect("fixture should contain offset indexes");
        assert_eq!(column_index.len(), 1);
        assert_eq!(offset_index.len(), 1);
        assert!(
            column_index[0][0].num_pages() >= 64,
            "event_id should span at least 64 independently prunable pages"
        );
        assert_eq!(
            column_index[0][0].num_pages() as usize,
            offset_index[0][0].page_locations().len()
        );

        assert_eq!(fixture.manifest.seed, PAGE_INDEX_AB_SEED.to_string());
        assert_eq!(fixture.manifest.row_count, 65_536);
        assert_eq!(fixture.manifest.row_group_count, 1);
        assert_eq!(fixture.manifest.predicate, "event_id >= 63488");
        assert_eq!(fixture.manifest.expected_row_count, 2_048);
        assert!(fixture.manifest.expected_pages_selected > 0);
        assert!(fixture.manifest.expected_pages_skipped > 0);
        assert_eq!(
            fixture.manifest.expected_pages_selected + fixture.manifest.expected_pages_skipped,
            column_index[0][0].num_pages()
        );
        assert!(fixture.manifest.footer_extent.length_bytes > 8);
        assert_eq!(fixture.manifest.column_index_extents.len(), 2);
        assert_eq!(fixture.manifest.offset_index_extents.len(), 2);
        assert_eq!(
            fixture
                .manifest
                .data_page_extents
                .iter()
                .filter(|page| page.column == "event_id")
                .count(),
            column_index[0][0].num_pages() as usize
        );
        assert_eq!(
            fixture
                .manifest
                .data_page_extents
                .iter()
                .filter(|page| page.column == "event_id" && page.predicate_match)
                .count(),
            fixture.manifest.expected_pages_selected as usize
        );
        assert_eq!(fixture.manifest.sha256.len(), 64);
    }

    #[tokio::test]
    async fn generated_fixture_writes_page_index_ab_bytes_and_manifest() {
        let output_root = env::temp_dir().join(format!(
            "axon-page-index-ab-output-test-{}",
            std::process::id()
        ));
        generate_fixture(&output_root)
            .await
            .expect("production-like fixtures should generate");

        let fixture_path = output_root.join("page-index-ab/event-id.parquet");
        let manifest_path = output_root.join("page-index-ab/manifest.json");
        assert!(fixture_path.is_file());
        assert!(manifest_path.is_file());
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(&manifest_path).expect("page-index fixture manifest should read"),
        )
        .expect("page-index fixture manifest should parse");
        assert_eq!(manifest["fixture_revision"], "local-page-index-ab-v1");
        assert_eq!(
            manifest["url_path"],
            "/fixtures/prod-like/page-index-ab/event-id.parquet"
        );
        assert_eq!(
            manifest["size_bytes"],
            fs::metadata(fixture_path)
                .expect("page-index fixture metadata should read")
                .len()
        );

        fs::remove_dir_all(output_root).expect("temporary fixture output should be removed");
    }
}

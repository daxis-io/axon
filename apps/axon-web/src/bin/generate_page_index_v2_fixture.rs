use std::env;
use std::error::Error;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::page_index::column_index::ColumnIndexMetaData;
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
use deltalake::parquet::file::serialized_reader::ReadOptionsBuilder;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const SCHEMA_VERSION: u32 = 2;
const FIXTURE_REVISION: &str = "s3-browser-perf-page-index-v2";
const IMMUTABLE_PREFIX: &str = "fixtures/s3-browser-perf-page-index-v2";
const DEFAULT_TARGET_ACTIVE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_DATA_PAGE_SIZE_BYTES: usize = 64 * 1024;
const DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT: usize = 1_024;
const DEFAULT_ESTIMATED_COMPRESSED_BYTES_PER_ROW: usize = 128;
const DEFAULT_SEED: u64 = 0xA501_1D3E_2026_0002;
const PAYLOAD_BYTES: usize = 128;
const PREDICATE_COLUMN: &str = "predicate_value";
const MISSING_INDEX_COLUMN: &str = "missing_index_value";
const SELECTIVITY_BASIS_POINTS: [u32; 7] = [0, 10, 100, 500, 2_000, 5_000, 10_000];

#[derive(Clone, Debug)]
struct FixtureConfig {
    target_active_bytes: usize,
    data_page_size_bytes: usize,
    data_page_row_count_limit: usize,
    estimated_compressed_bytes_per_row: usize,
    seed: u64,
}

impl FixtureConfig {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let config = Self {
            target_active_bytes: env_usize(
                "AXON_PAGE_INDEX_V2_TARGET_ACTIVE_BYTES",
                DEFAULT_TARGET_ACTIVE_BYTES,
            )?,
            data_page_size_bytes: env_usize(
                "AXON_PAGE_INDEX_V2_DATA_PAGE_SIZE_BYTES",
                DEFAULT_DATA_PAGE_SIZE_BYTES,
            )?,
            data_page_row_count_limit: env_usize(
                "AXON_PAGE_INDEX_V2_DATA_PAGE_ROW_COUNT_LIMIT",
                DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT,
            )?,
            estimated_compressed_bytes_per_row: env_usize(
                "AXON_PAGE_INDEX_V2_ESTIMATED_COMPRESSED_BYTES_PER_ROW",
                DEFAULT_ESTIMATED_COMPRESSED_BYTES_PER_ROW,
            )?,
            seed: env_u64("AXON_PAGE_INDEX_V2_SEED", DEFAULT_SEED)?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.target_active_bytes < 1024 * 1024 {
            return Err("AXON_PAGE_INDEX_V2_TARGET_ACTIVE_BYTES must be at least 1 MiB".into());
        }
        if self.data_page_size_bytes < 8 * 1024 {
            return Err("AXON_PAGE_INDEX_V2_DATA_PAGE_SIZE_BYTES must be at least 8 KiB".into());
        }
        if self.data_page_row_count_limit < 128 {
            return Err("AXON_PAGE_INDEX_V2_DATA_PAGE_ROW_COUNT_LIMIT must be at least 128".into());
        }
        if self.estimated_compressed_bytes_per_row == 0 {
            return Err(
                "AXON_PAGE_INDEX_V2_ESTIMATED_COMPRESSED_BYTES_PER_ROW must be positive".into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Layout {
    Ordered,
    EightPageClusters,
    FullyShuffled,
}

impl Layout {
    const ALL: [Self; 3] = [Self::Ordered, Self::EightPageClusters, Self::FullyShuffled];

    fn name(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::EightPageClusters => "eight-page-clusters",
            Self::FullyShuffled => "fully-shuffled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Geometry {
    FewLarge,
    ManySmall,
}

impl Geometry {
    const ALL: [Self; 2] = [Self::FewLarge, Self::ManySmall];

    fn name(self) -> &'static str {
        match self {
            Self::FewLarge => "few-large",
            Self::ManySmall => "many-small",
        }
    }

    fn active_file_count(self) -> usize {
        match self {
            Self::FewLarge => 4,
            Self::ManySmall => 32,
        }
    }

    fn row_groups_per_file(self) -> usize {
        match self {
            Self::FewLarge => 4,
            Self::ManySmall => 1,
        }
    }

    fn target_file_bytes(self) -> usize {
        match self {
            Self::FewLarge => 16 * 1024 * 1024,
            Self::ManySmall => 2 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Serialize)]
struct FixtureManifest {
    schema_version: u32,
    fixture_revision: &'static str,
    immutable_prefix: &'static str,
    generated_table_uri_base: String,
    generator: GeneratorManifest,
    writer: WriterManifest,
    predicate_envelope: PredicateEnvelopeManifest,
    tables: Vec<TableManifest>,
    negative_cases: Vec<NegativeCaseManifest>,
}

#[derive(Debug, Serialize)]
struct GeneratorManifest {
    package_version: &'static str,
    git_commit: String,
    git_worktree_dirty: bool,
    source_sha256: String,
    cargo_lock_sha256: String,
    seed: String,
    target_active_bytes_per_table: usize,
    estimated_compressed_bytes_per_row: usize,
}

#[derive(Debug, Serialize)]
struct WriterManifest {
    parquet_version: &'static str,
    compression: &'static str,
    statistics: &'static str,
    data_page_size_target_bytes: usize,
    data_page_row_count_limit: usize,
    dictionary_enabled: bool,
    missing_index_column_statistics: &'static str,
}

#[derive(Debug, Serialize)]
struct PredicateEnvelopeManifest {
    predicate_column: &'static str,
    supported_form: &'static str,
    narrow_projection_sql: &'static str,
    wide_projection_sql: &'static str,
    selectivity_basis_points: [u32; 7],
}

#[derive(Debug, Serialize)]
struct NegativeCaseManifest {
    name: &'static str,
    sql_predicate: &'static str,
    expected_plan: &'static str,
    expected_reason: &'static str,
}

#[derive(Debug, Serialize)]
struct TableManifest {
    name: String,
    relative_path: String,
    table_uri: String,
    layout: Layout,
    geometry: Geometry,
    target_active_bytes: usize,
    actual_active_bytes: u64,
    target_file_bytes: usize,
    active_file_count: usize,
    row_groups_per_file: usize,
    rows_per_row_group: usize,
    total_rows: u64,
    table_object_sha256: String,
    active_files: Vec<FileManifest>,
    selectivity_cases: Vec<SelectivityManifest>,
}

#[derive(Debug, Serialize)]
struct FileManifest {
    relative_path: String,
    size_bytes: u64,
    sha256: String,
    row_count: u64,
    row_group_count: usize,
    compression: &'static str,
    predicate_column_indexes_usable: bool,
    missing_index_column_has_column_index: bool,
    row_groups: Vec<RowGroupManifest>,
}

#[derive(Debug, Serialize)]
struct RowGroupManifest {
    row_group_index: usize,
    row_count: u64,
    predicate_min: i64,
    predicate_max: i64,
    column_index_extent: ExtentManifest,
    offset_index_extent: ExtentManifest,
    pages: Vec<PageManifest>,
}

#[derive(Clone, Debug, Serialize)]
struct ExtentManifest {
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Debug, Serialize)]
struct PageManifest {
    page_index: usize,
    first_row_index: u64,
    row_count: u64,
    offset_bytes: u64,
    length_bytes: u64,
    min_value: i64,
    max_value: i64,
}

#[derive(Debug, Serialize)]
struct SelectivityManifest {
    requested_basis_points: u32,
    cutoff: i64,
    expected_rows: u64,
    actual_basis_points: u32,
    narrow_result_checksum: String,
    wide_result_checksum: String,
}

#[derive(Debug)]
struct FileWriteResult {
    manifest: FileManifest,
    min_value: i64,
    max_value: i64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let output_root = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fixtures/s3-browser-perf-page-index-v2"));
    let output_root = if output_root.is_absolute() {
        output_root
    } else {
        env::current_dir()?.join(output_root)
    };
    let config = FixtureConfig::from_env()?;
    let manifest = generate_fixture(&output_root, &config)?;
    println!(
        "Generated {} immutable local fixture tables under {}",
        manifest.tables.len(),
        output_root.display()
    );
    println!(
        "Cloud upload was not attempted. The reserved immutable prefix is {IMMUTABLE_PREFIX}."
    );
    Ok(())
}

fn generate_fixture(
    output_root: &Path,
    config: &FixtureConfig,
) -> Result<FixtureManifest, Box<dyn Error>> {
    if output_root.exists() {
        fs::remove_dir_all(output_root)?;
    }
    fs::create_dir_all(output_root)?;
    let table_uri_base = env::var("AXON_PAGE_INDEX_V2_TABLE_URI_BASE")
        .unwrap_or_else(|_| format!("s3://<bucket>/{IMMUTABLE_PREFIX}"));
    let (git_commit, git_worktree_dirty) = git_provenance();

    let mut tables = Vec::with_capacity(Layout::ALL.len() * Geometry::ALL.len());
    for layout in Layout::ALL {
        for geometry in Geometry::ALL {
            tables.push(generate_table(
                output_root,
                config,
                layout,
                geometry,
                &table_uri_base,
            )?);
        }
    }

    let manifest = FixtureManifest {
        schema_version: SCHEMA_VERSION,
        fixture_revision: FIXTURE_REVISION,
        immutable_prefix: IMMUTABLE_PREFIX,
        generated_table_uri_base: table_uri_base,
        generator: GeneratorManifest {
            package_version: env!("CARGO_PKG_VERSION"),
            git_commit,
            git_worktree_dirty,
            source_sha256: sha256_bytes(include_bytes!("generate_page_index_v2_fixture.rs")),
            cargo_lock_sha256: sha256_bytes(include_bytes!("../../../../Cargo.lock")),
            seed: config.seed.to_string(),
            target_active_bytes_per_table: config.target_active_bytes,
            estimated_compressed_bytes_per_row: config.estimated_compressed_bytes_per_row,
        },
        writer: WriterManifest {
            parquet_version: "2.0",
            compression: "snappy",
            statistics: "page",
            data_page_size_target_bytes: config.data_page_size_bytes,
            data_page_row_count_limit: config.data_page_row_count_limit,
            dictionary_enabled: false,
            missing_index_column_statistics: "none",
        },
        predicate_envelope: PredicateEnvelopeManifest {
            predicate_column: PREDICATE_COLUMN,
            supported_form: "predicate_value < signed_integer_literal",
            narrow_projection_sql:
                "SELECT COUNT(*), SUM(predicate_value) FROM <table> WHERE predicate_value < <cutoff>",
            wide_projection_sql:
                "SELECT COUNT(*), SUM(predicate_value), SUM(event_id), SUM(LENGTH(payload)) FROM <table> WHERE predicate_value < <cutoff>",
            selectivity_basis_points: SELECTIVITY_BASIS_POINTS,
        },
        tables,
        negative_cases: vec![
            NegativeCaseManifest {
                name: "unsupported-compound",
                sql_predicate: "predicate_value < <cutoff> AND narrow_value >= 0",
                expected_plan: "skip",
                expected_reason: "unsupported_predicate",
            },
            NegativeCaseManifest {
                name: "unsupported-expression",
                sql_predicate: "predicate_value + 1 < <cutoff>",
                expected_plan: "skip",
                expected_reason: "unsupported_predicate",
            },
            NegativeCaseManifest {
                name: "type-incompatible",
                sql_predicate: "predicate_value < 'not-an-integer'",
                expected_plan: "skip",
                expected_reason: "unsupported_predicate",
            },
            NegativeCaseManifest {
                name: "missing-column-index",
                sql_predicate: "missing_index_value < <cutoff>",
                expected_plan: "skip",
                expected_reason: "missing_or_invalid_indexes",
            },
        ],
    };
    let manifest_path = output_root.join("fixture-manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    write_object_checksums(output_root)?;
    let provenance = json!({
        "schema_version": SCHEMA_VERSION,
        "fixture_revision": FIXTURE_REVISION,
        "immutable_prefix": IMMUTABLE_PREFIX,
        "manifest_sha256": sha256_file(&manifest_path)?,
        "object_checksums_sha256": sha256_file(&output_root.join("object-sha256.txt"))?,
        "cloud_upload_attempted": false,
    });
    fs::write(
        output_root.join("provenance.json"),
        serde_json::to_vec_pretty(&provenance)?,
    )?;
    Ok(manifest)
}

fn generate_table(
    output_root: &Path,
    config: &FixtureConfig,
    layout: Layout,
    geometry: Geometry,
    table_uri_base: &str,
) -> Result<TableManifest, Box<dyn Error>> {
    let name = format!("{}-{}", layout.name(), geometry.name());
    let relative_path = format!("tables/{name}");
    let table_root = output_root.join(&relative_path);
    fs::create_dir_all(table_root.join("_delta_log"))?;
    let total_row_groups = geometry.active_file_count() * geometry.row_groups_per_file();
    let rows_per_row_group = round_up(
        config.target_active_bytes / config.estimated_compressed_bytes_per_row / total_row_groups,
        config.data_page_row_count_limit,
    )
    .max(config.data_page_row_count_limit * 8);
    let rows_per_file = rows_per_row_group * geometry.row_groups_per_file();
    let total_rows_usize = rows_per_file * geometry.active_file_count();
    let total_rows = u64::try_from(total_rows_usize)?;
    let schema = fixture_schema();
    let writer_properties = writer_properties(config, rows_per_row_group);

    let mut active_files = Vec::with_capacity(geometry.active_file_count());
    for file_index in 0..geometry.active_file_count() {
        let file_name = format!("part-{file_index:05}.parquet");
        let file_path = table_root.join(&file_name);
        let result = write_parquet_file(
            &file_path,
            Arc::clone(&schema),
            writer_properties.clone(),
            config,
            layout,
            file_index,
            geometry.row_groups_per_file(),
            rows_per_row_group,
            total_rows_usize,
        )?;
        if result.min_value < 0 || result.max_value >= i64::try_from(total_rows_usize)? {
            return Err(
                format!("generated predicate values escaped table bounds for {name}").into(),
            );
        }
        active_files.push(result.manifest);
    }

    write_delta_log(&table_root, &name, &active_files)?;
    let actual_active_bytes = active_files.iter().map(|file| file.size_bytes).sum();
    let selectivity_cases = selectivity_cases(
        total_rows,
        layout,
        total_row_groups,
        rows_per_row_group,
        config.data_page_row_count_limit,
        config.seed,
    )?;
    let table_object_sha256 = table_object_hash(&table_root)?;
    Ok(TableManifest {
        name: name.clone(),
        relative_path,
        table_uri: format!("{}/{name}", table_uri_base.trim_end_matches('/')),
        layout,
        geometry,
        target_active_bytes: config.target_active_bytes,
        actual_active_bytes,
        target_file_bytes: geometry.target_file_bytes(),
        active_file_count: active_files.len(),
        row_groups_per_file: geometry.row_groups_per_file(),
        rows_per_row_group,
        total_rows,
        table_object_sha256,
        active_files,
        selectivity_cases,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_parquet_file(
    path: &Path,
    schema: Arc<Schema>,
    writer_properties: WriterProperties,
    config: &FixtureConfig,
    layout: Layout,
    file_index: usize,
    row_groups_per_file: usize,
    rows_per_row_group: usize,
    total_rows: usize,
) -> Result<FileWriteResult, Box<dyn Error>> {
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(writer_properties))?;
    let mut file_min = i64::MAX;
    let mut file_max = i64::MIN;
    for row_group_index in 0..row_groups_per_file {
        let global_row_group = file_index * row_groups_per_file + row_group_index;
        let base = global_row_group * rows_per_row_group;
        let values = layout_values(
            layout,
            base,
            rows_per_row_group,
            config.data_page_row_count_limit,
            config.seed ^ u64::try_from(global_row_group)?,
        );
        let min = values.iter().copied().min().ok_or("row group was empty")?;
        let max = values.iter().copied().max().ok_or("row group was empty")?;
        file_min = file_min.min(min);
        file_max = file_max.max(max);
        writer.write(&fixture_batch(base, &values, config.seed, total_rows)?)?;
        writer.flush()?;
    }
    writer.close()?;

    let size_bytes = fs::metadata(path)?.len();
    let sha256 = sha256_file(path)?;
    let read_options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(fs::File::open(path)?, read_options)?;
    let metadata = reader.metadata();
    let predicate_position = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.path().string() == PREDICATE_COLUMN)
        .ok_or("predicate column was absent from parquet schema")?;
    let missing_position = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.path().string() == MISSING_INDEX_COLUMN)
        .ok_or("missing-index column was absent from parquet schema")?;
    let column_indexes = metadata
        .column_index()
        .ok_or("decoded parquet column indexes were absent")?;
    let offset_indexes = metadata
        .offset_index()
        .ok_or("decoded parquet offset indexes were absent")?;
    let missing_index_column_has_column_index = column_indexes.iter().any(|row_group| {
        row_group
            .get(missing_position)
            .is_some_and(|index| !matches!(index, ColumnIndexMetaData::NONE))
    });
    if missing_index_column_has_column_index {
        return Err("negative-control column unexpectedly received a column index".into());
    }

    let mut row_groups = Vec::with_capacity(metadata.num_row_groups());
    for row_group_index in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(row_group_index);
        let column_chunk = row_group.column(predicate_position);
        let column_index = column_indexes
            .get(row_group_index)
            .and_then(|indexes| indexes.get(predicate_position))
            .ok_or("predicate column index was absent")?;
        let offset_index = offset_indexes
            .get(row_group_index)
            .and_then(|indexes| indexes.get(predicate_position))
            .ok_or("predicate offset index was absent")?;
        let (mins, maxes) = int64_page_bounds(column_index)?;
        let locations = offset_index.page_locations();
        if mins.len() != locations.len() || maxes.len() != locations.len() || locations.is_empty() {
            return Err("predicate page indexes and offset indexes were inconsistent".into());
        }
        let row_group_rows = u64::try_from(row_group.num_rows())?;
        let mut pages = Vec::with_capacity(locations.len());
        for (page_index, location) in locations.iter().enumerate() {
            let first_row_index = u64::try_from(location.first_row_index)?;
            let next_row_index = locations
                .get(page_index + 1)
                .map(|next| u64::try_from(next.first_row_index))
                .transpose()?
                .unwrap_or(row_group_rows);
            pages.push(PageManifest {
                page_index,
                first_row_index,
                row_count: next_row_index
                    .checked_sub(first_row_index)
                    .ok_or("page row bounds were not monotonic")?,
                offset_bytes: u64::try_from(location.offset)?,
                length_bytes: u64::try_from(location.compressed_page_size)?,
                min_value: mins[page_index],
                max_value: maxes[page_index],
            });
        }
        row_groups.push(RowGroupManifest {
            row_group_index,
            row_count: row_group_rows,
            predicate_min: *mins.iter().min().ok_or("predicate page mins were empty")?,
            predicate_max: *maxes
                .iter()
                .max()
                .ok_or("predicate page maxes were empty")?,
            column_index_extent: ExtentManifest {
                offset_bytes: u64::try_from(
                    column_chunk
                        .column_index_offset()
                        .ok_or("predicate column index offset was absent")?,
                )?,
                length_bytes: u64::try_from(
                    column_chunk
                        .column_index_length()
                        .ok_or("predicate column index length was absent")?,
                )?,
            },
            offset_index_extent: ExtentManifest {
                offset_bytes: u64::try_from(
                    column_chunk
                        .offset_index_offset()
                        .ok_or("predicate offset index offset was absent")?,
                )?,
                length_bytes: u64::try_from(
                    column_chunk
                        .offset_index_length()
                        .ok_or("predicate offset index length was absent")?,
                )?,
            },
            pages,
        });
    }
    if row_groups.len() != row_groups_per_file {
        return Err(format!(
            "expected {row_groups_per_file} row groups but parquet wrote {}",
            row_groups.len()
        )
        .into());
    }
    Ok(FileWriteResult {
        manifest: FileManifest {
            relative_path: path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("parquet file name was not UTF-8")?
                .to_string(),
            size_bytes,
            sha256,
            row_count: u64::try_from(rows_per_row_group * row_groups_per_file)?,
            row_group_count: row_groups.len(),
            compression: "snappy",
            predicate_column_indexes_usable: true,
            missing_index_column_has_column_index,
            row_groups,
        },
        min_value: file_min,
        max_value: file_max,
    })
}

fn writer_properties(config: &FixtureConfig, rows_per_row_group: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_created_by(format!(
            "axon {FIXTURE_REVISION} generator {}",
            env!("CARGO_PKG_VERSION")
        ))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_statistics_enabled(MISSING_INDEX_COLUMN.into(), EnabledStatistics::None)
        .set_max_row_group_row_count(Some(rows_per_row_group))
        .set_data_page_size_limit(config.data_page_size_bytes)
        .set_data_page_row_count_limit(config.data_page_row_count_limit)
        .set_write_batch_size(config.data_page_row_count_limit)
        .set_dictionary_enabled(false)
        .build()
}

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new(PREDICATE_COLUMN, DataType::Int64, false),
        Field::new(MISSING_INDEX_COLUMN, DataType::Int64, false),
        Field::new("narrow_value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn fixture_batch(
    base: usize,
    predicate_values: &[i64],
    seed: u64,
    total_rows: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut event_ids = Vec::with_capacity(predicate_values.len());
    let mut missing_index_values = Vec::with_capacity(predicate_values.len());
    let mut narrow_values = Vec::with_capacity(predicate_values.len());
    let mut payloads = Vec::with_capacity(predicate_values.len());
    for (row_offset, predicate_value) in predicate_values.iter().copied().enumerate() {
        let event_id = i64::try_from(base + row_offset)?;
        if predicate_value < 0 || predicate_value >= i64::try_from(total_rows)? {
            return Err("predicate value fell outside the table domain".into());
        }
        event_ids.push(event_id);
        missing_index_values.push(predicate_value);
        narrow_values.push(predicate_value % 10_000);
        payloads.push(payload_for(seed, event_id, predicate_value));
    }
    Ok(RecordBatch::try_new(
        fixture_schema(),
        vec![
            Arc::new(Int64Array::from(event_ids)) as ArrayRef,
            Arc::new(Int64Array::from(predicate_values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(missing_index_values)) as ArrayRef,
            Arc::new(Int64Array::from(narrow_values)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
        ],
    )?)
}

fn payload_for(seed: u64, event_id: i64, predicate_value: i64) -> String {
    let mut state = seed
        ^ u64::from_le_bytes(event_id.to_le_bytes())
        ^ u64::from_le_bytes(predicate_value.to_le_bytes()).rotate_left(23);
    let mut payload = String::with_capacity(PAYLOAD_BYTES);
    for _ in 0..4 {
        state = mix64(state);
        payload.push_str(&format!("{state:016x}"));
        state = mix64(state);
        payload.push_str(&format!("{state:016x}"));
    }
    debug_assert_eq!(payload.len(), PAYLOAD_BYTES);
    payload
}

fn layout_values(
    layout: Layout,
    base: usize,
    row_count: usize,
    page_rows: usize,
    seed: u64,
) -> Vec<i64> {
    let mut values = (base..base + row_count)
        .map(|value| i64::try_from(value).expect("fixture value should fit i64"))
        .collect::<Vec<_>>();
    match layout {
        Layout::Ordered => {}
        Layout::EightPageClusters => {
            let cluster_rows = page_rows.saturating_mul(8).max(1);
            let cluster_count = row_count.div_ceil(cluster_rows);
            let mut order = (0..cluster_count).collect::<Vec<_>>();
            deterministic_shuffle(&mut order, seed);
            let original = values.clone();
            values.clear();
            for cluster in order {
                let start = cluster * cluster_rows;
                let end = (start + cluster_rows).min(row_count);
                values.extend_from_slice(&original[start..end]);
            }
        }
        Layout::FullyShuffled => deterministic_shuffle(&mut values, seed),
    }
    values
}

fn deterministic_shuffle<T>(values: &mut [T], seed: u64) {
    let mut state = seed;
    for index in (1..values.len()).rev() {
        state = mix64(state);
        let selected =
            usize::try_from(state % u64::try_from(index + 1).unwrap_or(u64::MAX)).unwrap_or(0);
        values.swap(index, selected);
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn int64_page_bounds(index: &ColumnIndexMetaData) -> Result<(Vec<i64>, Vec<i64>), Box<dyn Error>> {
    let ColumnIndexMetaData::INT64(index) = index else {
        return Err("predicate column index was not INT64".into());
    };
    let mins = index
        .min_values_iter()
        .map(|value| value.copied().ok_or("predicate page minimum was absent"))
        .collect::<Result<Vec<_>, _>>()?;
    let maxes = index
        .max_values_iter()
        .map(|value| value.copied().ok_or("predicate page maximum was absent"))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((mins, maxes))
}

fn selectivity_cases(
    total_rows: u64,
    layout: Layout,
    total_row_groups: usize,
    rows_per_row_group: usize,
    page_rows: usize,
    seed: u64,
) -> Result<Vec<SelectivityManifest>, Box<dyn Error>> {
    if u64::try_from(total_row_groups.saturating_mul(rows_per_row_group))? != total_rows {
        return Err("selectivity geometry did not cover the table row domain".into());
    }
    let expected_rows = SELECTIVITY_BASIS_POINTS.map(|basis_points| {
        total_rows
            .saturating_mul(u64::from(basis_points))
            .div_ceil(10_000)
            .min(total_rows)
    });
    let cutoffs = expected_rows.map(|rows| i64::try_from(rows).unwrap_or(i64::MAX));
    let mut event_sums = [0_u128; SELECTIVITY_BASIS_POINTS.len()];
    for row_group_index in 0..total_row_groups {
        let base = row_group_index * rows_per_row_group;
        let values = layout_values(
            layout,
            base,
            rows_per_row_group,
            page_rows,
            seed ^ u64::try_from(row_group_index)?,
        );
        for (row_offset, predicate_value) in values.into_iter().enumerate() {
            let event_id = u128::try_from(base + row_offset)?;
            for (case_index, cutoff) in cutoffs.iter().copied().enumerate() {
                if predicate_value < cutoff {
                    event_sums[case_index] = event_sums[case_index].saturating_add(event_id);
                }
            }
        }
    }

    SELECTIVITY_BASIS_POINTS
        .into_iter()
        .enumerate()
        .map(|(case_index, requested_basis_points)| {
            let expected_rows = expected_rows[case_index];
            let predicate_sum = if expected_rows == 0 {
                0_u128
            } else {
                u128::from(expected_rows)
                    .saturating_mul(u128::from(expected_rows.saturating_sub(1)))
                    / 2
            };
            let payload_bytes = u128::from(expected_rows) * u128::try_from(PAYLOAD_BYTES)?;
            let actual_basis_points = u32::try_from(
                expected_rows
                    .saturating_mul(10_000)
                    .checked_div(total_rows)
                    .unwrap_or_default(),
            )?;
            Ok(SelectivityManifest {
                requested_basis_points,
                cutoff: cutoffs[case_index],
                expected_rows,
                actual_basis_points,
                narrow_result_checksum: result_checksum(&[
                    u128::from(expected_rows),
                    predicate_sum,
                ]),
                wide_result_checksum: result_checksum(&[
                    u128::from(expected_rows),
                    predicate_sum,
                    event_sums[case_index],
                    payload_bytes,
                ]),
            })
        })
        .collect()
}

fn result_checksum(values: &[u128]) -> String {
    let canonical = values
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(":");
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

fn write_delta_log(
    table_root: &Path,
    table_name: &str,
    files: &[FileManifest],
) -> Result<(), Box<dyn Error>> {
    let schema_string = json!({
        "type": "struct",
        "fields": [
            {"name": "event_id", "type": "long", "nullable": false, "metadata": {}},
            {"name": PREDICATE_COLUMN, "type": "long", "nullable": false, "metadata": {}},
            {"name": MISSING_INDEX_COLUMN, "type": "long", "nullable": false, "metadata": {}},
            {"name": "narrow_value", "type": "long", "nullable": false, "metadata": {}},
            {"name": "payload", "type": "string", "nullable": false, "metadata": {}}
        ]
    })
    .to_string();
    let mut actions = vec![
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({
            "metaData": {
                "id": format!("axon-{FIXTURE_REVISION}-{table_name}"),
                "name": table_name,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            }
        }),
    ];
    for file in files {
        let min = file
            .row_groups
            .iter()
            .map(|row_group| row_group.predicate_min)
            .min()
            .ok_or("file row groups were empty")?;
        let max = file
            .row_groups
            .iter()
            .map(|row_group| row_group.predicate_max)
            .max()
            .ok_or("file row groups were empty")?;
        let stats = json!({
            "numRecords": file.row_count,
            "minValues": {
                PREDICATE_COLUMN: min,
                MISSING_INDEX_COLUMN: min
            },
            "maxValues": {
                PREDICATE_COLUMN: max,
                MISSING_INDEX_COLUMN: max
            },
            "nullCount": {
                "event_id": 0,
                PREDICATE_COLUMN: 0,
                MISSING_INDEX_COLUMN: 0,
                "narrow_value": 0,
                "payload": 0
            }
        })
        .to_string();
        actions.push(json!({
            "add": {
                "path": file.relative_path,
                "partitionValues": {},
                "size": file.size_bytes,
                "modificationTime": 0,
                "dataChange": true,
                "stats": stats
            }
        }));
    }
    let body = actions
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    fs::write(
        table_root.join("_delta_log/00000000000000000000.json"),
        format!("{body}\n"),
    )?;
    Ok(())
}

fn write_object_checksums(output_root: &Path) -> Result<(), Box<dyn Error>> {
    let mut paths = Vec::new();
    collect_files(output_root, &mut paths)?;
    paths.retain(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_none_or(|name| name != "object-sha256.txt" && name != "provenance.json")
    });
    paths.sort();
    let mut lines = Vec::with_capacity(paths.len());
    for path in paths {
        let relative = path
            .strip_prefix(output_root)?
            .to_string_lossy()
            .replace('\\', "/");
        lines.push(format!("{}  {relative}", sha256_file(&path)?));
    }
    fs::write(
        output_root.join("object-sha256.txt"),
        format!("{}\n", lines.join("\n")),
    )?;
    Ok(())
}

fn table_object_hash(table_root: &Path) -> Result<String, Box<dyn Error>> {
    let mut paths = Vec::new();
    collect_files(table_root, &mut paths)?;
    paths.sort();
    let mut hasher = Sha256::new();
    for path in paths {
        hasher.update(path.strip_prefix(table_root)?.to_string_lossy().as_bytes());
        hasher.update([0]);
        let mut file = fs::File::open(path)?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_files(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, paths)?;
        } else {
            paths.push(path);
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn git_provenance() -> (String, bool) {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()
        .is_none_or(|output| !output.status.success() || !output.stdout.is_empty());
    (commit, dirty)
}

fn round_up(value: usize, multiple: usize) -> usize {
    value.div_ceil(multiple).saturating_mul(multiple)
}

fn env_usize(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    env::var(name)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("{name} must be an unsigned integer: {error}").into())
        })
        .unwrap_or(Ok(default))
}

fn env_u64(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    env::var(name)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| format!("{name} must be an unsigned integer: {error}").into())
        })
        .unwrap_or(Ok(default))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_are_deterministic_permutations_with_distinct_locality() {
        let ordered = layout_values(Layout::Ordered, 0, 32_768, 1_024, 7);
        let clustered = layout_values(Layout::EightPageClusters, 0, 32_768, 1_024, 7);
        let shuffled = layout_values(Layout::FullyShuffled, 0, 32_768, 1_024, 7);
        let mut clustered_sorted = clustered.clone();
        clustered_sorted.sort_unstable();
        let mut shuffled_sorted = shuffled.clone();
        shuffled_sorted.sort_unstable();
        assert_eq!(clustered_sorted, ordered);
        assert_eq!(shuffled_sorted, ordered);
        assert_ne!(clustered, ordered);
        assert_ne!(shuffled, ordered);
        assert_eq!(
            clustered,
            layout_values(Layout::EightPageClusters, 0, 32_768, 1_024, 7)
        );
    }

    #[test]
    fn selectivity_cases_cover_frozen_cutoffs_and_checksums() {
        let cases = selectivity_cases(10_000, Layout::Ordered, 1, 10_000, 1_000, 7)
            .expect("selectivity cases should build");
        assert_eq!(cases.len(), 7);
        assert_eq!(cases[0].expected_rows, 0);
        assert_eq!(cases[1].expected_rows, 10);
        assert_eq!(cases[5].expected_rows, 5_000);
        assert_eq!(cases[6].expected_rows, 10_000);
        assert_ne!(
            cases[2].narrow_result_checksum,
            cases[2].wide_result_checksum
        );
    }

    #[test]
    fn small_file_has_usable_predicate_indexes_and_missing_index_control() {
        let root = env::temp_dir().join(format!(
            "axon-page-index-v2-generator-test-{}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).expect("stale generator test root should remove");
        }
        fs::create_dir_all(&root).expect("generator test root should create");
        let config = FixtureConfig {
            target_active_bytes: 1024 * 1024,
            data_page_size_bytes: 64 * 1024,
            data_page_row_count_limit: 1_024,
            estimated_compressed_bytes_per_row: 32,
            seed: 11,
        };
        let path = root.join("test.parquet");
        let result = write_parquet_file(
            &path,
            fixture_schema(),
            writer_properties(&config, 8_192),
            &config,
            Layout::Ordered,
            0,
            1,
            8_192,
            8_192,
        )
        .expect("small indexed parquet file should generate");
        assert!(result.manifest.predicate_column_indexes_usable);
        assert!(!result.manifest.missing_index_column_has_column_index);
        assert_eq!(result.manifest.row_groups.len(), 1);
        assert!(result.manifest.row_groups[0].pages.len() >= 8);
        fs::remove_dir_all(root).expect("generator test root should remove");
    }
}

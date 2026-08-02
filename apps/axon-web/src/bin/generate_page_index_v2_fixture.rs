use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::page_index::column_index::ColumnIndexMetaData;
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
use deltalake::parquet::file::serialized_reader::ReadOptionsBuilder;
use deltalake::parquet::record::Field as ParquetField;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

const SCHEMA_VERSION: u32 = 3;
const FIXTURE_REVISION: &str = "s3-browser-perf-page-index-v2";
const IMMUTABLE_PREFIX: &str = "fixtures/s3-browser-perf-page-index-v2";
const PUBLIC_BUCKET: &str = "axon-public-s3-fixture-452456948477";
const PUBLIC_REGION: &str = "us-east-2";
const PUBLIC_TABLE_URI_BASE: &str =
    "s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf-page-index-v2";
const MANIFEST_FILE: &str = "fixture-manifest.json";
const CHECKSUM_FILE: &str = "object-sha256.txt";
const PROVENANCE_FILE: &str = "provenance.json";
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
struct FixtureManifest {
    schema_version: u32,
    fixture_revision: String,
    immutable_prefix: String,
    generated_table_uri_base: String,
    publication: PublicationManifest,
    generator: GeneratorManifest,
    writer: WriterManifest,
    predicate_envelope: PredicateEnvelopeManifest,
    tables: Vec<TableManifest>,
    negative_cases: Vec<NegativeCaseManifest>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PublicationManifest {
    bucket: String,
    region: String,
    immutable_prefix: String,
    table_uri_base: String,
    completion_marker: String,
    completion_marker_upload_order: String,
    cloud_upload_attempted: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct GeneratorManifest {
    package_version: String,
    git_commit: String,
    git_worktree_clean: bool,
    source_sha256: String,
    cargo_lock_sha256: String,
    seed: String,
    target_active_bytes_per_table: usize,
    estimated_compressed_bytes_per_row: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct WriterManifest {
    parquet_version: String,
    compression: String,
    statistics: String,
    data_page_size_target_bytes: usize,
    data_page_row_count_limit: usize,
    dictionary_enabled: bool,
    missing_index_column_statistics: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct PredicateEnvelopeManifest {
    predicate_column: String,
    supported_form: String,
    narrow_projection_sql: String,
    wide_projection_sql: String,
    selectivity_basis_points: [u32; 7],
}

#[derive(Debug, Deserialize, Serialize)]
struct NegativeCaseManifest {
    name: String,
    sql_predicate: String,
    expected_plan: String,
    expected_reason: String,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
struct FileManifest {
    relative_path: String,
    size_bytes: u64,
    sha256: String,
    row_count: u64,
    row_group_count: usize,
    compression: String,
    predicate_column_indexes_usable: bool,
    missing_index_column_has_column_index: bool,
    row_groups: Vec<RowGroupManifest>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RowGroupManifest {
    row_group_index: usize,
    row_count: u64,
    predicate_min: i64,
    predicate_max: i64,
    column_index_extent: ExtentManifest,
    offset_index_extent: ExtentManifest,
    pages: Vec<PageManifest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ExtentManifest {
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PageManifest {
    page_index: usize,
    first_row_index: u64,
    row_count: u64,
    offset_bytes: u64,
    length_bytes: u64,
    min_value: i64,
    max_value: i64,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug)]
struct GitProvenance {
    commit: String,
    worktree_clean: bool,
}

#[derive(Debug)]
struct DecodedRow {
    event_id: i64,
    predicate_value: i64,
    payload_length: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileIdentity {
    length: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    change_seconds: i64,
    #[cfg(unix)]
    change_nanoseconds: i64,
}

struct SnapshotFile {
    path: PathBuf,
    file: fs::File,
    identity: FileIdentity,
}

struct FixtureSnapshot {
    root: PathBuf,
    objects: BTreeMap<String, SnapshotFile>,
}

impl FixtureSnapshot {
    fn open(root: &Path) -> Result<Self, Box<dyn Error>> {
        let root_metadata = fs::symlink_metadata(root)?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err("fixture root was a symlink or non-directory".into());
        }
        let root = root.canonicalize()?;
        let mut objects = BTreeMap::new();
        Self::open_directory(&root, &root, &mut objects)?;
        Ok(Self { root, objects })
    }

    fn open_directory(
        root: &Path,
        directory: &Path,
        objects: &mut BTreeMap<String, SnapshotFile>,
    ) -> Result<(), Box<dyn Error>> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_symlink() {
                return Err(format!(
                    "symlink/non-regular fixture object was rejected: {}",
                    path.display()
                )
                .into());
            }
            if file_type.is_dir() {
                Self::open_directory(root, &path, objects)?;
                continue;
            }
            if !file_type.is_file() {
                return Err(format!(
                    "symlink/non-regular fixture object was rejected: {}",
                    path.display()
                )
                .into());
            }
            let relative = relative_utf8_path(root, &path)?;
            let before = file_identity(&fs::symlink_metadata(&path)?);
            let file = open_regular_file(&path)?;
            let after = file_identity(&file.metadata()?);
            if before != after {
                return Err(format!("fixture object changed while opening: {relative}").into());
            }
            if objects
                .insert(
                    relative.clone(),
                    SnapshotFile {
                        path,
                        file,
                        identity: after,
                    },
                )
                .is_some()
            {
                return Err(format!("fixture inventory repeated {relative}").into());
            }
        }
        Ok(())
    }

    fn inventory(&self) -> BTreeSet<String> {
        self.objects.keys().cloned().collect()
    }

    fn object(&self, relative: &str) -> Result<&SnapshotFile, Box<dyn Error>> {
        self.objects
            .get(relative)
            .ok_or_else(|| format!("fixture snapshot omitted {relative}").into())
    }

    fn open_clone(&self, relative: &str) -> Result<fs::File, Box<dyn Error>> {
        let object = self.object(relative)?;
        require_snapshot_identity(object)?;
        let mut file = object.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }

    fn bytes(&self, relative: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let object = self.object(relative)?;
        require_snapshot_identity(object)?;
        let mut file = object.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::with_capacity(usize::try_from(object.identity.length)?);
        file.read_to_end(&mut bytes)?;
        require_snapshot_identity(object)?;
        Ok(bytes)
    }

    fn text(&self, relative: &str) -> Result<String, Box<dyn Error>> {
        Ok(String::from_utf8(self.bytes(relative)?)?)
    }

    fn sha256(&self, relative: &str) -> Result<String, Box<dyn Error>> {
        Ok(sha256_bytes(&self.bytes(relative)?))
    }

    fn verify_unchanged(&self) -> Result<(), Box<dyn Error>> {
        for object in self.objects.values() {
            require_snapshot_identity(object)?;
            let path_metadata = fs::symlink_metadata(&object.path)?;
            if path_metadata.file_type().is_symlink()
                || !path_metadata.is_file()
                || file_identity(&path_metadata) != object.identity
            {
                return Err(format!(
                    "fixture object changed or became non-regular: {}",
                    object.path.display()
                )
                .into());
            }
        }
        let current = FixtureSnapshot::open(&self.root)?;
        require_exact_inventory(&self.inventory(), &current.inventory(), "stable fixture")?;
        for (relative, original) in &self.objects {
            if current.object(relative)?.identity != original.identity {
                return Err(format!("fixture object identity changed: {relative}").into());
            }
        }
        Ok(())
    }
}

fn file_identity(metadata: &fs::Metadata) -> FileIdentity {
    FileIdentity {
        length: metadata.len(),
        modified: metadata.modified().ok(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
        #[cfg(unix)]
        change_seconds: metadata.ctime(),
        #[cfg(unix)]
        change_nanoseconds: metadata.ctime_nsec(),
    }
}

fn open_regular_file(path: &Path) -> Result<fs::File, Box<dyn Error>> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    Ok(options.open(path)?)
}

fn require_snapshot_identity(object: &SnapshotFile) -> Result<(), Box<dyn Error>> {
    if file_identity(&object.file.metadata()?) != object.identity {
        return Err(format!(
            "fixture object changed during verification: {}",
            object.path.display()
        )
        .into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let first = args.next();
    if first.as_deref() == Some("--verify") {
        let fixture_root = args
            .next()
            .map(PathBuf::from)
            .ok_or("--verify requires a fixture root")?;
        if args.next().is_some() {
            return Err("--verify accepts exactly one fixture root".into());
        }
        let fixture_root = absolute_path(&fixture_root)?;
        verify_fixture(&fixture_root)?;
        println!(
            "Verified page-index v2 fixture at {}",
            fixture_root.display()
        );
        return Ok(());
    }
    if args.next().is_some() {
        return Err("generation accepts at most one output root".into());
    }
    let output_root = first
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fixtures/s3-browser-perf-page-index-v2"));
    let output_root = absolute_path(&output_root)?;
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
    let table_uri_override = env::var("AXON_PAGE_INDEX_V2_TABLE_URI_BASE").ok();
    let table_uri_base = canonical_table_uri_base(table_uri_override.as_deref())?;
    let git = git_provenance_at(&env::current_dir()?);

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
        fixture_revision: FIXTURE_REVISION.to_string(),
        immutable_prefix: IMMUTABLE_PREFIX.to_string(),
        generated_table_uri_base: table_uri_base.clone(),
        publication: PublicationManifest {
            bucket: PUBLIC_BUCKET.to_string(),
            region: PUBLIC_REGION.to_string(),
            immutable_prefix: IMMUTABLE_PREFIX.to_string(),
            table_uri_base,
            completion_marker: PROVENANCE_FILE.to_string(),
            completion_marker_upload_order: "last".to_string(),
            cloud_upload_attempted: false,
        },
        generator: GeneratorManifest {
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            git_commit: git.commit,
            git_worktree_clean: git.worktree_clean,
            source_sha256: sha256_bytes(include_bytes!("generate_page_index_v2_fixture.rs")),
            cargo_lock_sha256: sha256_bytes(include_bytes!("../../../../Cargo.lock")),
            seed: config.seed.to_string(),
            target_active_bytes_per_table: config.target_active_bytes,
            estimated_compressed_bytes_per_row: config.estimated_compressed_bytes_per_row,
        },
        writer: WriterManifest {
            parquet_version: "2.0".to_string(),
            compression: "snappy".to_string(),
            statistics: "page".to_string(),
            data_page_size_target_bytes: config.data_page_size_bytes,
            data_page_row_count_limit: config.data_page_row_count_limit,
            dictionary_enabled: false,
            missing_index_column_statistics: "none".to_string(),
        },
        predicate_envelope: PredicateEnvelopeManifest {
            predicate_column: PREDICATE_COLUMN.to_string(),
            supported_form: "predicate_value < signed_integer_literal".to_string(),
            narrow_projection_sql:
                "SELECT COUNT(*), SUM(predicate_value) FROM <table> WHERE predicate_value < <cutoff>".to_string(),
            wide_projection_sql:
                "SELECT COUNT(*), SUM(predicate_value), SUM(event_id), SUM(LENGTH(payload)) FROM <table> WHERE predicate_value < <cutoff>".to_string(),
            selectivity_basis_points: SELECTIVITY_BASIS_POINTS,
        },
        tables,
        negative_cases: vec![
            NegativeCaseManifest {
                name: "unsupported-compound".to_string(),
                sql_predicate: "predicate_value < <cutoff> AND narrow_value >= 0".to_string(),
                expected_plan: "skip".to_string(),
                expected_reason: "unsupported_predicate".to_string(),
            },
            NegativeCaseManifest {
                name: "unsupported-expression".to_string(),
                sql_predicate: "predicate_value + 1 < <cutoff>".to_string(),
                expected_plan: "skip".to_string(),
                expected_reason: "unsupported_predicate".to_string(),
            },
            NegativeCaseManifest {
                name: "type-incompatible".to_string(),
                sql_predicate: "predicate_value < 'not-an-integer'".to_string(),
                expected_plan: "skip".to_string(),
                expected_reason: "unsupported_predicate".to_string(),
            },
            NegativeCaseManifest {
                name: "missing-column-index".to_string(),
                sql_predicate: "missing_index_value < <cutoff>".to_string(),
                expected_plan: "skip".to_string(),
                expected_reason: "missing_or_invalid_indexes".to_string(),
            },
        ],
    };
    let manifest_path = output_root.join(MANIFEST_FILE);
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    write_object_checksums(output_root)?;
    let provenance = json!({
        "schema_version": SCHEMA_VERSION,
        "fixture_revision": FIXTURE_REVISION,
        "publication": &manifest.publication,
        "generator": &manifest.generator,
        "writer": &manifest.writer,
        "manifest_sha256": sha256_file(&manifest_path)?,
        "object_checksums_sha256": sha256_file(&output_root.join(CHECKSUM_FILE))?,
        "cloud_upload_attempted": false,
        "completion_marker": PROVENANCE_FILE,
        "completion_marker_upload_order": "last",
    });
    fs::write(
        output_root.join(PROVENANCE_FILE),
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
    let table_uri = build_table_uri(table_uri_base, &relative_path);
    Ok(TableManifest {
        name: name.clone(),
        relative_path,
        table_uri,
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
            compression: "snappy".to_string(),
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
    let mut actions = vec![
        json!({"protocol": delta_protocol_payload()}),
        json!({"metaData": delta_metadata_payload(table_name)}),
    ];
    for file in files {
        actions.push(json!({"add": delta_add_payload(file)?}));
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

fn delta_schema_string() -> String {
    json!({
        "type": "struct",
        "fields": [
            {"name": "event_id", "type": "long", "nullable": false, "metadata": {}},
            {"name": PREDICATE_COLUMN, "type": "long", "nullable": false, "metadata": {}},
            {"name": MISSING_INDEX_COLUMN, "type": "long", "nullable": false, "metadata": {}},
            {"name": "narrow_value", "type": "long", "nullable": false, "metadata": {}},
            {"name": "payload", "type": "string", "nullable": false, "metadata": {}}
        ]
    })
    .to_string()
}

fn delta_protocol_payload() -> serde_json::Value {
    json!({"minReaderVersion": 1, "minWriterVersion": 2})
}

fn delta_metadata_payload(table_name: &str) -> serde_json::Value {
    json!({
        "id": format!("axon-{FIXTURE_REVISION}-{table_name}"),
        "name": table_name,
        "format": {"provider": "parquet", "options": {}},
        "schemaString": delta_schema_string(),
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 0
    })
}

fn delta_add_payload(file: &FileManifest) -> Result<serde_json::Value, Box<dyn Error>> {
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
    Ok(json!({
        "path": file.relative_path,
        "partitionValues": {},
        "size": file.size_bytes,
        "modificationTime": 0,
        "dataChange": true,
        "stats": stats
    }))
}

fn write_object_checksums(output_root: &Path) -> Result<(), Box<dyn Error>> {
    let mut paths = Vec::new();
    collect_files(output_root, &mut paths)?;
    paths.retain(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_none_or(|name| name != CHECKSUM_FILE && name != PROVENANCE_FILE)
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
        output_root.join(CHECKSUM_FILE),
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

fn table_object_hash_snapshot(
    snapshot: &FixtureSnapshot,
    table_relative_path: &str,
) -> Result<String, Box<dyn Error>> {
    let prefix = format!("{}/", table_relative_path.trim_end_matches('/'));
    let paths = snapshot
        .objects
        .keys()
        .filter(|path| path.starts_with(&prefix))
        .cloned()
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Err(format!("table snapshot inventory was empty for {table_relative_path}").into());
    }
    let mut hasher = Sha256::new();
    for path in paths {
        hasher.update(
            path.strip_prefix(&prefix)
                .ok_or("table object path escaped prefix")?,
        );
        hasher.update([0]);
        hasher.update(snapshot.bytes(&path)?);
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

fn absolute_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    })
}

fn build_table_uri(table_uri_base: &str, relative_path: &str) -> String {
    format!(
        "{}/{}",
        table_uri_base.trim_end_matches('/'),
        relative_path.trim_start_matches('/')
    )
}

fn canonical_table_uri_base(override_value: Option<&str>) -> Result<String, Box<dyn Error>> {
    let value = override_value.unwrap_or(PUBLIC_TABLE_URI_BASE);
    if value.trim_end_matches('/') != PUBLIC_TABLE_URI_BASE {
        return Err(format!(
            "AXON_PAGE_INDEX_V2_TABLE_URI_BASE must equal the reserved immutable base {PUBLIC_TABLE_URI_BASE}"
        )
        .into());
    }
    Ok(PUBLIC_TABLE_URI_BASE.to_string())
}

fn git_provenance_at(root: &Path) -> GitProvenance {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let worktree_clean = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .current_dir(root)
        .output()
        .ok()
        .is_some_and(|output| output.status.success() && output.stdout.is_empty());
    GitProvenance {
        commit,
        worktree_clean,
    }
}

fn require_exact_inventory(
    expected: &BTreeSet<String>,
    actual: &BTreeSet<String>,
    label: &str,
) -> Result<(), Box<dyn Error>> {
    if expected == actual {
        return Ok(());
    }
    let missing = expected.difference(actual).cloned().collect::<Vec<_>>();
    let extra = actual.difference(expected).cloned().collect::<Vec<_>>();
    Err(format!("{label} inventory differed: missing={missing:?}, extra={extra:?}").into())
}

fn verify_fixture(fixture_root: &Path) -> Result<(), Box<dyn Error>> {
    let snapshot = FixtureSnapshot::open(fixture_root)?;
    let manifest: FixtureManifest = serde_json::from_slice(&snapshot.bytes(MANIFEST_FILE)?)?;
    verify_manifest_contract(&manifest)?;

    let mut expected_objects = BTreeSet::from([MANIFEST_FILE.to_string()]);
    let mut table_names = BTreeSet::new();
    let mut layout_geometry = BTreeSet::new();
    for table in &manifest.tables {
        if !table_names.insert(table.name.clone()) {
            return Err(format!("duplicate table name in manifest: {}", table.name).into());
        }
        layout_geometry.insert((table.layout, table.geometry));
        validate_relative_path(&table.relative_path)?;
        let expected_uri =
            build_table_uri(&manifest.generated_table_uri_base, &table.relative_path);
        if table.table_uri != expected_uri {
            return Err(format!(
                "table URI did not equal generated_table_uri_base plus relative_path: expected {expected_uri}, got {}",
                table.table_uri
            )
            .into());
        }
        expected_objects.insert(format!(
            "{}/_delta_log/00000000000000000000.json",
            table.relative_path
        ));
        for file in &table.active_files {
            validate_relative_path(&file.relative_path)?;
            expected_objects.insert(format!("{}/{}", table.relative_path, file.relative_path));
        }
    }
    let expected_matrix = Layout::ALL
        .into_iter()
        .flat_map(|layout| {
            Geometry::ALL
                .into_iter()
                .map(move |geometry| (layout, geometry))
        })
        .collect::<BTreeSet<_>>();
    if layout_geometry != expected_matrix || manifest.tables.len() != expected_matrix.len() {
        return Err("manifest did not contain the exact three-layout/two-geometry matrix".into());
    }

    let checksums = parse_object_checksums(&snapshot.text(CHECKSUM_FILE)?)?;
    let checksum_inventory = checksums.keys().cloned().collect::<BTreeSet<_>>();
    require_exact_inventory(&expected_objects, &checksum_inventory, "checksum")?;

    let disk_inventory = snapshot.inventory();
    let mut expected_disk = expected_objects.clone();
    expected_disk.insert(CHECKSUM_FILE.to_string());
    expected_disk.insert(PROVENANCE_FILE.to_string());
    require_exact_inventory(&expected_disk, &disk_inventory, "disk")?;

    for table in &manifest.tables {
        verify_table(&snapshot, table)?;
    }

    for (relative, expected_sha) in &checksums {
        let actual_sha = snapshot.sha256(relative)?;
        if &actual_sha != expected_sha {
            return Err(format!("object checksum differed for {relative}").into());
        }
    }
    verify_provenance(&snapshot, &manifest)?;
    snapshot.verify_unchanged()?;
    Ok(())
}

fn verify_manifest_contract(manifest: &FixtureManifest) -> Result<(), Box<dyn Error>> {
    if manifest.schema_version != SCHEMA_VERSION
        || manifest.fixture_revision != FIXTURE_REVISION
        || manifest.immutable_prefix != IMMUTABLE_PREFIX
        || manifest.generated_table_uri_base != PUBLIC_TABLE_URI_BASE
    {
        return Err(
            "manifest identity or schema contract was not the publishable v3 contract".into(),
        );
    }
    let publication = &manifest.publication;
    if publication.bucket != PUBLIC_BUCKET
        || publication.region != PUBLIC_REGION
        || publication.immutable_prefix != IMMUTABLE_PREFIX
        || publication.table_uri_base != PUBLIC_TABLE_URI_BASE
        || publication.completion_marker != PROVENANCE_FILE
        || publication.completion_marker_upload_order != "last"
        || publication.cloud_upload_attempted
    {
        return Err(
            "manifest publication contract differed from the reserved immutable destination".into(),
        );
    }
    if manifest.predicate_envelope.predicate_column != PREDICATE_COLUMN
        || manifest.predicate_envelope.selectivity_basis_points != SELECTIVITY_BASIS_POINTS
        || manifest.negative_cases.len() != 4
        || manifest.writer.parquet_version != "2.0"
        || manifest.writer.compression != "snappy"
        || manifest.writer.statistics != "page"
        || manifest.writer.dictionary_enabled
        || manifest.writer.missing_index_column_statistics != "none"
    {
        return Err("manifest writer or predicate contract differed".into());
    }
    Ok(())
}

fn verify_provenance(
    snapshot: &FixtureSnapshot,
    manifest: &FixtureManifest,
) -> Result<(), Box<dyn Error>> {
    let provenance: serde_json::Value = serde_json::from_slice(&snapshot.bytes(PROVENANCE_FILE)?)?;
    let expected_manifest_sha = provenance
        .get("manifest_sha256")
        .and_then(serde_json::Value::as_str)
        .ok_or("provenance manifest_sha256 was absent")?;
    let expected_checksums_sha = provenance
        .get("object_checksums_sha256")
        .and_then(serde_json::Value::as_str)
        .ok_or("provenance object_checksums_sha256 was absent")?;
    if expected_manifest_sha != snapshot.sha256(MANIFEST_FILE)?
        || expected_checksums_sha != snapshot.sha256(CHECKSUM_FILE)?
    {
        return Err("provenance identity chain did not match manifest and checksum bytes".into());
    }
    if provenance.get("schema_version") != Some(&json!(SCHEMA_VERSION))
        || provenance.get("fixture_revision") != Some(&json!(FIXTURE_REVISION))
        || provenance.get("publication") != Some(&serde_json::to_value(&manifest.publication)?)
        || provenance.get("generator") != Some(&serde_json::to_value(&manifest.generator)?)
        || provenance.get("writer") != Some(&serde_json::to_value(&manifest.writer)?)
        || provenance.get("cloud_upload_attempted") != Some(&json!(false))
        || provenance.get("completion_marker") != Some(&json!(PROVENANCE_FILE))
        || provenance.get("completion_marker_upload_order") != Some(&json!("last"))
    {
        return Err("provenance contract did not pin the manifest generation contract".into());
    }

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let source = repo_root.join("apps/axon-web/src/bin/generate_page_index_v2_fixture.rs");
    let lockfile = repo_root.join("Cargo.lock");
    if manifest.generator.source_sha256 != sha256_file(&source)?
        || manifest.generator.cargo_lock_sha256 != sha256_file(&lockfile)?
    {
        return Err("generator source or Cargo.lock hash differed from provenance".into());
    }
    let current = git_provenance_at(&repo_root);
    require_clean_head_provenance(
        &manifest.generator.git_commit,
        manifest.generator.git_worktree_clean,
        &current,
    )?;
    Ok(())
}

fn require_clean_head_provenance(
    generator_commit: &str,
    generator_worktree_clean: bool,
    current: &GitProvenance,
) -> Result<(), Box<dyn Error>> {
    if generator_commit == "unknown" {
        return Err("generator git commit was unknown".into());
    }
    if !generator_worktree_clean {
        return Err("generator worktree was not clean".into());
    }
    if current.commit == "unknown" || current.commit != generator_commit {
        return Err("generator git commit did not equal HEAD".into());
    }
    if !current.worktree_clean {
        return Err("current verifier worktree was not clean".into());
    }
    Ok(())
}

fn verify_table(snapshot: &FixtureSnapshot, table: &TableManifest) -> Result<(), Box<dyn Error>> {
    let expected_file_count = table.geometry.active_file_count();
    if table.active_file_count != expected_file_count
        || table.active_files.len() != expected_file_count
        || table.row_groups_per_file != table.geometry.row_groups_per_file()
    {
        return Err(format!(
            "table {} geometry differed from its manifest contract",
            table.name
        )
        .into());
    }
    let mut decoded_rows = Vec::with_capacity(usize::try_from(table.total_rows)?);
    let mut active_bytes = 0_u64;
    let mut active_names = BTreeSet::new();
    for file in &table.active_files {
        if !active_names.insert(file.relative_path.clone()) {
            return Err(format!(
                "table {} repeated active file {}",
                table.name, file.relative_path
            )
            .into());
        }
        let relative = format!("{}/{}", table.relative_path, file.relative_path);
        let rows = verify_parquet_file(snapshot, &relative, file)?;
        active_bytes = active_bytes
            .checked_add(file.size_bytes)
            .ok_or("active byte count overflowed")?;
        decoded_rows.extend(rows);
    }
    if active_bytes != table.actual_active_bytes
        || u64::try_from(decoded_rows.len())? != table.total_rows
    {
        return Err(format!("table {} active bytes or rows differed", table.name).into());
    }
    verify_selectivity(table, &decoded_rows)?;
    verify_delta_log(snapshot, table, &active_names)?;
    if table.table_object_sha256 != table_object_hash_snapshot(snapshot, &table.relative_path)? {
        return Err(format!("table {} object hash differed", table.name).into());
    }
    Ok(())
}

fn verify_parquet_file(
    snapshot: &FixtureSnapshot,
    relative: &str,
    file: &FileManifest,
) -> Result<Vec<DecodedRow>, Box<dyn Error>> {
    let size_bytes = snapshot.object(relative)?.identity.length;
    if size_bytes == 0 || size_bytes != file.size_bytes || snapshot.sha256(relative)? != file.sha256
    {
        return Err(format!("Parquet size or hash differed for {relative}").into());
    }
    if file.compression != "snappy" || !file.predicate_column_indexes_usable {
        return Err(format!("Parquet manifest flags were invalid for {relative}").into());
    }

    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(snapshot.open_clone(relative)?, options)?;
    let metadata = reader.metadata();
    verify_physical_writer_contract(metadata)?;
    let columns = metadata.file_metadata().schema_descr().columns();
    let predicate_position = columns
        .iter()
        .position(|column| column.path().string() == PREDICATE_COLUMN)
        .ok_or("predicate column was absent from Parquet schema")?;
    let missing_position = columns
        .iter()
        .position(|column| column.path().string() == MISSING_INDEX_COLUMN)
        .ok_or("negative-control column was absent from Parquet schema")?;
    if columns[predicate_position].physical_type() != deltalake::parquet::basic::Type::INT64
        || columns[predicate_position].max_def_level() != 0
    {
        return Err("predicate column was not required INT64".into());
    }
    let column_indexes = metadata
        .column_index()
        .ok_or("Parquet column indexes were absent")?;
    let offset_indexes = metadata
        .offset_index()
        .ok_or("Parquet offset indexes were absent")?;
    if metadata.num_row_groups() != file.row_group_count
        || file.row_groups.len() != file.row_group_count
    {
        return Err("Parquet row-group count differed from manifest".into());
    }
    for indexes in column_indexes {
        if indexes
            .get(missing_position)
            .is_some_and(|index| !matches!(index, ColumnIndexMetaData::NONE))
        {
            return Err("negative-control column unexpectedly had a column index".into());
        }
    }
    if file.missing_index_column_has_column_index {
        return Err("negative-control manifest flag claimed a column index".into());
    }

    let decoded = decode_rows(snapshot, relative)?;
    if u64::try_from(decoded.len())? != file.row_count {
        return Err("decoded Parquet row count differed from manifest".into());
    }
    let mut decoded_base = 0_usize;
    for row_group_index in 0..metadata.num_row_groups() {
        let physical_row_group = metadata.row_group(row_group_index);
        let recorded = file
            .row_groups
            .get(row_group_index)
            .ok_or("recorded Parquet row group was absent")?;
        if recorded.row_group_index != row_group_index
            || recorded.row_count != u64::try_from(physical_row_group.num_rows())?
        {
            return Err("row-group metadata differed from manifest".into());
        }
        let column_chunk = physical_row_group.column(predicate_position);
        let physical_column_extent = metadata_extent(
            column_chunk.column_index_offset(),
            column_chunk.column_index_length().map(i64::from),
            "column index",
            size_bytes,
        )?;
        let physical_offset_extent = metadata_extent(
            column_chunk.offset_index_offset(),
            column_chunk.offset_index_length().map(i64::from),
            "offset index",
            size_bytes,
        )?;
        if physical_column_extent != recorded.column_index_extent
            || physical_offset_extent != recorded.offset_index_extent
        {
            return Err("index extent metadata differed from manifest".into());
        }
        let column_index = column_indexes
            .get(row_group_index)
            .and_then(|row_group| row_group.get(predicate_position))
            .ok_or("predicate column index was absent")?;
        let offset_index = offset_indexes
            .get(row_group_index)
            .and_then(|row_group| row_group.get(predicate_position))
            .ok_or("predicate offset index was absent")?;
        let (mins, maxes) = int64_page_bounds(column_index)?;
        let locations = offset_index.page_locations();
        if locations.is_empty()
            || locations.len() != mins.len()
            || locations.len() != maxes.len()
            || locations.len() != recorded.pages.len()
        {
            return Err("predicate page and offset indexes were inconsistent".into());
        }
        let row_group_rows = usize::try_from(recorded.row_count)?;
        let row_group_decoded = decoded
            .get(decoded_base..decoded_base + row_group_rows)
            .ok_or("decoded rows did not cover row group")?;
        let mut previous_first = None;
        for (page_index, location) in locations.iter().enumerate() {
            let first = usize::try_from(location.first_row_index)?;
            if page_index == 0 && first != 0 {
                return Err("first page row position did not start at zero".into());
            }
            if previous_first.is_some_and(|previous| first <= previous) {
                return Err("page first-row positions were not strictly increasing".into());
            }
            previous_first = Some(first);
            let next = locations
                .get(page_index + 1)
                .map(|next| usize::try_from(next.first_row_index))
                .transpose()?
                .unwrap_or(row_group_rows);
            if next <= first || next > row_group_rows {
                return Err("page row locations did not cover the row group exactly".into());
            }
            let page_extent = metadata_extent(
                Some(location.offset),
                Some(i64::from(location.compressed_page_size)),
                "data page",
                size_bytes,
            )?;
            let physical_page = PageManifest {
                page_index,
                first_row_index: u64::try_from(first)?,
                row_count: u64::try_from(next - first)?,
                offset_bytes: page_extent.offset_bytes,
                length_bytes: page_extent.length_bytes,
                min_value: mins[page_index],
                max_value: maxes[page_index],
            };
            if recorded.pages.get(page_index) != Some(&physical_page) {
                return Err("page metadata differed from manifest".into());
            }
            let page_rows = &row_group_decoded[first..next];
            let decoded_min = page_rows
                .iter()
                .map(|row| row.predicate_value)
                .min()
                .ok_or("decoded page was empty")?;
            let decoded_max = page_rows
                .iter()
                .map(|row| row.predicate_value)
                .max()
                .ok_or("decoded page was empty")?;
            if decoded_min != physical_page.min_value || decoded_max != physical_page.max_value {
                return Err("decoded page values differed from page-index bounds".into());
            }
        }
        if recorded.predicate_min != *mins.iter().min().ok_or("page minima were empty")?
            || recorded.predicate_max != *maxes.iter().max().ok_or("page maxima were empty")?
        {
            return Err("row-group predicate bounds differed from physical indexes".into());
        }
        decoded_base += row_group_rows;
    }
    if decoded_base != decoded.len() {
        return Err("row-group page coverage did not cover the decoded file".into());
    }
    Ok(decoded)
}

fn verify_physical_writer_contract(
    metadata: &deltalake::parquet::file::metadata::ParquetMetaData,
) -> Result<(), Box<dyn Error>> {
    use deltalake::parquet::basic::Encoding;

    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        for column in row_group.columns() {
            if column.compression() != Compression::SNAPPY {
                return Err(format!(
                    "Parquet row group {row_group_index} column {} was not physically Snappy-compressed",
                    column.column_path()
                )
                .into());
            }
            if column.dictionary_page_offset().is_some()
                || column.encodings().any(|encoding| {
                    matches!(
                        encoding,
                        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
                    )
                })
            {
                return Err(format!(
                    "Parquet row group {row_group_index} column {} used dictionary encoding",
                    column.column_path()
                )
                .into());
            }
        }
    }
    Ok(())
}

fn decode_rows(
    snapshot: &FixtureSnapshot,
    relative: &str,
) -> Result<Vec<DecodedRow>, Box<dyn Error>> {
    let reader = SerializedFileReader::new(snapshot.open_clone(relative)?)?;
    let rows = reader.get_row_iter(None)?;
    let mut decoded = Vec::new();
    for row in rows {
        let row = row?;
        let mut event_id = None;
        let mut predicate_value = None;
        let mut payload_length = None;
        for (name, field) in row.get_column_iter() {
            match (name.as_str(), field) {
                ("event_id", ParquetField::Long(value)) => event_id = Some(*value),
                (PREDICATE_COLUMN, ParquetField::Long(value)) => predicate_value = Some(*value),
                ("payload", ParquetField::Str(value)) => payload_length = Some(value.len()),
                _ => {}
            }
        }
        decoded.push(DecodedRow {
            event_id: event_id.ok_or("decoded row omitted event_id")?,
            predicate_value: predicate_value.ok_or("decoded row omitted predicate_value")?,
            payload_length: payload_length.ok_or("decoded row omitted payload")?,
        });
    }
    Ok(decoded)
}

fn metadata_extent(
    offset: Option<i64>,
    length: Option<i64>,
    label: &str,
    file_size: u64,
) -> Result<ExtentManifest, Box<dyn Error>> {
    let offset_bytes = u64::try_from(offset.ok_or_else(|| format!("{label} offset was absent"))?)?;
    let length_bytes = u64::try_from(length.ok_or_else(|| format!("{label} length was absent"))?)?;
    if length_bytes == 0
        || offset_bytes
            .checked_add(length_bytes)
            .is_none_or(|end| end > file_size)
    {
        return Err(format!("{label} extent was empty or outside file bounds").into());
    }
    Ok(ExtentManifest {
        offset_bytes,
        length_bytes,
    })
}

fn verify_selectivity(table: &TableManifest, rows: &[DecodedRow]) -> Result<(), Box<dyn Error>> {
    if table.selectivity_cases.len() != SELECTIVITY_BASIS_POINTS.len() {
        return Err(format!(
            "table {} did not record seven selectivity cases",
            table.name
        )
        .into());
    }
    for (case_index, case) in table.selectivity_cases.iter().enumerate() {
        if case.requested_basis_points != SELECTIVITY_BASIS_POINTS[case_index] {
            return Err("selectivity basis-point cases were not canonical".into());
        }
        let matching = rows
            .iter()
            .filter(|row| row.predicate_value < case.cutoff)
            .collect::<Vec<_>>();
        let count = u64::try_from(matching.len())?;
        let predicate_sum = matching.iter().try_fold(0_u128, |sum, row| {
            Ok::<_, Box<dyn Error>>(sum + u128::try_from(row.predicate_value)?)
        })?;
        let event_sum = matching.iter().try_fold(0_u128, |sum, row| {
            Ok::<_, Box<dyn Error>>(sum + u128::try_from(row.event_id)?)
        })?;
        let payload_sum = matching.iter().try_fold(0_u128, |sum, row| {
            Ok::<_, Box<dyn Error>>(sum + u128::try_from(row.payload_length)?)
        })?;
        let actual_basis_points = u32::try_from(
            count
                .saturating_mul(10_000)
                .checked_div(table.total_rows)
                .unwrap_or_default(),
        )?;
        if count != case.expected_rows
            || actual_basis_points != case.actual_basis_points
            || result_checksum(&[u128::from(count), predicate_sum]) != case.narrow_result_checksum
            || result_checksum(&[u128::from(count), predicate_sum, event_sum, payload_sum])
                != case.wide_result_checksum
        {
            return Err(format!(
                "table {} selectivity result differed at {} bp",
                table.name, case.requested_basis_points
            )
            .into());
        }
    }
    Ok(())
}

fn verify_delta_log(
    snapshot: &FixtureSnapshot,
    table: &TableManifest,
    active_names: &BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
    let log_path = format!(
        "{}/_delta_log/00000000000000000000.json",
        table.relative_path
    );
    let body = snapshot.text(&log_path)?;
    validate_delta_log_body(&body, table, active_names)
}

fn validate_delta_log_body(
    body: &str,
    table: &TableManifest,
    active_names: &BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
    let mut add_actions = BTreeMap::new();
    let mut protocol_seen = false;
    let mut metadata_seen = false;
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let action: serde_json::Value = serde_json::from_str(line)?;
        let object = action
            .as_object()
            .ok_or("Delta log action was not a JSON object")?;
        if object.len() != 1 {
            return Err("Delta log line did not contain exactly one action".into());
        }
        let (kind, payload) = object.iter().next().ok_or("Delta action was empty")?;
        match kind.as_str() {
            "protocol" => {
                if protocol_seen || payload != &delta_protocol_payload() {
                    return Err("Delta protocol action differed from the canonical contract".into());
                }
                protocol_seen = true;
            }
            "metaData" => {
                if metadata_seen || payload != &delta_metadata_payload(&table.name) {
                    return Err(
                        "Delta metadata action differed from the canonical schema contract".into(),
                    );
                }
                metadata_seen = true;
            }
            "add" => {
                let path = payload
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("Delta add path was absent")?
                    .to_string();
                let file = table
                    .active_files
                    .iter()
                    .find(|file| file.relative_path == path)
                    .ok_or_else(|| format!("Delta add referenced unexpected path {path}"))?;
                if payload != &delta_add_payload(file)? {
                    return Err(format!("Delta add payload differed for {path}").into());
                }
                if add_actions.insert(path.clone(), payload.clone()).is_some() {
                    return Err(format!("Delta log repeated add path {path}").into());
                }
            }
            _ => {
                return Err(format!("unexpected Delta action was rejected: {kind}").into());
            }
        }
    }
    let delta_names = add_actions.keys().cloned().collect::<BTreeSet<_>>();
    require_exact_inventory(active_names, &delta_names, "Delta add")?;
    if !protocol_seen || !metadata_seen {
        return Err("Delta log did not contain exactly one protocol and metadata action".into());
    }
    Ok(())
}

fn parse_object_checksums(body: &str) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    let mut checksums = BTreeMap::new();
    for line in body.lines() {
        let (sha, relative) = line
            .split_once("  ")
            .ok_or("checksum line did not use sha256 double-space path format")?;
        if sha.len() != 64
            || !sha
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("checksum line did not contain a lowercase SHA-256".into());
        }
        validate_relative_path(relative)?;
        if checksums
            .insert(relative.to_string(), sha.to_string())
            .is_some()
        {
            return Err(format!("checksum inventory repeated {relative}").into());
        }
    }
    Ok(checksums)
}

fn validate_relative_path(path: &str) -> Result<(), Box<dyn Error>> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(format!(
            "fixture path was not a safe relative path: {}",
            path.display()
        )
        .into());
    }
    Ok(())
}

fn relative_utf8_path(root: &Path, path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(path
        .strip_prefix(root)?
        .to_str()
        .ok_or("fixture path was not UTF-8")?
        .replace('\\', "/"))
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
    fn table_uri_uses_the_complete_relative_table_path() {
        assert_eq!(
            build_table_uri(
                "s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf-page-index-v2/",
                "tables/ordered-few-large",
            ),
            "s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf-page-index-v2/tables/ordered-few-large"
        );
    }

    #[test]
    fn canonical_table_uri_base_defaults_and_rejects_conflicting_overrides() {
        assert_eq!(
            canonical_table_uri_base(None).expect("default URI should be canonical"),
            PUBLIC_TABLE_URI_BASE
        );
        assert_eq!(
            canonical_table_uri_base(Some(PUBLIC_TABLE_URI_BASE))
                .expect("matching override should be canonical"),
            PUBLIC_TABLE_URI_BASE
        );
        assert!(canonical_table_uri_base(Some("s3://different/prefix")).is_err());
    }

    #[test]
    fn git_provenance_counts_untracked_but_not_ignored_files_as_dirty() {
        let root = temporary_test_root("git-provenance");
        fs::create_dir_all(&root).expect("git test root should create");
        run_git(&root, &["init", "-q"]);
        run_git(
            &root,
            &["config", "user.email", "fixture-test@axon.invalid"],
        );
        run_git(&root, &["config", "user.name", "Axon Fixture Test"]);
        fs::write(root.join(".gitignore"), "target/\n").expect("gitignore should write");
        fs::write(root.join("tracked.txt"), "tracked\n").expect("tracked file should write");
        run_git(&root, &["add", ".gitignore", "tracked.txt"]);
        run_git(&root, &["commit", "-qm", "fixture"]);

        fs::create_dir_all(root.join("target")).expect("ignored directory should create");
        fs::write(root.join("target/output"), "ignored\n").expect("ignored output should write");
        let clean = git_provenance_at(&root);
        assert!(clean.worktree_clean);

        fs::write(root.join("untracked.txt"), "untracked\n").expect("untracked file should write");
        let dirty = git_provenance_at(&root);
        assert!(!dirty.worktree_clean);
        assert_ne!(dirty.commit, "unknown");
        fs::remove_dir_all(root).expect("git test root should remove");
    }

    #[test]
    fn git_provenance_counts_tracked_changes_as_dirty() {
        let root = temporary_git_repository("tracked-dirty");
        fs::write(root.join("tracked.txt"), "changed\n").expect("tracked file should mutate");
        assert!(!git_provenance_at(&root).worktree_clean);
        fs::remove_dir_all(root).expect("tracked-dirty root should remove");
    }

    #[test]
    fn inventory_requires_exact_set_equality() {
        let expected = ["fixture-manifest.json", "tables/a/part.parquet"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let missing = ["fixture-manifest.json"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let extra = [
            "fixture-manifest.json",
            "tables/a/part.parquet",
            "unexpected.txt",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert!(require_exact_inventory(&expected, &expected, "test").is_ok());
        assert!(require_exact_inventory(&expected, &missing, "test").is_err());
        assert!(require_exact_inventory(&expected, &extra, "test").is_err());
    }

    #[test]
    fn physical_verifier_rejects_a_missing_predicate_column_index() {
        let root = temporary_test_root("missing-index");
        fs::create_dir_all(&root).expect("missing-index root should create");
        let path = root.join("missing-index.parquet");
        let config = FixtureConfig {
            target_active_bytes: 1024 * 1024,
            data_page_size_bytes: 64 * 1024,
            data_page_row_count_limit: 1_024,
            estimated_compressed_bytes_per_row: 32,
            seed: 17,
        };
        let file = fs::File::create(&path).expect("missing-index parquet should create");
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ArrowWriter::try_new(file, fixture_schema(), Some(properties))
            .expect("missing-index writer should create");
        let values = (0_i64..8_192).collect::<Vec<_>>();
        writer
            .write(
                &fixture_batch(0, &values, config.seed, values.len()).expect("batch should build"),
            )
            .expect("missing-index batch should write");
        writer.close().expect("missing-index writer should close");
        let manifest = FileManifest {
            relative_path: "missing-index.parquet".to_string(),
            size_bytes: fs::metadata(&path).expect("metadata should read").len(),
            sha256: sha256_file(&path).expect("hash should compute"),
            row_count: 8_192,
            row_group_count: 1,
            compression: "snappy".to_string(),
            predicate_column_indexes_usable: true,
            missing_index_column_has_column_index: false,
            row_groups: vec![RowGroupManifest {
                row_group_index: 0,
                row_count: 8_192,
                predicate_min: 0,
                predicate_max: 8_191,
                column_index_extent: ExtentManifest {
                    offset_bytes: 1,
                    length_bytes: 1,
                },
                offset_index_extent: ExtentManifest {
                    offset_bytes: 1,
                    length_bytes: 1,
                },
                pages: Vec::new(),
            }],
        };
        let snapshot = FixtureSnapshot::open(&root).expect("missing-index snapshot should open");
        let error = verify_parquet_file(&snapshot, "missing-index.parquet", &manifest)
            .expect_err("missing predicate indexes must be rejected")
            .to_string();
        assert!(
            error.contains("column index offset was absent")
                || error.contains("predicate column index was absent")
                || error.contains("column indexes were absent"),
            "{error}"
        );
        fs::remove_dir_all(root).expect("missing-index root should remove");
    }

    #[test]
    fn publishable_provenance_rejects_dirty_unknown_and_non_head_generation() {
        let head = GitProvenance {
            commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            worktree_clean: true,
        };
        assert!(require_clean_head_provenance(&head.commit, true, &head).is_ok());
        assert!(require_clean_head_provenance(&head.commit, false, &head).is_err());
        assert!(require_clean_head_provenance("unknown", true, &head).is_err());
        assert!(require_clean_head_provenance(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            true,
            &head
        )
        .is_err());
    }

    #[test]
    fn delta_verifier_rejects_remove_actions() {
        let (root, table, active_names) = canonical_delta_test_table("remove");
        let log_path = root.join("_delta_log/00000000000000000000.json");
        let mut body = fs::read_to_string(&log_path).expect("Delta log should read");
        body.push_str("{\"remove\":{\"path\":\"part-00000.parquet\",\"deletionTimestamp\":0,\"dataChange\":true}}\n");
        assert!(validate_delta_log_body(&body, &table, &active_names).is_err());
        fs::remove_dir_all(root).expect("Delta remove root should remove");
    }

    #[test]
    fn delta_verifier_rejects_unknown_actions() {
        let (root, table, active_names) = canonical_delta_test_table("unknown");
        let log_path = root.join("_delta_log/00000000000000000000.json");
        let mut body = fs::read_to_string(&log_path).expect("Delta log should read");
        body.push_str("{\"txn\":{\"appId\":\"unexpected\",\"version\":1}}\n");
        assert!(validate_delta_log_body(&body, &table, &active_names).is_err());
        fs::remove_dir_all(root).expect("Delta unknown root should remove");
    }

    #[test]
    fn delta_verifier_rejects_wrong_protocol() {
        let (root, table, active_names) = canonical_delta_test_table("protocol");
        let body = fs::read_to_string(root.join("_delta_log/00000000000000000000.json"))
            .expect("Delta log should read")
            .replacen("\"minReaderVersion\":1", "\"minReaderVersion\":2", 1);
        assert!(validate_delta_log_body(&body, &table, &active_names).is_err());
        fs::remove_dir_all(root).expect("Delta protocol root should remove");
    }

    #[test]
    fn delta_verifier_rejects_wrong_metadata_schema() {
        let (root, table, active_names) = canonical_delta_test_table("metadata");
        let body = fs::read_to_string(root.join("_delta_log/00000000000000000000.json"))
            .expect("Delta log should read")
            .replacen(
                "\\\"type\\\":\\\"long\\\"",
                "\\\"type\\\":\\\"string\\\"",
                1,
            );
        assert!(validate_delta_log_body(&body, &table, &active_names).is_err());
        fs::remove_dir_all(root).expect("Delta metadata root should remove");
    }

    #[cfg(unix)]
    #[test]
    fn fixture_snapshot_rejects_file_and_directory_symlinks() {
        use std::os::unix::fs::symlink;

        let external = temporary_test_root("external-symlink-target");
        fs::create_dir_all(&external).expect("external symlink target should create");
        fs::write(external.join("object.parquet"), "PAR1externalPAR1")
            .expect("external object should write");

        let file_root = temporary_test_root("file-symlink-root");
        fs::create_dir_all(&file_root).expect("file symlink root should create");
        symlink(
            external.join("object.parquet"),
            file_root.join("object.parquet"),
        )
        .expect("file symlink should create");
        assert!(FixtureSnapshot::open(&file_root).is_err());

        let directory_root = temporary_test_root("directory-symlink-root");
        fs::create_dir_all(&directory_root).expect("directory symlink root should create");
        symlink(&external, directory_root.join("tables")).expect("directory symlink should create");
        assert!(FixtureSnapshot::open(&directory_root).is_err());

        fs::remove_dir_all(file_root).expect("file symlink root should remove");
        fs::remove_dir_all(directory_root).expect("directory symlink root should remove");
        fs::remove_dir_all(external).expect("external symlink target should remove");
    }

    #[test]
    fn fixture_snapshot_rejects_in_place_mutation_after_open() {
        let root = temporary_test_root("snapshot-mutation");
        fs::create_dir_all(&root).expect("snapshot root should create");
        fs::write(root.join("object.txt"), "before").expect("snapshot object should write");
        let snapshot = FixtureSnapshot::open(&root).expect("snapshot should open");
        fs::write(root.join("object.txt"), "after-after").expect("snapshot object should mutate");
        assert!(snapshot.verify_unchanged().is_err());
        fs::remove_dir_all(root).expect("snapshot root should remove");
    }

    #[test]
    fn physical_writer_verifier_rejects_non_snappy_compression() {
        let (root, path) = write_writer_contract_test_file(
            "zstd",
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_dictionary_enabled(false)
                .build(),
        );
        let reader = indexed_reader(&path);
        assert!(verify_physical_writer_contract(reader.metadata()).is_err());
        fs::remove_dir_all(root).expect("zstd root should remove");
    }

    #[test]
    fn physical_writer_verifier_rejects_dictionary_encoding() {
        let (root, path) = write_writer_contract_test_file(
            "dictionary",
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_dictionary_enabled(true)
                .build(),
        );
        let reader = indexed_reader(&path);
        assert!(
            reader
                .metadata()
                .row_groups()
                .iter()
                .any(|row_group| row_group
                    .columns()
                    .iter()
                    .flat_map(|column| column.encodings())
                    .any(|encoding| matches!(
                        encoding,
                        deltalake::parquet::basic::Encoding::PLAIN_DICTIONARY
                            | deltalake::parquet::basic::Encoding::RLE_DICTIONARY
                    ))),
            "test fixture must physically contain dictionary encoding"
        );
        assert!(verify_physical_writer_contract(reader.metadata()).is_err());
        fs::remove_dir_all(root).expect("dictionary root should remove");
    }

    fn temporary_test_root(name: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "axon-page-index-v2-{name}-{}-{}",
            std::process::id(),
            mix64(name.len() as u64)
        ))
    }

    fn temporary_git_repository(name: &str) -> PathBuf {
        let root = temporary_test_root(name);
        fs::create_dir_all(&root).expect("git test root should create");
        run_git(&root, &["init", "-q"]);
        run_git(
            &root,
            &["config", "user.email", "fixture-test@axon.invalid"],
        );
        run_git(&root, &["config", "user.name", "Axon Fixture Test"]);
        fs::write(root.join(".gitignore"), "target/\n").expect("gitignore should write");
        fs::write(root.join("tracked.txt"), "tracked\n").expect("tracked file should write");
        run_git(&root, &["add", ".gitignore", "tracked.txt"]);
        run_git(&root, &["commit", "-qm", "fixture"]);
        root
    }

    fn canonical_delta_test_table(name: &str) -> (PathBuf, TableManifest, BTreeSet<String>) {
        let root = temporary_test_root(&format!("delta-{name}"));
        fs::create_dir_all(root.join("_delta_log")).expect("Delta log root should create");
        let file = canonical_test_file_manifest();
        write_delta_log(&root, name, std::slice::from_ref(&file))
            .expect("canonical Delta log should write");
        let active_names = BTreeSet::from([file.relative_path.clone()]);
        let table = TableManifest {
            name: name.to_string(),
            relative_path: format!("tables/{name}"),
            table_uri: build_table_uri(PUBLIC_TABLE_URI_BASE, &format!("tables/{name}")),
            layout: Layout::Ordered,
            geometry: Geometry::FewLarge,
            target_active_bytes: 1,
            actual_active_bytes: file.size_bytes,
            target_file_bytes: 1,
            active_file_count: 1,
            row_groups_per_file: 1,
            rows_per_row_group: 1,
            total_rows: 1,
            table_object_sha256: String::new(),
            active_files: vec![file],
            selectivity_cases: Vec::new(),
        };
        (root, table, active_names)
    }

    fn canonical_test_file_manifest() -> FileManifest {
        FileManifest {
            relative_path: "part-00000.parquet".to_string(),
            size_bytes: 123,
            sha256: "0".repeat(64),
            row_count: 1,
            row_group_count: 1,
            compression: "snappy".to_string(),
            predicate_column_indexes_usable: true,
            missing_index_column_has_column_index: false,
            row_groups: vec![RowGroupManifest {
                row_group_index: 0,
                row_count: 1,
                predicate_min: 0,
                predicate_max: 0,
                column_index_extent: ExtentManifest {
                    offset_bytes: 1,
                    length_bytes: 1,
                },
                offset_index_extent: ExtentManifest {
                    offset_bytes: 2,
                    length_bytes: 1,
                },
                pages: vec![PageManifest {
                    page_index: 0,
                    first_row_index: 0,
                    row_count: 1,
                    offset_bytes: 4,
                    length_bytes: 1,
                    min_value: 0,
                    max_value: 0,
                }],
            }],
        }
    }

    fn write_writer_contract_test_file(
        name: &str,
        properties: WriterProperties,
    ) -> (PathBuf, PathBuf) {
        let root = temporary_test_root(&format!("writer-{name}"));
        fs::create_dir_all(&root).expect("writer test root should create");
        let path = root.join("test.parquet");
        let file = fs::File::create(&path).expect("writer test Parquet should create");
        let mut writer = ArrowWriter::try_new(file, fixture_schema(), Some(properties))
            .expect("writer test should create");
        let values = (0_i64..16_384).collect::<Vec<_>>();
        writer
            .write(&fixture_batch(0, &values, 23, values.len()).expect("batch should build"))
            .expect("writer test batch should write");
        writer.close().expect("writer test should close");
        (root, path)
    }

    fn indexed_reader(path: &Path) -> SerializedFileReader<fs::File> {
        SerializedFileReader::new_with_options(
            fs::File::open(path).expect("indexed test file should open"),
            ReadOptionsBuilder::new().with_page_index().build(),
        )
        .expect("indexed test reader should open")
    }

    fn run_git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(root)
            .status()
            .expect("git should run");
        assert!(status.success(), "git {args:?} should succeed");
    }

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

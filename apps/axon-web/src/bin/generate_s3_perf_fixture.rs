use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fs;
use std::io::Read;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
use deltalake::protocol::SaveMode;
use deltalake::{checkpoints, DeltaTable};
use serde::Serialize;
use sha2::{Digest, Sha256};

const FIXTURE_SCHEMA_VERSION: u32 = 1;
const FIXTURE_REVISION: &str = "s3-browser-perf-v1";
const DEFAULT_TARGET_ACTIVE_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_ACTIVE_FILE_COUNT: usize = 8;
const DEFAULT_ROW_GROUPS_PER_FILE: usize = 4;
const ESTIMATED_COMPRESSED_BYTES_PER_ROW: usize = 128;
const DEFAULT_DATA_PAGE_SIZE_BYTES: usize = 64 * 1024;
const DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT: usize = 16 * 1024;
const BASE_EVENT_DATE_DAYS: i32 = 20_454; // 2026-01-01
const MICROS_PER_DAY: i64 = 86_400_000_000;
const FIXTURE_NAME: &str = "S3 browser performance Delta table";
const TABLE_NAME: &str = "axon_s3_browser_perf_fixture";

#[derive(Debug, Clone)]
struct FixtureConfig {
    target_active_bytes: usize,
    active_file_count: usize,
    row_groups_per_file: usize,
    rows_per_row_group: usize,
    data_page_size_bytes: usize,
    data_page_row_count_limit: usize,
    seed: u64,
}

impl FixtureConfig {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let target_active_bytes = env_usize(
            "AXON_S3_PERF_FIXTURE_TARGET_ACTIVE_BYTES",
            DEFAULT_TARGET_ACTIVE_BYTES,
        )?;
        let active_file_count = env_usize(
            "AXON_S3_PERF_FIXTURE_ACTIVE_FILES",
            DEFAULT_ACTIVE_FILE_COUNT,
        )?;
        let row_groups_per_file = env_usize(
            "AXON_S3_PERF_FIXTURE_ROW_GROUPS_PER_FILE",
            DEFAULT_ROW_GROUPS_PER_FILE,
        )?;
        let derived_rows =
            derive_rows_per_row_group(target_active_bytes, active_file_count, row_groups_per_file);
        let rows_per_row_group =
            env_usize("AXON_S3_PERF_FIXTURE_ROWS_PER_ROW_GROUP", derived_rows)?;
        let data_page_size_bytes = env_usize(
            "AXON_S3_PERF_FIXTURE_DATA_PAGE_SIZE_BYTES",
            DEFAULT_DATA_PAGE_SIZE_BYTES,
        )?;
        let data_page_row_count_limit = env_usize(
            "AXON_S3_PERF_FIXTURE_DATA_PAGE_ROW_COUNT_LIMIT",
            DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT,
        )?;
        let seed = env_u64("AXON_S3_PERF_FIXTURE_SEED", 0xA501_5EED_DA7A_2026)?;

        let config = Self {
            target_active_bytes,
            active_file_count,
            row_groups_per_file,
            rows_per_row_group,
            data_page_size_bytes,
            data_page_row_count_limit,
            seed,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if !(4..=12).contains(&self.active_file_count) {
            return Err("AXON_S3_PERF_FIXTURE_ACTIVE_FILES must be between 4 and 12".into());
        }
        if self.row_groups_per_file < 2 {
            return Err("AXON_S3_PERF_FIXTURE_ROW_GROUPS_PER_FILE must be at least 2".into());
        }
        if self.rows_per_row_group < 256 {
            return Err("AXON_S3_PERF_FIXTURE_ROWS_PER_ROW_GROUP must be at least 256".into());
        }
        if self.data_page_size_bytes < 8 * 1024 {
            return Err("AXON_S3_PERF_FIXTURE_DATA_PAGE_SIZE_BYTES must be at least 8192".into());
        }
        if self.data_page_row_count_limit < 512 {
            return Err(
                "AXON_S3_PERF_FIXTURE_DATA_PAGE_ROW_COUNT_LIMIT must be at least 512".into(),
            );
        }
        Ok(())
    }

    fn rows_per_file(&self) -> usize {
        self.rows_per_row_group * self.row_groups_per_file
    }
}

#[derive(Debug, Clone, Serialize)]
struct FixtureManifest {
    schema_version: u32,
    fixture_revision: &'static str,
    name: &'static str,
    table_uri: String,
    region: String,
    generator: FixtureGeneratorManifest,
    expected_latest_version: i64,
    checkpoint_version: i64,
    target_active_data_bytes: u64,
    actual_active_data_bytes: u64,
    active_file_count: usize,
    row_groups_per_file: usize,
    rows_per_row_group: usize,
    generated_steps: Vec<GeneratedStep>,
    objects: Vec<ManifestObject>,
    active_data_files: Vec<DataFileInventory>,
}

#[derive(Debug, Clone, Serialize)]
struct FixtureGeneratorManifest {
    seed: u64,
    target_active_bytes: usize,
    active_file_count: usize,
    row_groups_per_file: usize,
    rows_per_row_group: usize,
    data_page_size_bytes: usize,
    data_page_row_count_limit: usize,
}

impl From<&FixtureConfig> for FixtureGeneratorManifest {
    fn from(config: &FixtureConfig) -> Self {
        Self {
            seed: config.seed,
            target_active_bytes: config.target_active_bytes,
            active_file_count: config.active_file_count,
            row_groups_per_file: config.row_groups_per_file,
            rows_per_row_group: config.rows_per_row_group,
            data_page_size_bytes: config.data_page_size_bytes,
            data_page_row_count_limit: config.data_page_row_count_limit,
        }
    }
}

#[derive(Debug, Serialize)]
struct FixtureProvenance {
    schema_version: u32,
    fixture_revision: &'static str,
    table_uri: String,
    region: String,
    manifest_sha256: String,
    object_checksums_sha256: String,
    required_object_count: usize,
    active_file_count: usize,
    active_data_bytes: u64,
    generator: FixtureGeneratorProvenance,
}

#[derive(Debug, Serialize)]
struct FixtureGeneratorProvenance {
    binary: &'static str,
    #[serde(flatten)]
    config: FixtureGeneratorManifest,
}

#[derive(Debug, Clone, Serialize)]
struct GeneratedStep {
    version: i64,
    label: String,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct ManifestObject {
    relative_path: String,
    url_path: String,
    kind: ObjectKind,
    size_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObjectKind {
    CommitJson,
    CheckpointParquet,
    LastCheckpoint,
}

#[derive(Debug, Clone, Serialize)]
struct DataFileInventory {
    relative_path: String,
    url_path: String,
    size_bytes: u64,
    row_count: i64,
    row_group_count: usize,
    compression: String,
    partition_values: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct S3UploadConfig {
    bucket: String,
    prefix: String,
    region: String,
}

#[derive(Debug)]
enum UploadGate {
    Ready(S3UploadConfig),
    MissingEnv(Vec<&'static str>),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let output_root_arg = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fixtures/s3-perf-generated"));
    let output_root = absolute_output_root(&output_root_arg)?;
    let config = FixtureConfig::from_env()?;
    let upload_gate = S3UploadConfig::from_env()?;
    let (table_uri_hint, region_hint) = match &upload_gate {
        UploadGate::Ready(upload) => (upload.table_uri(), upload.region.clone()),
        UploadGate::MissingEnv(_) => (
            "s3://<bucket>/<prefix>/table".to_string(),
            "<region>".to_string(),
        ),
    };

    let manifest = generate_fixture(&output_root, &config, table_uri_hint, region_hint).await?;
    write_s3_access_artifacts(&output_root, &upload_gate)?;
    report_local_generation(&output_root, &manifest);

    match upload_gate {
        UploadGate::Ready(upload) => {
            if !aws_credentials_available()? {
                println!(
                    "S3 upload skipped: AWS credentials are unavailable. Run `aws sso login` and rerun this command."
                );
                return Ok(());
            }
            upload_fixture(&output_root.join("table"), &upload)?;
            println!("S3 upload complete: {}", upload.table_uri());
            println!(
                "Validate with: AXON_LIVE_PUBLIC_S3_TABLE_URI={} AXON_LIVE_PUBLIC_S3_REGION={} CI=1 npm run test:browser:public-s3-live -- --reporter=line",
                upload.table_uri(),
                upload.region
            );
        }
        UploadGate::MissingEnv(missing) => {
            println!(
                "S3 upload skipped: missing {}.",
                missing
                    .iter()
                    .map(|name| format!("`{name}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    Ok(())
}

fn absolute_output_root(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    })
}

async fn generate_fixture(
    output_root: &Path,
    config: &FixtureConfig,
    table_uri: String,
    region: String,
) -> Result<FixtureManifest, Box<dyn Error>> {
    if output_root.exists() {
        fs::remove_dir_all(output_root)?;
    }
    fs::create_dir_all(output_root)?;
    let table_root = output_root.join("table");
    let table_url = deltalake::ensure_table_uri(table_root.to_string_lossy())?;
    let table = DeltaTable::try_from_url(table_url).await?;
    let table = table
        .create()
        .with_columns(table_columns())
        .with_partition_columns(vec!["event_date", "region"])
        .with_table_name(TABLE_NAME)
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

    let mut generated_steps = vec![GeneratedStep {
        version: 0,
        label: "create table".to_string(),
        detail: "Create browser-compatible Delta metadata with event_date and region partitions."
            .to_string(),
    }];

    let table = write_perf_batch(
        table,
        config,
        batch_for_file(config, 0, BatchPhase::Initial),
        SaveMode::Append,
    )
    .await?;
    generated_steps.push(GeneratedStep {
        version: 1,
        label: "append historical north america".to_string(),
        detail: "Write old active data that will later be removed by overwrite.".to_string(),
    });

    let table = write_perf_batch(
        table,
        config,
        batch_for_file(config, 1, BatchPhase::Initial),
        SaveMode::Append,
    )
    .await?;
    generated_steps.push(GeneratedStep {
        version: 2,
        label: "append historical europe + checkpoint".to_string(),
        detail: "Append a second historical partition and write a checkpoint at version 2."
            .to_string(),
    });

    checkpoints::create_checkpoint(&table, None).await?;

    let mut table = write_perf_batch(
        table,
        config,
        batch_for_file(config, 0, BatchPhase::Active),
        SaveMode::Overwrite,
    )
    .await?;
    generated_steps.push(GeneratedStep {
        version: 3,
        label: "overwrite active file 1".to_string(),
        detail: "Full-table overwrite removes historical files and starts the active snapshot."
            .to_string(),
    });

    for file_index in 1..config.active_file_count {
        table = write_perf_batch(
            table,
            config,
            batch_for_file(config, file_index, BatchPhase::Active),
            SaveMode::Append,
        )
        .await?;
        generated_steps.push(GeneratedStep {
            version: 3 + i64::try_from(file_index)?,
            label: format!("append active file {}", file_index + 1),
            detail: format!(
                "Append deterministic active partition data file {} of {}.",
                file_index + 1,
                config.active_file_count
            ),
        });
    }

    sanitize_delta_commit_info_locations(&table_root, &table_uri)?;

    let active_relative_paths = active_relative_paths(&table, &table_root)?;
    let active_data_files =
        collect_active_data_files(output_root, &table_root, &active_relative_paths)?;
    let actual_active_data_bytes = active_data_files
        .iter()
        .map(|file| file.size_bytes)
        .sum::<u64>();

    let manifest = FixtureManifest {
        schema_version: FIXTURE_SCHEMA_VERSION,
        fixture_revision: FIXTURE_REVISION,
        name: FIXTURE_NAME,
        table_uri: table_uri.clone(),
        region: region.clone(),
        generator: FixtureGeneratorManifest::from(config),
        expected_latest_version: i64::try_from(generated_steps.len())? - 1,
        checkpoint_version: 2,
        target_active_data_bytes: u64::try_from(config.target_active_bytes)?,
        actual_active_data_bytes,
        active_file_count: active_data_files.len(),
        row_groups_per_file: config.row_groups_per_file,
        rows_per_row_group: config.rows_per_row_group,
        generated_steps,
        objects: collect_manifest_objects(output_root, &table_root)?,
        active_data_files,
    };
    fs::write(
        output_root.join("s3-perf-fixture-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    write_fixture_provenance(output_root, &manifest, table_uri, region)?;
    Ok(manifest)
}

fn write_fixture_provenance(
    output_root: &Path,
    manifest: &FixtureManifest,
    table_uri: String,
    region: String,
) -> Result<(), Box<dyn Error>> {
    let checksum_path = output_root.join("s3-perf-object-sha256.txt");
    let mut checksum_lines = Vec::new();
    for relative_path in manifest
        .objects
        .iter()
        .map(|object| object.relative_path.as_str())
        .chain(
            manifest
                .active_data_files
                .iter()
                .map(|file| file.relative_path.as_str()),
        )
    {
        let object_path = output_root.join("table").join(relative_path);
        checksum_lines.push(format!(
            "{}  table/{}",
            sha256_file(&object_path)?,
            relative_path
        ));
    }
    checksum_lines.sort();
    fs::write(&checksum_path, format!("{}\n", checksum_lines.join("\n")))?;

    let manifest_path = output_root.join("s3-perf-fixture-manifest.json");
    let provenance = FixtureProvenance {
        schema_version: FIXTURE_SCHEMA_VERSION,
        fixture_revision: FIXTURE_REVISION,
        table_uri,
        region,
        manifest_sha256: sha256_file(&manifest_path)?,
        object_checksums_sha256: sha256_file(&checksum_path)?,
        required_object_count: checksum_lines.len(),
        active_file_count: manifest.active_file_count,
        active_data_bytes: manifest.actual_active_data_bytes,
        generator: FixtureGeneratorProvenance {
            binary: "generate-s3-perf-fixture",
            config: manifest.generator.clone(),
        },
    };
    fs::write(
        output_root.join("s3-perf-provenance.json"),
        serde_json::to_vec_pretty(&provenance)?,
    )?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

async fn write_perf_batch(
    table: DeltaTable,
    config: &FixtureConfig,
    batch: RecordBatch,
    mode: SaveMode,
) -> Result<DeltaTable, Box<dyn Error>> {
    Ok(table
        .write(vec![batch])
        .with_save_mode(mode)
        .with_partition_columns(vec!["event_date", "region"])
        .with_target_file_size(None::<NonZeroU64>)
        .with_write_batch_size(config.rows_per_row_group)
        .with_writer_properties(writer_properties(config))
        .await?)
}

fn writer_properties(config: &FixtureConfig) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(config.rows_per_row_group))
        .set_data_page_size_limit(config.data_page_size_bytes)
        .set_data_page_row_count_limit(config.data_page_row_count_limit)
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(config.data_page_size_bytes)
        .set_write_batch_size(
            config
                .data_page_row_count_limit
                .min(config.rows_per_row_group),
        )
        .build()
}

#[derive(Clone, Copy, Debug)]
enum BatchPhase {
    Initial,
    Active,
}

fn batch_for_file(config: &FixtureConfig, file_index: usize, phase: BatchPhase) -> RecordBatch {
    let row_count = config.rows_per_file();
    let partition = partition_for_file(file_index, phase);
    let mut event_ids = Vec::with_capacity(row_count);
    let mut event_ts = Vec::with_capacity(row_count);
    let mut event_dates = Vec::with_capacity(row_count);
    let mut regions = Vec::with_capacity(row_count);
    let mut customer_ids = Vec::with_capacity(row_count);
    let mut session_ids = Vec::with_capacity(row_count);
    let mut statuses = Vec::with_capacity(row_count);
    let mut amounts = Vec::with_capacity(row_count);
    let mut discounts = Vec::with_capacity(row_count);
    let mut quantities = Vec::with_capacity(row_count);
    let mut is_mobile = Vec::with_capacity(row_count);
    let mut is_priority = Vec::with_capacity(row_count);
    let mut skus = Vec::with_capacity(row_count);
    let mut channels = Vec::with_capacity(row_count);
    let mut notes = Vec::with_capacity(row_count);

    let phase_offset = match phase {
        BatchPhase::Initial => 10_000_000_000_i64,
        BatchPhase::Active => 20_000_000_000_i64,
    };
    let mut rng = DeterministicRng::new(config.seed ^ ((file_index as u64 + 1) * 0x9E37_79B9));

    for row_index in 0..row_count {
        let global_row = phase_offset
            + i64::try_from(file_index * row_count + row_index)
                .expect("fixture row id should fit in i64");
        let skew = rng.next_u32() % 100;
        let status = status_for(skew, row_index);
        let amount = amount_for(&mut rng, status, row_index);
        let maybe_discount = if row_index % 11 == 0 {
            None
        } else {
            Some(((rng.next_u32() % 2_500) as f64) / 100.0)
        };
        let quantity = i32::try_from(1 + (rng.next_u32() % 9)).expect("quantity should fit i32");
        let day_offset = i64::from(partition.event_date_days) * MICROS_PER_DAY;
        let row_micros = i64::try_from((row_index % 86_400) * 1_000_000)
            .expect("row timestamp offset should fit i64");

        event_ids.push(global_row);
        event_ts.push(day_offset + row_micros);
        event_dates.push(partition.event_date);
        regions.push(partition.region.to_string());
        customer_ids.push(format!("cust-{:08x}-{:08x}", rng.next_u32(), row_index));
        session_ids.push(format!(
            "sess-{:08x}{:08x}{:08x}",
            rng.next_u32(),
            rng.next_u32(),
            file_index
        ));
        statuses.push(status.to_string());
        amounts.push(amount);
        discounts.push(maybe_discount);
        quantities.push(quantity);
        is_mobile.push((rng.next_u32() % 100) < 68);
        is_priority.push(if row_index % 13 == 0 {
            None
        } else {
            Some((rng.next_u32() % 100) < 17)
        });
        skus.push(format!(
            "sku-{}-{:06x}",
            row_index % 512,
            rng.next_u32() & 0xFF_FFFF
        ));
        channels.push(channel_for(rng.next_u32()).to_string());
        notes.push(if row_index % 7 == 0 {
            None
        } else {
            Some(format!(
                "note-{:08x}-{:08x}-{:08x}-{:08x}",
                rng.next_u32(),
                rng.next_u32(),
                rng.next_u32(),
                row_index
            ))
        });
    }

    RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(event_ids)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(event_ts).with_timezone("UTC")) as ArrayRef,
            Arc::new(StringArray::from(event_dates)) as ArrayRef,
            Arc::new(StringArray::from(regions)) as ArrayRef,
            Arc::new(StringArray::from(customer_ids)) as ArrayRef,
            Arc::new(StringArray::from(session_ids)) as ArrayRef,
            Arc::new(StringArray::from(statuses)) as ArrayRef,
            Arc::new(Float64Array::from(amounts)) as ArrayRef,
            Arc::new(Float64Array::from(discounts)) as ArrayRef,
            Arc::new(Int32Array::from(quantities)) as ArrayRef,
            Arc::new(BooleanArray::from(is_mobile)) as ArrayRef,
            Arc::new(BooleanArray::from(is_priority)) as ArrayRef,
            Arc::new(StringArray::from(skus)) as ArrayRef,
            Arc::new(StringArray::from(channels)) as ArrayRef,
            Arc::new(StringArray::from(notes)) as ArrayRef,
        ],
    )
    .expect("S3 performance fixture batch should be valid")
}

#[derive(Clone, Copy, Debug)]
struct PartitionSpec {
    event_date_days: i32,
    event_date: &'static str,
    region: &'static str,
}

fn partition_for_file(file_index: usize, phase: BatchPhase) -> PartitionSpec {
    const ACTIVE_DATES: [&str; 3] = ["2026-01-01", "2026-01-02", "2026-01-03"];
    const ACTIVE_REGIONS: [&str; 4] = ["us-east", "us-west", "eu-west", "ap-south"];
    const INITIAL_DATES: [&str; 2] = ["2025-12-18", "2025-12-19"];
    const INITIAL_REGIONS: [&str; 2] = ["legacy-east", "legacy-west"];
    match phase {
        BatchPhase::Initial => {
            let date_index = file_index % INITIAL_DATES.len();
            PartitionSpec {
                event_date_days: BASE_EVENT_DATE_DAYS - 14 + i32::try_from(file_index).unwrap_or(0),
                event_date: INITIAL_DATES[date_index],
                region: INITIAL_REGIONS[file_index % INITIAL_REGIONS.len()],
            }
        }
        BatchPhase::Active => {
            let date_index = file_index / ACTIVE_REGIONS.len();
            PartitionSpec {
                event_date_days: BASE_EVENT_DATE_DAYS + i32::try_from(date_index).unwrap_or(0),
                event_date: ACTIVE_DATES[date_index % ACTIVE_DATES.len()],
                region: ACTIVE_REGIONS[file_index % ACTIVE_REGIONS.len()],
            }
        }
    }
}

fn status_for(skew: u32, row_index: usize) -> &'static str {
    if row_index % 97 == 0 {
        "refunded"
    } else if skew < 46 {
        "paid"
    } else if skew < 68 {
        "shipped"
    } else if skew < 86 {
        "pending"
    } else {
        "failed"
    }
}

fn amount_for(rng: &mut DeterministicRng, status: &str, row_index: usize) -> f64 {
    let base = match status {
        "paid" => 110.0,
        "shipped" => 145.0,
        "pending" => 45.0,
        "failed" => 15.0,
        _ => 70.0,
    };
    let skew = if row_index % 251 == 0 { 450.0 } else { 0.0 };
    base + skew + f64::from(rng.next_u32() % 20_000) / 100.0
}

fn channel_for(value: u32) -> &'static str {
    match value % 10 {
        0 | 1 | 2 | 3 => "web",
        4 | 5 | 6 => "mobile",
        7 | 8 => "partner",
        _ => "store",
    }
}

#[derive(Debug)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.state >> 32) as u32
    }
}

fn table_columns() -> Vec<StructField> {
    vec![
        field("event_id", PrimitiveType::Long, false),
        field("event_ts", PrimitiveType::Timestamp, false),
        field("event_date", PrimitiveType::String, false),
        field("region", PrimitiveType::String, false),
        field("customer_id", PrimitiveType::String, false),
        field("session_id", PrimitiveType::String, false),
        field("status", PrimitiveType::String, false),
        field("amount", PrimitiveType::Double, false),
        field("discount", PrimitiveType::Double, true),
        field("quantity", PrimitiveType::Integer, false),
        field("is_mobile", PrimitiveType::Boolean, false),
        field("is_priority", PrimitiveType::Boolean, true),
        field("sku", PrimitiveType::String, false),
        field("channel", PrimitiveType::String, false),
        field("notes", PrimitiveType::String, true),
    ]
}

fn field(name: &str, data_type: PrimitiveType, nullable: bool) -> StructField {
    StructField::new(name.to_string(), DataType::Primitive(data_type), nullable)
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("event_id", ArrowDataType::Int64, false),
        Field::new(
            "event_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_date", ArrowDataType::Utf8, false),
        Field::new("region", ArrowDataType::Utf8, false),
        Field::new("customer_id", ArrowDataType::Utf8, false),
        Field::new("session_id", ArrowDataType::Utf8, false),
        Field::new("status", ArrowDataType::Utf8, false),
        Field::new("amount", ArrowDataType::Float64, false),
        Field::new("discount", ArrowDataType::Float64, true),
        Field::new("quantity", ArrowDataType::Int32, false),
        Field::new("is_mobile", ArrowDataType::Boolean, false),
        Field::new("is_priority", ArrowDataType::Boolean, true),
        Field::new("sku", ArrowDataType::Utf8, false),
        Field::new("channel", ArrowDataType::Utf8, false),
        Field::new("notes", ArrowDataType::Utf8, true),
    ])
}

fn active_relative_paths(
    table: &DeltaTable,
    table_root: &Path,
) -> Result<BTreeSet<String>, Box<dyn Error>> {
    let mut paths = BTreeSet::new();
    for uri in table.get_file_uris()? {
        let path = uri.strip_prefix("file://").unwrap_or(&uri);
        let relative = Path::new(path)
            .strip_prefix(table_root)?
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        paths.insert(relative);
    }
    Ok(paths)
}

fn collect_active_data_files(
    output_root: &Path,
    table_root: &Path,
    active_relative_paths: &BTreeSet<String>,
) -> Result<Vec<DataFileInventory>, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(table_root, &mut files)?;
    files.sort();
    files
        .into_iter()
        .filter_map(|path| {
            let relative_path = relative_to(table_root, &path).ok()?;
            if !active_relative_paths.contains(&relative_path) {
                return None;
            }
            Some((path, relative_path))
        })
        .map(|(path, relative_path)| {
            let parquet = parquet_inventory(&path)?;
            Ok(DataFileInventory {
                url_path: public_url_path(output_root, &path)?,
                size_bytes: fs::metadata(&path)?.len(),
                row_count: parquet.row_count,
                row_group_count: parquet.row_group_count,
                compression: parquet.compression,
                partition_values: partition_values_from_path(&relative_path),
                relative_path,
            })
        })
        .collect()
}

struct ParquetInventory {
    row_count: i64,
    row_group_count: usize,
    compression: String,
}

fn parquet_inventory(path: &Path) -> Result<ParquetInventory, Box<dyn Error>> {
    let file = fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let compression = if metadata.num_row_groups() > 0 && metadata.row_group(0).num_columns() > 0 {
        format!("{:?}", metadata.row_group(0).column(0).compression())
    } else {
        "UNKNOWN".to_string()
    };
    Ok(ParquetInventory {
        row_count: metadata.file_metadata().num_rows(),
        row_group_count: metadata.num_row_groups(),
        compression,
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

fn sanitize_delta_commit_info_locations(
    table_root: &Path,
    table_uri: &str,
) -> Result<(), Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(&table_root.join("_delta_log"), &mut files)?;
    files.sort();

    for path in files {
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }

        let text = fs::read_to_string(&path)?;
        let mut changed = false;
        let mut output = String::with_capacity(text.len());
        for line in text.lines() {
            if line.trim().is_empty() {
                output.push_str(line);
            } else {
                let mut value: serde_json::Value = serde_json::from_str(line)?;
                if sanitize_commit_info_location_value(&mut value, table_uri) {
                    changed = true;
                    output.push_str(&serde_json::to_string(&value)?);
                } else {
                    output.push_str(line);
                }
            }
            output.push('\n');
        }

        if changed {
            fs::write(path, output)?;
        }
    }

    Ok(())
}

fn sanitize_commit_info_location_value(value: &mut serde_json::Value, table_uri: &str) -> bool {
    let Some(location_value) = value.pointer_mut("/commitInfo/operationParameters/location") else {
        return false;
    };
    let Some(location) = location_value.as_str() else {
        return false;
    };
    if !location.starts_with("file:") {
        return false;
    }

    *location_value = serde_json::Value::String(table_uri.to_string());
    true
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
        "/fixtures/s3-perf/{}",
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

fn derive_rows_per_row_group(
    target_active_bytes: usize,
    active_file_count: usize,
    row_groups_per_file: usize,
) -> usize {
    let row_groups = active_file_count.max(1) * row_groups_per_file.max(1);
    let rows = (target_active_bytes / row_groups / ESTIMATED_COMPRESSED_BYTES_PER_ROW).max(256);
    round_up(rows, 1024)
}

fn round_up(value: usize, multiple: usize) -> usize {
    value.div_ceil(multiple) * multiple
}

fn env_usize(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().parse()?),
        Ok(_) | Err(_) => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().parse()?),
        Ok(_) | Err(_) => Ok(default),
    }
}

impl S3UploadConfig {
    fn from_env() -> Result<UploadGate, Box<dyn Error>> {
        let bucket = env::var("AXON_S3_PERF_FIXTURE_BUCKET").ok();
        let prefix = env::var("AXON_S3_PERF_FIXTURE_PREFIX").ok();
        let region = env::var("AXON_LIVE_PUBLIC_S3_REGION").ok();
        let mut missing = Vec::new();
        if bucket.as_ref().is_none_or(|value| value.trim().is_empty()) {
            missing.push("AXON_S3_PERF_FIXTURE_BUCKET");
        }
        if prefix.as_ref().is_none_or(|value| value.trim().is_empty()) {
            missing.push("AXON_S3_PERF_FIXTURE_PREFIX");
        }
        if region.as_ref().is_none_or(|value| value.trim().is_empty()) {
            missing.push("AXON_LIVE_PUBLIC_S3_REGION");
        }
        if !missing.is_empty() {
            return Ok(UploadGate::MissingEnv(missing));
        }

        let config = Self {
            bucket: normalize_bucket(&bucket.expect("checked bucket env"))?,
            prefix: normalize_prefix(&prefix.expect("checked prefix env"))?,
            region: normalize_region(&region.expect("checked region env"))?,
        };
        Ok(UploadGate::Ready(config))
    }

    fn table_uri(&self) -> String {
        format!("s3://{}/{}/table", self.bucket, self.prefix)
    }

    fn target_uri(&self) -> String {
        format!("s3://{}/{}/table", self.bucket, self.prefix)
    }

    fn table_root_url(&self) -> String {
        format!(
            "https://{}.s3.{}.amazonaws.com/{}/table/",
            self.bucket, self.region, self.prefix
        )
    }
}

fn normalize_bucket(value: &str) -> Result<String, Box<dyn Error>> {
    let bucket = value.trim().to_lowercase();
    if contains_secret_material(&bucket) {
        return Err("S3 fixture bucket must not contain credential material".into());
    }
    if !bucket
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        || bucket.starts_with('-')
        || bucket.ends_with('-')
        || !(3..=63).contains(&bucket.len())
    {
        return Err("S3 fixture bucket must be a DNS-compatible bucket name without dots".into());
    }
    Ok(bucket)
}

fn normalize_prefix(value: &str) -> Result<String, Box<dyn Error>> {
    let prefix = value.trim().trim_matches('/').to_string();
    if prefix.is_empty() {
        return Err("S3 fixture prefix must not be empty".into());
    }
    if contains_secret_material(&prefix)
        || prefix
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(
            "S3 fixture prefix must be a plain object prefix without secret material".into(),
        );
    }
    Ok(prefix)
}

fn normalize_region(value: &str) -> Result<String, Box<dyn Error>> {
    let region = value.trim().to_lowercase();
    if !region
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        || !region.contains('-')
    {
        return Err("AXON_LIVE_PUBLIC_S3_REGION must be an AWS region identifier".into());
    }
    Ok(region)
}

fn contains_secret_material(value: &str) -> bool {
    let lower = value.to_lowercase();
    looks_like_aws_access_key(value)
        || lower.contains("x-amz-signature")
        || lower.contains("x-amz-credential")
        || lower.contains("x-amz-security-token")
        || lower.contains("aws_access_key_id")
        || lower.contains("aws_secret_access_key")
        || lower.contains("aws_session_token")
        || lower.contains("access_token")
        || lower.contains("bearer")
}

fn looks_like_aws_access_key(value: &str) -> bool {
    value.as_bytes().windows(20).any(|window| {
        window.starts_with(b"AKIA")
            && window
                .iter()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    })
}

fn write_s3_access_artifacts(output_root: &Path, gate: &UploadGate) -> Result<(), Box<dyn Error>> {
    let (bucket, prefix, region, table_uri, root_url) = match gate {
        UploadGate::Ready(upload) => (
            upload.bucket.as_str(),
            upload.prefix.as_str(),
            upload.region.as_str(),
            upload.table_uri(),
            upload.table_root_url(),
        ),
        UploadGate::MissingEnv(_) => (
            "<bucket>",
            "<prefix>",
            "<region>",
            "s3://<bucket>/<prefix>/table".to_string(),
            "https://<bucket>.s3.<region>.amazonaws.com/<prefix>/table/".to_string(),
        ),
    };
    fs::write(
        output_root.join("s3-public-read-policy.json"),
        serde_json::to_vec_pretty(&s3_public_policy(bucket, prefix))?,
    )?;
    fs::write(
        output_root.join("s3-cors.json"),
        serde_json::to_vec_pretty(&s3_cors())?,
    )?;
    fs::write(
        output_root.join("S3_ACCESS.md"),
        s3_access_markdown(bucket, prefix, region, &table_uri, &root_url),
    )?;
    Ok(())
}

fn s3_public_policy(bucket: &str, prefix: &str) -> serde_json::Value {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadFixtureObjects",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/{prefix}/*")
            },
            {
                "Sid": "PublicListFixturePrefix",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": format!("arn:aws:s3:::{bucket}"),
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            format!("{prefix}/*")
                        ]
                    }
                }
            }
        ]
    })
}

fn s3_cors() -> serde_json::Value {
    serde_json::json!({
        "CORSRules": [
            {
                "AllowedHeaders": [
                    "Range",
                    "If-Range",
                    "Content-Type",
                    "If-Match",
                    "If-None-Match",
                    "If-Modified-Since",
                    "If-Unmodified-Since"
                ],
                "AllowedMethods": ["GET", "HEAD"],
                "AllowedOrigins": ["*"],
                "ExposeHeaders": [
                    "Accept-Ranges",
                    "Content-Length",
                    "Content-Range",
                    "ETag",
                    "Last-Modified",
                    "x-amz-request-id",
                    "x-amz-id-2"
                ],
                "MaxAgeSeconds": 3600
            }
        ]
    })
}

fn s3_access_markdown(
    bucket: &str,
    prefix: &str,
    region: &str,
    table_uri: &str,
    root_url: &str,
) -> String {
    let bucket_origin = format!("https://{bucket}.s3.{region}.amazonaws.com");
    format!(
        r#"# S3 Performance Fixture Access

Table URI:

```text
{table_uri}
```

The generator wrote `s3-perf-fixture-manifest.json`,
`s3-perf-object-sha256.txt`, and `s3-perf-provenance.json` beside this file.
Validate those files before promotion:

```bash
AXON_S3_PERF_METADATA_ROOT=<generated-output-root> \
  bash scripts/verify-s3-perf-fixture.sh --metadata-only
```

Apply public-read policy and CORS:

```bash
aws s3api put-bucket-policy --bucket {bucket} --policy file://s3-public-read-policy.json
aws s3api put-bucket-cors --bucket {bucket} --cors-configuration file://s3-cors.json
```

Anonymous browser validation:

```bash
curl -sS -D /tmp/axon-s3-perf-list.headers -o /tmp/axon-s3-perf-list.xml \
  -H 'Origin: http://127.0.0.1:5173' \
  '{bucket_origin}/?list-type=2&prefix={prefix}/table/_delta_log/&max-keys=5'

curl -sS -D /tmp/axon-s3-perf-log.headers -o /tmp/axon-s3-perf-log.json \
  -H 'Origin: http://127.0.0.1:5173' \
  '{root_url}_delta_log/00000000000000000000.json'

curl -sS -D /tmp/axon-s3-perf-range.headers -o /tmp/axon-s3-perf-range.bin \
  -H 'Origin: http://127.0.0.1:5173' \
  -H 'Range: bytes=0-3' \
  '{root_url}<active-parquet-relative-path>'
```

Browser smoke:

```bash
AXON_LIVE_PUBLIC_S3_TABLE_URI={table_uri} \
AXON_LIVE_PUBLIC_S3_REGION={region} \
CI=1 npm run test:browser:public-s3-live -- --reporter=line
```
"#
    )
}

fn aws_credentials_available() -> Result<bool, Box<dyn Error>> {
    let output = Command::new("aws")
        .args(["sts", "get-caller-identity", "--output", "json"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match output {
        Ok(status) => Ok(status.success()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(Box::new(error)),
    }
}

fn upload_fixture(table_root: &Path, upload: &S3UploadConfig) -> Result<(), Box<dyn Error>> {
    let status = Command::new("aws")
        .args([
            "s3",
            "sync",
            table_root
                .to_str()
                .ok_or("fixture table path is not valid UTF-8")?,
            &upload.target_uri(),
            "--delete",
            "--only-show-errors",
        ])
        .status()?;
    if !status.success() {
        return Err(format!("aws s3 sync failed with status {status}").into());
    }
    Ok(())
}

fn report_local_generation(output_root: &Path, manifest: &FixtureManifest) {
    println!(
        "Generated local S3 performance fixture: {}",
        output_root.display()
    );
    println!("Table URI hint: {}", manifest.table_uri);
    println!(
        "Active files: {}, active bytes: {}, row groups/file target: {}, rows/row group: {}",
        manifest.active_file_count,
        manifest.actual_active_data_bytes,
        manifest.row_groups_per_file,
        manifest.rows_per_row_group
    );
    println!(
        "Manifest: {}",
        output_root.join("s3-perf-fixture-manifest.json").display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_plan_matches_performance_fixture_contract() {
        let rows_per_group = derive_rows_per_row_group(
            DEFAULT_TARGET_ACTIVE_BYTES,
            DEFAULT_ACTIVE_FILE_COUNT,
            DEFAULT_ROW_GROUPS_PER_FILE,
        );

        assert_eq!(DEFAULT_ACTIVE_FILE_COUNT, 8);
        assert_eq!(DEFAULT_ROW_GROUPS_PER_FILE, 4);
        assert!(rows_per_group >= 32 * 1024);
        assert_eq!(rows_per_group % 1024, 0);

        let config = FixtureConfig {
            target_active_bytes: DEFAULT_TARGET_ACTIVE_BYTES,
            active_file_count: DEFAULT_ACTIVE_FILE_COUNT,
            row_groups_per_file: DEFAULT_ROW_GROUPS_PER_FILE,
            rows_per_row_group: rows_per_group,
            data_page_size_bytes: DEFAULT_DATA_PAGE_SIZE_BYTES,
            data_page_row_count_limit: DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT,
            seed: 42,
        };
        config.validate().expect("default config should validate");
        assert_eq!(config.rows_per_file(), rows_per_group * 4);
    }

    #[test]
    fn s3_policy_and_cors_are_prefix_limited_and_browser_range_ready() {
        let policy = s3_public_policy("axon-public-s3-fixture-452456948477", "fixtures/perf");
        let policy_text = serde_json::to_string(&policy).expect("policy should serialize");
        assert!(policy_text.contains("s3:GetObject"));
        assert!(policy_text.contains("s3:ListBucket"));
        assert!(policy_text
            .contains("arn:aws:s3:::axon-public-s3-fixture-452456948477/fixtures/perf/*"));
        assert!(!policy_text.contains("arn:aws:s3:::axon-public-s3-fixture-452456948477/*"));

        let cors = s3_cors();
        let cors_text = serde_json::to_string(&cors).expect("cors should serialize");
        for expected in [
            "GET",
            "HEAD",
            "Range",
            "If-Range",
            "Accept-Ranges",
            "Content-Length",
            "Content-Range",
            "ETag",
            "Last-Modified",
            "x-amz-request-id",
            "x-amz-id-2",
        ] {
            assert!(cors_text.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn generated_batch_contains_materialized_perf_query_columns() {
        let config = FixtureConfig {
            target_active_bytes: 8 * 1024 * 1024,
            active_file_count: 4,
            row_groups_per_file: 2,
            rows_per_row_group: 256,
            data_page_size_bytes: DEFAULT_DATA_PAGE_SIZE_BYTES,
            data_page_row_count_limit: 1024,
            seed: 42,
        };
        let batch = batch_for_file(&config, 0, BatchPhase::Active);
        let schema = batch.schema();
        for column in [
            "event_id",
            "event_ts",
            "event_date",
            "region",
            "customer_id",
            "amount",
            "status",
        ] {
            assert!(
                schema.field_with_name(column).is_ok(),
                "missing selected query column {column}"
            );
        }
        assert_eq!(
            schema
                .field_with_name("event_date")
                .expect("event_date field should exist")
                .data_type(),
            &ArrowDataType::Utf8
        );
        assert_eq!(batch.num_rows(), 512);
    }

    #[test]
    fn output_root_is_absolute_before_delta_file_inventory() {
        let output_root = absolute_output_root(Path::new("public/fixtures/s3-perf"))
            .expect("relative output root should resolve");
        assert!(output_root.is_absolute());
        assert!(output_root.ends_with(Path::new("public/fixtures/s3-perf")));
    }

    #[test]
    fn commit_info_file_locations_are_rewritten_to_public_table_uri() {
        let mut value = serde_json::json!({
            "commitInfo": {
                "operationParameters": {
                    "location": "file:///private/tmp/axon/table"
                }
            }
        });
        assert!(sanitize_commit_info_location_value(
            &mut value,
            "s3://bucket/prefix/table"
        ));
        assert_eq!(
            value["commitInfo"]["operationParameters"]["location"],
            "s3://bucket/prefix/table"
        );

        let mut non_file = serde_json::json!({
            "commitInfo": {
                "operationParameters": {
                    "location": "s3://bucket/prefix/table"
                }
            }
        });
        assert!(!sanitize_commit_info_location_value(
            &mut non_file,
            "s3://other/table"
        ));
        assert_eq!(
            non_file["commitInfo"]["operationParameters"]["location"],
            "s3://bucket/prefix/table"
        );
    }

    #[test]
    fn fixture_provenance_uses_streaming_sha256() {
        let path = env::temp_dir().join(format!(
            "axon-s3-perf-sha256-{}-{}.txt",
            std::process::id(),
            FIXTURE_REVISION
        ));
        fs::write(&path, b"abc").expect("temporary digest fixture should write");
        let digest = sha256_file(&path).expect("temporary digest fixture should hash");
        fs::remove_file(&path).expect("temporary digest fixture should be removable");

        assert_eq!(
            digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}

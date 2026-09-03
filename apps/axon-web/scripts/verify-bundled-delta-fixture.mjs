#!/usr/bin/env node

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { join, posix, relative, resolve, sep } from 'node:path';

const FIXTURE_PATH = 'fixtures/prod-like';
const MANIFEST_PATH = `${FIXTURE_PATH}/delta-log-manifest.json`;
const URL_PREFIX = `/${FIXTURE_PATH}/table/`;
const MAX_DELTA_TABLE_BYTES = 128 * 1024;
const MAX_PAGE_INDEX_FIXTURE_BYTES = 768 * 1024;
const MAX_BUNDLED_FIXTURE_BYTES = 1024 * 1024;

const staticRoot = process.argv[2];
if (!staticRoot) {
  console.error('usage: verify-bundled-delta-fixture.mjs <static-root>');
  process.exit(2);
}

try {
  const result = verifyBundledDeltaFixture(resolve(staticRoot));
  console.log(
    `verified bundled Delta fixture: ${result.totalBytes} bytes, ${result.fileCount} files`,
  );
} catch (error) {
  console.error(`FAIL: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}

function verifyBundledDeltaFixture(root) {
  const fixtureRoot = join(root, FIXTURE_PATH);
  const manifestFile = join(root, MANIFEST_PATH);
  assertRegularFile(
    manifestFile,
    `bundled Delta fixture manifest is missing at '${MANIFEST_PATH}'`,
  );

  const manifest = parseJsonFile(manifestFile, MANIFEST_PATH);
  assert(manifest.expected_latest_version === 3, 'fixture latest Delta version must be 3');
  assert(manifest.checkpoint_version === 2, 'fixture checkpoint version must be 2');
  assert(Array.isArray(manifest.objects), 'fixture manifest objects must be an array');
  assert(Array.isArray(manifest.data_files), 'fixture manifest data_files must be an array');

  const expectedFiles = new Set(['delta-log-manifest.json']);
  const commitObjects = manifest.objects.filter((object) => object.kind === 'commit_json');
  const checkpoints = manifest.objects.filter((object) => object.kind === 'checkpoint_parquet');
  const lastCheckpoints = manifest.objects.filter((object) => object.kind === 'last_checkpoint');
  assert(commitObjects.length === 4, 'fixture must contain four Delta commit JSON files');
  assert(checkpoints.length === 1, 'fixture must contain one Delta checkpoint parquet file');
  assert(lastCheckpoints.length === 1, "fixture must contain Delta's _last_checkpoint file");
  assert(manifest.data_files.length === 5, 'fixture must contain five partitioned Parquet files');

  for (const entry of [...manifest.objects, ...manifest.data_files]) {
    verifyInventoryEntry(entry, fixtureRoot, expectedFiles);
  }

  verifyLatestSnapshotActions(manifest, fixtureRoot);
  const pageIndexBytes = verifyPageIndexFixture(fixtureRoot, expectedFiles);

  const actualFiles = listRegularFiles(fixtureRoot);
  assertSameInventory(actualFiles, expectedFiles);
  const totalBytes = actualFiles.reduce(
    (total, path) => total + statSync(join(fixtureRoot, path)).size,
    0,
  );
  const deltaTableBytes = totalBytes - pageIndexBytes;
  assert(
    deltaTableBytes <= MAX_DELTA_TABLE_BYTES,
    `bundled Delta table is ${deltaTableBytes} bytes; maximum is ${MAX_DELTA_TABLE_BYTES} bytes`,
  );
  assert(
    totalBytes <= MAX_BUNDLED_FIXTURE_BYTES,
    `bundled fixture set is ${totalBytes} bytes; maximum is ${MAX_BUNDLED_FIXTURE_BYTES} bytes`,
  );

  return { totalBytes, fileCount: actualFiles.length };
}

function verifyPageIndexFixture(fixtureRoot, expectedFiles) {
  const manifestRelativePath = 'page-index-ab/manifest.json';
  const parquetRelativePath = 'page-index-ab/event-id.parquet';
  const manifestFile = join(fixtureRoot, ...manifestRelativePath.split('/'));
  const parquetFile = join(fixtureRoot, ...parquetRelativePath.split('/'));
  expectedFiles.add(manifestRelativePath);
  expectedFiles.add(parquetRelativePath);

  assertRegularFile(
    manifestFile,
    `bundled page-index fixture manifest is missing at '${FIXTURE_PATH}/${manifestRelativePath}'`,
  );
  const manifest = parseJsonFile(manifestFile, manifestRelativePath);
  assert(manifest.schema_version === 1, 'page-index fixture schema version must be 1');
  assert(
    manifest.fixture_revision === 'local-page-index-ab-v1',
    "page-index fixture revision must be 'local-page-index-ab-v1'",
  );
  assert(
    manifest.url_path === `/${FIXTURE_PATH}/${parquetRelativePath}`,
    'page-index fixture URL must address the bundled Parquet file',
  );
  assert(manifest.row_count === 65_536, 'page-index fixture must contain 65536 rows');
  assert(manifest.row_group_count === 1, 'page-index fixture must contain one row group');
  assert(manifest.expected_pages_selected === 2, 'page-index fixture must select two pages');
  assert(manifest.expected_pages_skipped === 62, 'page-index fixture must skip 62 pages');
  assert(
    Array.isArray(manifest.data_page_extents) && manifest.data_page_extents.length === 128,
    'page-index fixture must inventory 128 data page extents',
  );

  assertRegularFile(
    parquetFile,
    `bundled page-index fixture Parquet is missing at '${FIXTURE_PATH}/${parquetRelativePath}'`,
  );
  assert(
    statSync(parquetFile).size === manifest.size_bytes,
    `fixture size mismatch for '${parquetRelativePath}'`,
  );
  verifyParquetMagic(parquetFile, parquetRelativePath);
  const sha256 = createHash('sha256').update(readFileSync(parquetFile)).digest('hex');
  assert(sha256 === manifest.sha256, `fixture checksum mismatch for '${parquetRelativePath}'`);

  const totalBytes = statSync(manifestFile).size + statSync(parquetFile).size;
  assert(
    totalBytes <= MAX_PAGE_INDEX_FIXTURE_BYTES,
    `bundled page-index fixture is ${totalBytes} bytes; maximum is ${MAX_PAGE_INDEX_FIXTURE_BYTES} bytes`,
  );
  return totalBytes;
}

function verifyInventoryEntry(entry, fixtureRoot, expectedFiles) {
  assert(entry && typeof entry === 'object', 'fixture inventory entries must be objects');
  const relativePath = safeRelativePath(entry.relative_path);
  const expectedUrl = `${URL_PREFIX}${relativePath}`;
  assert(
    entry.url_path === expectedUrl,
    `fixture URL for '${relativePath}' must be '${expectedUrl}'`,
  );
  assert(
    Number.isSafeInteger(entry.size_bytes) && entry.size_bytes > 0,
    `fixture size for '${relativePath}' must be a positive integer`,
  );

  const fixtureRelativePath = posix.join('table', relativePath);
  assert(!expectedFiles.has(fixtureRelativePath), `duplicate fixture path '${relativePath}'`);
  expectedFiles.add(fixtureRelativePath);
  const file = join(fixtureRoot, ...fixtureRelativePath.split('/'));
  assertRegularFile(file, `fixture inventory references missing file '${fixtureRelativePath}'`);
  assert(
    statSync(file).size === entry.size_bytes,
    `fixture size mismatch for '${fixtureRelativePath}'`,
  );

  if (relativePath.endsWith('.parquet')) verifyParquetMagic(file, fixtureRelativePath);
}

function verifyLatestSnapshotActions(manifest, fixtureRoot) {
  const version = String(manifest.expected_latest_version).padStart(20, '0');
  const relativePath = `_delta_log/${version}.json`;
  const finalCommit = join(fixtureRoot, 'table', '_delta_log', `${version}.json`);
  assertRegularFile(finalCommit, `fixture final commit is missing at 'table/${relativePath}'`);
  const actions = readFileSync(finalCommit, 'utf8')
    .trim()
    .split('\n')
    .map((line, index) => parseJson(line, `${relativePath} line ${index + 1}`));
  const addedPaths = actions.flatMap((action) => (action.add?.path ? [action.add.path] : []));
  const removedPaths = actions.flatMap((action) =>
    action.remove?.path ? [action.remove.path] : [],
  );
  assert(addedPaths.length === 2, 'fixture final snapshot commit must add two Parquet files');
  assert(
    removedPaths.length === 3,
    'fixture final snapshot commit must remove three Parquet files',
  );
  assert(
    addedPaths.some((path) => path.startsWith('category=B/')) &&
      addedPaths.some((path) => path.startsWith('category=D/')),
    'fixture latest snapshot must contain the B and D partitions',
  );
}

function verifyParquetMagic(file, label) {
  const bytes = readFileSync(file);
  assert(bytes.length >= 8, `Parquet fixture '${label}' is too small`);
  assert(
    bytes.subarray(0, 4).toString('ascii') === 'PAR1' &&
      bytes.subarray(-4).toString('ascii') === 'PAR1',
    `fixture '${label}' does not have Parquet magic bytes`,
  );
}

function listRegularFiles(root) {
  const files = [];
  visit(root);
  return files.sort();

  function visit(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      assert(
        !entry.isSymbolicLink(),
        `fixture must not contain symlink '${fixtureRelative(path)}'`,
      );
      if (entry.isDirectory()) visit(path);
      else {
        assert(entry.isFile(), `fixture contains non-regular entry '${fixtureRelative(path)}'`);
        files.push(fixtureRelative(path));
      }
    }
  }

  function fixtureRelative(path) {
    return relative(root, path).split(sep).join('/');
  }
}

function assertSameInventory(actualFiles, expectedFiles) {
  const expected = [...expectedFiles].sort();
  const missing = expected.filter((path) => !actualFiles.includes(path));
  const unexpected = actualFiles.filter((path) => !expectedFiles.has(path));
  assert(missing.length === 0, `fixture is missing inventoried files: ${missing.join(', ')}`);
  assert(unexpected.length === 0, `fixture has unlisted files: ${unexpected.join(', ')}`);
}

function safeRelativePath(value) {
  assert(typeof value === 'string' && value.length > 0, 'fixture relative_path must be non-empty');
  assert(!value.includes('\\'), `fixture relative_path '${value}' must use forward slashes`);
  assert(
    !posix.isAbsolute(value) && posix.normalize(value) === value && !value.startsWith('../'),
    `unsafe fixture relative_path '${value}'`,
  );
  return value;
}

function parseJsonFile(file, label) {
  return parseJson(readFileSync(file, 'utf8'), label);
}

function parseJson(value, label) {
  try {
    return JSON.parse(value);
  } catch (error) {
    throw new Error(
      `${label} is not valid JSON: ${error instanceof Error ? error.message : error}`,
      { cause: error },
    );
  }
}

function assertRegularFile(path, message) {
  try {
    assert(statSync(path).isFile(), message);
  } catch (error) {
    if (error instanceof Error && error.message === message) throw error;
    throw new Error(message, { cause: error });
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { initSync, resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';

// Runs the snapshot resolver from the compiled WASM glue, the same entry point the local-folder
// and public object-storage paths call, so the replay under test is the one Rust actually ships.

const DV_FIXTURE_ROOT = new URL(
  '../../tests/fixtures/delta-rs-table-with-deletion-logs/',
  import.meta.url,
);
const DV_CHECKPOINT = '_delta_log/00000000000000000020.checkpoint.parquet';
const DV_DATA_FILE = 'part-00000-cb251d5e-b665-437a-a9a7-fbfc5137c77d.c000.snappy.parquet';
const DV_DATA_FILE_SIZE = 10_499;
const TABLE_URI = 'browser-local://delta-table/table-with-deletion-logs';

type ResolvedSnapshot = {
  snapshot_version: number;
  active_files: Array<{ path: string; size_bytes: number }>;
  required_capabilities: { capabilities: Record<string, string> };
  browser_compatibility: { capabilities: Record<string, string> };
};

const objectUrls: string[] = [];

beforeAll(() => {
  initSync({ module: readFileSync(new URL('../wasm/axon_web_wasm_bg.wasm', import.meta.url)) });
});

afterEach(() => {
  for (const url of objectUrls.splice(0)) URL.revokeObjectURL(url);
});

describe('WASM snapshot replay of a Databricks deletion-vector checkpoint', () => {
  // The checkpoint lists the live add(p, dvB) before the same-path tombstones remove(p, no DV)
  // and remove(p, dvA). Kernel and delta-rs resolve it to one live file carrying a DV.
  it('keeps the live file that the checkpoint lists before its tombstones', async () => {
    const snapshot = await resolve({
      [DV_CHECKPOINT]: vendoredCheckpoint(),
      '_delta_log/_last_checkpoint': JSON.stringify({ version: 20, size: 5 }),
    });

    expect(snapshot.snapshot_version).toBe(20);
    expect(snapshot.active_files).toMatchObject([
      { path: DV_DATA_FILE, size_bytes: DV_DATA_FILE_SIZE },
    ]);
    expect(snapshot.required_capabilities.capabilities).toEqual({
      deletion_vectors: 'native_only',
    });
  });

  // Turning delta.enableDeletionVectors off without a purge leaves the deletion vector live, so
  // the live file is the only remaining evidence and must still require the native-only capability.
  it('keeps the deletion-vector gate after the table property is turned off', async () => {
    const snapshot = await resolve({
      [DV_CHECKPOINT]: vendoredCheckpoint(),
      '_delta_log/00000000000000000021.json': [
        {
          commitInfo: {
            timestamp: 1_690_885_090_000,
            operation: 'SET TBLPROPERTIES',
            operationParameters: { properties: '{"delta.enableDeletionVectors":"false"}' },
          },
        },
        {
          metaData: {
            id: 'bc4dc621-70d1-4392-86c0-fb1cf9d0f04a',
            format: { provider: 'parquet', options: {} },
            schemaString: JSON.stringify({
              type: 'struct',
              fields: [
                ['address', 'string'],
                ['age', 'double'],
                ['company', 'string'],
                ['id', 'long'],
                ['name', 'string'],
                ['nbr', 'long'],
                ['phone_number', 'string'],
              ].map(([name, type]) => ({ name, type, nullable: true, metadata: {} })),
            }),
            partitionColumns: [],
            configuration: { 'delta.enableDeletionVectors': 'false' },
            createdTime: 1_690_885_055_727,
          },
        },
      ]
        .map((action) => JSON.stringify(action))
        .join('\n'),
      '_delta_log/_last_checkpoint': JSON.stringify({ version: 20, size: 5 }),
    });

    expect(snapshot.snapshot_version).toBe(21);
    expect(snapshot.active_files.map((file) => file.path)).toEqual([DV_DATA_FILE]);
    expect(snapshot.required_capabilities.capabilities).toEqual({
      deletion_vectors: 'native_only',
    });
    expect(snapshot.browser_compatibility.capabilities).toEqual({
      deletion_vectors: 'native_only',
    });
  });
});

function vendoredCheckpoint(): Uint8Array<ArrayBuffer> {
  const checkpoint = new Uint8Array(readFileSync(new URL(DV_CHECKPOINT, DV_FIXTURE_ROOT)));
  const provenance = JSON.parse(
    readFileSync(new URL('provenance.json', DV_FIXTURE_ROOT), 'utf8'),
  ) as { files: Record<string, { sha256: string }> };
  expect(createHash('sha256').update(checkpoint).digest('hex')).toBe(
    provenance.files[DV_CHECKPOINT]?.sha256,
  );
  return checkpoint;
}

async function resolve(
  logObjects: Record<string, string | Uint8Array<ArrayBuffer>>,
): Promise<ResolvedSnapshot> {
  const objects = Object.entries(logObjects).map(([relativePath, body]) => {
    const blob = new Blob([body]);
    const url = URL.createObjectURL(blob);
    objectUrls.push(url);
    return { relative_path: relativePath, url, size_bytes: blob.size };
  });
  return JSON.parse(
    await resolve_delta_snapshot_from_manifest(JSON.stringify({ objects }), TABLE_URI, undefined),
  ) as ResolvedSnapshot;
}

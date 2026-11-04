import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { IDBFactory } from 'fake-indexeddb';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// Drives the shipping local-folder entry point through the real snapshot resolver in the
// compiled WASM glue, so the capability report under test is the one Rust actually emits.
vi.mock('../wasm/axon_web_wasm.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../wasm/axon_web_wasm.js')>();
  const { readFileSync: readWasm } = await import('node:fs');
  actual.initSync({
    module: readWasm(new URL('../wasm/axon_web_wasm_bg.wasm', import.meta.url)),
  });
  return { ...actual, default: vi.fn(async () => actual) };
});

import { openLocalDeltaTableFromFileList, releaseLocalDeltaObjectUrls } from './local-delta.ts';

const DV_FIXTURE_ROOT = new URL(
  '../../tests/fixtures/delta-rs-table-with-deletion-logs/',
  import.meta.url,
);
const DV_CHECKPOINT = '_delta_log/00000000000000000020.checkpoint.parquet';
const DV_DATA_FILE = 'part-00000-cb251d5e-b665-437a-a9a7-fbfc5137c77d.c000.snappy.parquet';
const DV_DATA_FILE_SIZE = 10_499;
const V2_CHECKPOINT_UUID = '3a0d65cd-4b8d-4c5e-9a3e-2b1f4b6f0c11';

const DV_UNSUPPORTED = {
  name: 'LocalDeltaError',
  code: 'unsupported_delta_feature',
  message:
    'Selected local Delta table uses deletion vectors, which this browser runtime cannot apply.',
};

class MemoryStorage implements Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> {
  private readonly records = new Map<string, string>();

  getItem(key: string): string | null {
    return this.records.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.records.set(key, value);
  }

  removeItem(key: string): void {
    this.records.delete(key);
  }
}

beforeEach(() => {
  Object.defineProperty(globalThis, 'indexedDB', { configurable: true, value: new IDBFactory() });
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: new MemoryStorage(),
  });
});

afterEach(() => {
  releaseLocalDeltaObjectUrls();
});

describe('local Delta folders with checkpoint-only capability evidence', () => {
  it('rejects a Databricks deletion-vector checkpoint whose JSON commits were cleaned up', async () => {
    const checkpoint = new Uint8Array(readFileSync(new URL(DV_CHECKPOINT, DV_FIXTURE_ROOT)));
    const provenance = JSON.parse(
      readFileSync(new URL('provenance.json', DV_FIXTURE_ROOT), 'utf8'),
    ) as { files: Record<string, { sha256: string }> };
    expect(createHash('sha256').update(checkpoint).digest('hex')).toBe(
      provenance.files[DV_CHECKPOINT]?.sha256,
    );

    await expect(
      openLocalDeltaTableFromFileList(
        [
          tableFile(DV_CHECKPOINT, checkpoint),
          tableFile('_delta_log/_last_checkpoint', JSON.stringify({ version: 20, size: 5 })),
          tableFile(DV_DATA_FILE, new Uint8Array(DV_DATA_FILE_SIZE)),
        ],
        { registryId: 'checkpoint-only-dv-parquet' },
      ),
    ).rejects.toMatchObject(DV_UNSUPPORTED);
  });

  it('rejects a V2 checkpoint that carries a live deletion-vector add without a tombstone', async () => {
    const protocol = {
      protocol: {
        minReaderVersion: 3,
        minWriterVersion: 7,
        readerFeatures: ['deletionVectors', 'v2Checkpoint'],
        writerFeatures: ['deletionVectors', 'v2Checkpoint'],
      },
    };
    const metaData = tableMetadata({
      'delta.enableDeletionVectors': 'true',
      'delta.checkpointPolicy': 'v2',
    });
    const add = {
      add: {
        path: DV_DATA_FILE,
        partitionValues: {},
        size: DV_DATA_FILE_SIZE,
        modificationTime: 1_690_885_056_000,
        dataChange: false,
        stats: JSON.stringify({ numRecords: 100 }),
        deletionVector: {
          storageType: 'u',
          pathOrInlineDv: 'Q6Kt3y1b)0MgZSWwPunr',
          offset: 1,
          sizeInBytes: 36,
          cardinality: 2,
        },
      },
    };

    await expect(
      openLocalDeltaTableFromFileList(
        [
          tableFile(
            `_delta_log/00000000000000000020.checkpoint.${V2_CHECKPOINT_UUID}.json`,
            jsonLines({ checkpointMetadata: { version: 20 } }, protocol, metaData, add),
          ),
          tableFile('_delta_log/00000000000000000020.json', jsonLines(commitInfo('UPDATE'))),
          tableFile('_delta_log/_last_checkpoint', JSON.stringify({ version: 20, size: 4 })),
          tableFile(DV_DATA_FILE, new Uint8Array(DV_DATA_FILE_SIZE)),
        ],
        { registryId: 'checkpoint-only-dv-json' },
      ),
    ).rejects.toMatchObject(DV_UNSUPPORTED);
  });

  it('still opens a checkpoint-only table that needs no native-only capability', async () => {
    const add = {
      add: {
        path: 'part-00000.parquet',
        partitionValues: {},
        size: 7,
        modificationTime: 1_690_885_056_000,
        dataChange: false,
        stats: JSON.stringify({ numRecords: 1 }),
      },
    };

    const runtime = await openLocalDeltaTableFromFileList(
      [
        tableFile(
          `_delta_log/00000000000000000003.checkpoint.${V2_CHECKPOINT_UUID}.json`,
          jsonLines(
            { checkpointMetadata: { version: 3 } },
            { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
            tableMetadata({}),
            add,
          ),
        ),
        tableFile('_delta_log/_last_checkpoint', JSON.stringify({ version: 3, size: 4 })),
        tableFile('part-00000.parquet', new Uint8Array(7)),
      ],
      { registryId: 'checkpoint-only-plain' },
    );

    expect(runtime.descriptor.snapshotVersion).toBe(3n);
    expect(runtime.descriptor.activeFiles.map((file) => file.path)).toEqual(['part-00000.parquet']);
    expect(runtime.descriptor.requiredCapabilities?.capabilities).toEqual([]);
    expect(runtime.descriptor.browserCompatibility?.capabilities).toEqual([]);
  });
});

describe('local Delta folders with JSON-commit capability evidence', () => {
  it('rejects a legacy-protocol change-data-feed table that the commit scan lets through', async () => {
    // Writer version 4 enables change data feed through table configuration alone, with no
    // protocol feature list for the JSON-commit scan to reject. The resolver reports it as
    // native_only, and local folders have no native engine to fall back to.
    const add = {
      add: {
        path: 'part-00000.parquet',
        partitionValues: {},
        size: 7,
        modificationTime: 1_690_885_056_000,
        dataChange: true,
        stats: JSON.stringify({ numRecords: 1 }),
      },
    };

    await expect(
      openLocalDeltaTableFromFileList(
        [
          tableFile(
            '_delta_log/00000000000000000000.json',
            jsonLines(
              commitInfo('CREATE TABLE'),
              { protocol: { minReaderVersion: 1, minWriterVersion: 4 } },
              tableMetadata({ 'delta.enableChangeDataFeed': 'true' }),
              add,
            ),
          ),
          tableFile('part-00000.parquet', new Uint8Array(7)),
        ],
        { registryId: 'json-only-change-data-feed' },
      ),
    ).rejects.toMatchObject({
      name: 'LocalDeltaError',
      code: 'unsupported_delta_feature',
      message: 'Selected local Delta table requires unsupported features: change_data_feed.',
    });
  });
});

function tableMetadata(configuration: Record<string, string>) {
  return {
    metaData: {
      id: 'bc4dc621-70d1-4392-86c0-fb1cf9d0f04a',
      format: { provider: 'parquet', options: {} },
      schemaString: JSON.stringify({
        type: 'struct',
        fields: [{ name: 'id', type: 'long', nullable: true, metadata: {} }],
      }),
      partitionColumns: [],
      configuration,
      createdTime: 1_690_885_055_727,
    },
  };
}

function commitInfo(operation: string) {
  return { commitInfo: { timestamp: 1_690_885_085_712, operation } };
}

function jsonLines(...actions: unknown[]): string {
  return actions.map((action) => JSON.stringify(action)).join('\n');
}

function tableFile(relativePath: string, body: string | Uint8Array<ArrayBuffer>): File {
  const file = new File([body], relativePath.split('/').at(-1) ?? relativePath, {
    lastModified: 1_797_000_000_000,
  });
  Object.defineProperty(file, 'webkitRelativePath', {
    configurable: true,
    value: `dv-table/${relativePath}`,
  });
  return file;
}

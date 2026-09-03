import { afterEach, describe, expect, it, vi } from 'vitest';
import { clearQueryRuntimeState, publishQueryRuntimeState } from './query-runtime-state.ts';
import type { QueryTableSource } from './query-source.ts';
import { loadCommits } from './snapshot.ts';
import type { CommitEntry, Catalog } from './types.ts';

const source: QueryTableSource = {
  kind: 'local_delta',
  catalogName: 'uploaded',
  schemaName: 'default',
  tableName: 'events',
  localRegistryId: 'local-events',
  storage: 'browser-local://delta-table/events',
  region: 'browser-local',
  snapshot: 41,
};

const commit: CommitEntry = {
  v: 41,
  ts: '2027-01-01 00:00:41Z',
  op: 'WRITE',
  author: 'delta-test',
  adds: 2,
  removes: 3,
  current: true,
  note: '2 add / 3 remove',
};

const catalog: Catalog = {
  name: 'uploaded',
  region: 'browser-local',
  storage: 'browser-local://delta-table/events',
  tables: [],
};

afterEach(() => {
  clearQueryRuntimeState();
  vi.unstubAllGlobals();
});

describe('snapshot commit loading', () => {
  it('returns typed commit history published for a local Delta runtime', async () => {
    publishQueryRuntimeState({ source, catalog, commits: [commit] }, 1);

    await expect(loadCommits(source)).resolves.toEqual([commit]);
  });

  it('preserves manifest-backed commit loading through the shared rollup', async () => {
    const manifestSource: QueryTableSource = {
      kind: 'manifest',
      catalogName: 'sample',
      schemaName: 'default',
      tableName: 'events',
      manifestUrl: '/manifest.json',
      storage: 'fixture',
      region: 'browser-local',
    };
    vi.stubGlobal('window', { location: { href: 'https://axon.test/' } });
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        ok: true,
        text: async () =>
          [
            JSON.stringify({
              commitInfo: {
                timestamp: Date.parse('2027-01-01T00:00:03Z'),
                operation: 'WRITE',
                engineInfo: 'delta-test',
              },
            }),
            JSON.stringify({ add: { path: 'part.parquet', size: 7 } }),
          ].join('\n'),
      })),
    );
    publishQueryRuntimeState(
      {
        source: manifestSource,
        catalog,
        manifest: {
          objects: [
            {
              relative_path: '_delta_log/00000000000000000003.json',
              url_path: '/00000000000000000003.json',
              kind: 'commit_json',
            },
          ],
        },
      },
      1,
    );

    await expect(loadCommits(manifestSource)).resolves.toEqual([
      {
        v: 3,
        ts: '2027-01-01 00:00:03Z',
        op: 'WRITE',
        author: 'delta-test',
        adds: 1,
        removes: 0,
        current: true,
        note: '1 add / 0 remove',
      },
    ]);
  });
});

import { describe, expect, it } from 'vitest';
import { buildDeltaCommitHistory } from './delta-commit-history.ts';

describe('Delta commit history', () => {
  it('rolls available JSON actions up newest-first at the resolved snapshot', () => {
    const history = buildDeltaCommitHistory(
      [
        {
          relativePath: '_delta_log/00000000000000000000.json',
          text: JSON.stringify({ metaData: {} }),
        },
        {
          relativePath: '_delta_log/00000000000000000002.json',
          text: [
            JSON.stringify({
              commitInfo: {
                timestamp: Date.parse('2027-01-01T00:00:02Z'),
                operation: 'WRITE',
                userName: 'local-user',
              },
            }),
            JSON.stringify({ remove: { path: 'old.parquet' } }),
            JSON.stringify({ add: { path: 'new.parquet', size: 7 } }),
          ].join('\n'),
        },
        {
          relativePath: '_delta_log/00000000000000000003.json',
          text: JSON.stringify({ add: { path: 'future.parquet', size: 9 } }),
        },
        {
          relativePath: '_delta_log/00000000000000000002.checkpoint.parquet',
          text: 'not JSON commit data',
        },
      ],
      2,
    );

    expect(history).toEqual([
      {
        v: 2,
        ts: '2027-01-01 00:00:02Z',
        op: 'WRITE',
        author: 'local-user',
        adds: 1,
        removes: 1,
        current: true,
        note: '1 add / 1 remove',
      },
      {
        v: 0,
        ts: '—',
        op: 'CREATE TABLE',
        author: 'unknown',
        adds: 0,
        removes: 0,
        current: false,
        note: 'commit',
      },
    ]);
  });
});

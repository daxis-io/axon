// Delta commit-log connector. Manifest sources fetch commit JSON on demand;
// local sources publish already-parsed entries with their resolved runtime.

import { buildDeltaCommitHistory, deltaCommitVersion } from './delta-commit-history.ts';
import { localDeltaCommitHistory } from './local-delta.ts';
import type { QueryTableSource } from './query-source.ts';
import { getQueryRuntimeState } from './query-runtime-state.ts';
import type { CommitEntry } from './types.ts';

export async function loadCommits(source: QueryTableSource): Promise<CommitEntry[]> {
  const state = getQueryRuntimeState(source);
  if (source.kind === 'local_delta') {
    return state?.commits ?? localDeltaCommitHistory(source.localRegistryId, source.snapshot) ?? [];
  }
  if (source.kind !== 'manifest') return [];
  if (!state?.manifest) return [];
  const baseHref = window.location.href;
  const commitLogs: Array<{ relativePath: string; text: string }> = [];

  const commitObjects = state.manifest.objects
    .filter((obj) => (obj.kind ?? classify(obj.relative_path)) === 'commit_json')
    .map((obj) => ({ ...obj, version: deltaCommitVersion(obj.relative_path) }))
    .filter((obj): obj is typeof obj & { version: number } => obj.version !== undefined)
    .sort((left, right) => right.version - left.version);

  const latestVersion = commitObjects[0]?.version;

  for (const obj of commitObjects) {
    const url = new URL(obj.url_path, baseHref).toString();
    let text: string;
    try {
      const res = await fetch(url);
      if (!res.ok) continue;
      text = await res.text();
    } catch {
      continue;
    }
    commitLogs.push({ relativePath: obj.relative_path, text });
  }

  return buildDeltaCommitHistory(commitLogs, latestVersion);
}

function classify(path: string): 'commit_json' | 'checkpoint_parquet' | 'last_checkpoint' {
  if (path === '_delta_log/_last_checkpoint') return 'last_checkpoint';
  if (path.endsWith('.checkpoint.parquet')) return 'checkpoint_parquet';
  return 'commit_json';
}

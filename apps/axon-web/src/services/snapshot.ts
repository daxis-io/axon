// Delta commit-log connector. Fetches each commit JSON from the fixture manifest,
// parses the action stream, and rolls each version up into a CommitEntry the
// Snapshot tab can render. Lifted from main.ts's parser (originally rendered as
// flat action rows in the sandbox; here we aggregate per-commit).

import { getSession, subscribeSession } from './query.ts';
import { SAMPLE_QUERY_SOURCE, sameQuerySource, type QueryTableSource } from './query-source.ts';
import type { CommitEntry, CommitOp } from './types.ts';

type ParsedAction =
  | { kind: 'add'; path: string; size?: number }
  | { kind: 'remove'; path: string }
  | { kind: 'protocol'; minReader?: number; minWriter?: number; features?: string[] }
  | { kind: 'metaData' }
  | {
      kind: 'commitInfo';
      timestamp?: number;
      operation?: string;
      author?: string;
      operationMetrics?: Record<string, string>;
    }
  | { kind: 'other' };

const KNOWN_OPS: ReadonlyArray<CommitOp> = ['MERGE', 'WRITE', 'DELETE', 'OPTIMIZE', 'CREATE TABLE'];

export async function loadCommits(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Promise<CommitEntry[]> {
  if (source.kind !== 'manifest') return [];

  const state = await getSession(source);
  if (!state.manifest) return [];
  const baseHref = window.location.href;
  const commits: CommitEntry[] = [];

  // Sort commit_json objects by version so the timeline is monotonically ordered.
  const commitObjects = state.manifest.objects
    .filter((obj) => (obj.kind ?? classify(obj.relative_path)) === 'commit_json')
    .map((obj) => ({ ...obj, version: versionFromPath(obj.relative_path) }))
    .filter((obj) => obj.version != null)
    .sort((a, b) => (b.version ?? 0) - (a.version ?? 0)); // newest first for UI

  const latestVersion = Math.max(...commitObjects.map((o) => o.version ?? 0));

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
    const actions = parseCommitText(text);
    commits.push(rollup(obj.version!, actions, obj.version === latestVersion));
  }

  return commits;
}

export function subscribeCommits(
  listener: (commits: CommitEntry[]) => void,
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): () => void {
  if (source.kind !== 'manifest') {
    listener([]);
    return () => {};
  }

  return subscribeSession((state) => {
    if (!sameQuerySource(state.source, source)) return;
    loadCommits(source)
      .then(listener)
      .catch((err) => {
        console.error('failed to load commits:', err);
      });
  });
}

function parseCommitText(text: string): ParsedAction[] {
  const out: ParsedAction[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line) continue;
    let action: Record<string, unknown>;
    try {
      action = JSON.parse(line) as Record<string, unknown>;
    } catch {
      continue;
    }
    if (isObject(action.add)) {
      out.push({
        kind: 'add',
        path: stringOr(action.add.path, ''),
        size: numberOr(action.add.size, undefined),
      });
    } else if (isObject(action.remove)) {
      out.push({ kind: 'remove', path: stringOr(action.remove.path, '') });
    } else if (isObject(action.protocol)) {
      out.push({
        kind: 'protocol',
        minReader: numberOr(action.protocol.minReaderVersion, undefined),
        minWriter: numberOr(action.protocol.minWriterVersion, undefined),
        features: stringArray(action.protocol.readerFeatures),
      });
    } else if (isObject(action.metaData)) {
      out.push({ kind: 'metaData' });
    } else if (isObject(action.commitInfo)) {
      out.push({
        kind: 'commitInfo',
        timestamp: numberOr(action.commitInfo.timestamp, undefined),
        operation: stringOr(action.commitInfo.operation, undefined),
        author:
          stringOr(action.commitInfo.userName, undefined) ??
          stringOr(action.commitInfo.engineInfo, undefined),
        operationMetrics: isObject(action.commitInfo.operationMetrics)
          ? (action.commitInfo.operationMetrics as Record<string, string>)
          : undefined,
      });
    } else {
      out.push({ kind: 'other' });
    }
  }
  return out;
}

function rollup(version: number, actions: ParsedAction[], current: boolean): CommitEntry {
  const commitInfo = actions.find(
    (a): a is Extract<ParsedAction, { kind: 'commitInfo' }> => a.kind === 'commitInfo',
  );
  const adds = actions.filter((a) => a.kind === 'add').length;
  const removes = actions.filter((a) => a.kind === 'remove').length;
  const hasMeta = actions.some((a) => a.kind === 'metaData');

  return {
    v: version,
    ts: commitInfo?.timestamp
      ? new Date(commitInfo.timestamp).toISOString().replace('T', ' ').replace('.000Z', 'Z')
      : '—',
    op: normalizeOp(commitInfo?.operation, { adds, removes, hasMeta }),
    author: commitInfo?.author ?? 'unknown',
    adds,
    removes,
    current,
    note: buildNote(commitInfo, adds, removes),
  };
}

function normalizeOp(
  raw: string | undefined,
  hints: { adds: number; removes: number; hasMeta: boolean },
): CommitOp {
  if (raw) {
    const upper = raw.toUpperCase();
    for (const op of KNOWN_OPS) if (upper === op) return op;
    if (upper.includes('MERGE')) return 'MERGE';
    if (upper.includes('DELETE')) return 'DELETE';
    if (upper.includes('OPTIMIZE')) return 'OPTIMIZE';
    if (upper.includes('CREATE')) return 'CREATE TABLE';
    if (upper.includes('WRITE') || upper.includes('APPEND')) return 'WRITE';
  }
  // Without commitInfo, infer coarsely from action mix.
  if (hints.hasMeta && hints.adds === 0 && hints.removes === 0) return 'CREATE TABLE';
  if (hints.removes > 0 && hints.adds === 0) return 'DELETE';
  if (hints.adds > 0 && hints.removes > 0) return 'MERGE';
  if (hints.adds > 0) return 'WRITE';
  return 'UNKNOWN';
}

function buildNote(
  commitInfo: Extract<ParsedAction, { kind: 'commitInfo' }> | undefined,
  adds: number,
  removes: number,
): string {
  const metrics = commitInfo?.operationMetrics ?? {};
  const rowsAdded = metrics.numOutputRows ?? metrics.numTargetRowsInserted;
  if (rowsAdded) return `${commitInfo?.operation ?? 'op'} · ${rowsAdded} rows`;
  if (adds > 0 || removes > 0) return `${adds} add / ${removes} remove`;
  return commitInfo?.operation ?? 'commit';
}

function classify(path: string): 'commit_json' | 'checkpoint_parquet' | 'last_checkpoint' {
  if (path === '_delta_log/_last_checkpoint') return 'last_checkpoint';
  if (path.endsWith('.checkpoint.parquet')) return 'checkpoint_parquet';
  return 'commit_json';
}

function versionFromPath(path: string): number | null {
  const m = /_delta_log\/(\d{20})\.json$/.exec(path);
  return m ? Number.parseInt(m[1], 10) : null;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function stringOr<T extends string | undefined>(value: unknown, fallback: T): string | T {
  return typeof value === 'string' ? value : fallback;
}

function numberOr<T extends number | undefined>(value: unknown, fallback: T): number | T {
  return typeof value === 'number' ? value : fallback;
}

function stringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const out: string[] = [];
  for (const v of value) if (typeof v === 'string') out.push(v);
  return out;
}

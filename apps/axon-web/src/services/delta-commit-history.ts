import type { CommitEntry, CommitOp } from './types.ts';

export type DeltaCommitLogText = Readonly<{
  relativePath: string;
  text: string;
}>;

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

export function buildDeltaCommitHistory(
  logs: readonly DeltaCommitLogText[],
  resolvedSnapshotVersion?: number,
): CommitEntry[] {
  const versioned = logs
    .map((log) => ({ ...log, version: deltaCommitVersion(log.relativePath) }))
    .filter(
      (log): log is DeltaCommitLogText & { version: number } =>
        log.version !== undefined &&
        (resolvedSnapshotVersion === undefined || log.version <= resolvedSnapshotVersion),
    )
    .sort((left, right) => right.version - left.version);
  const currentVersion = resolvedSnapshotVersion ?? versioned[0]?.version;

  return versioned.map((log) =>
    rollup(log.version, parseCommitText(log.text), log.version === currentVersion),
  );
}

export function deltaCommitVersion(path: string): number | undefined {
  const match = /_delta_log\/(\d{20})\.json$/.exec(path);
  if (!match) return undefined;
  const version = Number.parseInt(match[1], 10);
  return Number.isSafeInteger(version) ? version : undefined;
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
    (action): action is Extract<ParsedAction, { kind: 'commitInfo' }> =>
      action.kind === 'commitInfo',
  );
  const adds = actions.filter((action) => action.kind === 'add').length;
  const removes = actions.filter((action) => action.kind === 'remove').length;
  const hasMeta = actions.some((action) => action.kind === 'metaData');

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
  for (const item of value) if (typeof item === 'string') out.push(item);
  return out;
}

import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export type BrowserRuntimeBuildManifest = {
  schema_version: 1;
  tier: 'external-memory';
  browser_external_memory: true;
  source_commit: string | null;
  source_dirty: boolean;
};

type SourceProvenance = { sourceCommit: string | null; sourceDirty: boolean };

export function browserRuntimeBuildManifest(
  provenance: SourceProvenance = resolvedSourceProvenance(),
): BrowserRuntimeBuildManifest {
  return {
    schema_version: 1,
    tier: 'external-memory',
    browser_external_memory: true,
    source_commit: provenance.sourceCommit,
    source_dirty: provenance.sourceDirty,
  };
}

export function verifyBrowserRuntimeBuildManifest(
  value: unknown,
): asserts value is BrowserRuntimeBuildManifest {
  const expected = browserRuntimeBuildManifest();
  if (
    typeof value !== 'object' ||
    value === null ||
    !('schema_version' in value) ||
    value.schema_version !== expected.schema_version ||
    !('tier' in value) ||
    value.tier !== expected.tier ||
    !('browser_external_memory' in value) ||
    value.browser_external_memory !== expected.browser_external_memory ||
    !('source_commit' in value) ||
    (value.source_commit !== null &&
      (typeof value.source_commit !== 'string' || !/^[0-9a-f]{40}$/.test(value.source_commit))) ||
    !('source_dirty' in value) ||
    typeof value.source_dirty !== 'boolean'
  ) {
    throw new Error('browser runtime build manifest did not match the spill-capable artifact');
  }
}

export function verifyQualificationRuntimeBuildManifest(
  value: unknown,
  expectedCommit: string,
): asserts value is BrowserRuntimeBuildManifest {
  verifyBrowserRuntimeBuildManifest(value);
  const manifest = value as BrowserRuntimeBuildManifest;
  if (manifest.source_commit !== expectedCommit) {
    throw new Error(
      'browser runtime build commit did not match the requested qualification commit',
    );
  }
  if (manifest.source_dirty) {
    throw new Error('browser runtime build was dirty and cannot produce qualification evidence');
  }
}

function resolvedSourceProvenance(): SourceProvenance {
  const resolvedCommit = process.env.AXON_RUNTIME_BUILD_RESOLVED_COMMIT;
  const resolvedDirty = process.env.AXON_RUNTIME_BUILD_RESOLVED_DIRTY;
  if (resolvedCommit !== undefined || resolvedDirty !== undefined) {
    return {
      sourceCommit: resolvedCommit && /^[0-9a-f]{40}$/.test(resolvedCommit) ? resolvedCommit : null,
      sourceDirty: resolvedDirty !== 'false',
    };
  }
  const commit = git(['rev-parse', 'HEAD']);
  const status = git(['status', '--porcelain', '--untracked-files=all']);
  return {
    sourceCommit: commit && /^[0-9a-f]{40}$/.test(commit) ? commit : null,
    sourceDirty: status === null || status.length > 0,
  };
}

function git(args: string[]): string | null {
  const result = spawnSync('git', args, { encoding: 'utf8' });
  if (result.status !== 0 || result.error) return null;
  return result.stdout.trim();
}

function runBuild(): void {
  const provenance = resolvedSourceProvenance();
  const expectedCommit = process.env.AXON_RUNTIME_BUILD_SOURCE_COMMIT;
  if (expectedCommit && provenance.sourceCommit !== expectedCommit) {
    throw new Error('resolved Axon source commit did not match AXON_RUNTIME_BUILD_SOURCE_COMMIT');
  }
  if (process.env.AXON_RUNTIME_BUILD_REQUIRE_CLEAN === '1' && provenance.sourceDirty) {
    throw new Error('Axon qualification build requires a clean source checkout');
  }
  const environment = {
    ...process.env,
    AXON_RUNTIME_BUILD_RESOLVED_COMMIT: provenance.sourceCommit ?? '',
    AXON_RUNTIME_BUILD_RESOLVED_DIRTY: String(provenance.sourceDirty),
  };
  run('npm', ['run', 'build:fixture'], environment);
  run('npm', ['run', 'build:wasm'], environment);
  run('npm', ['exec', '--', 'tsc', '--noEmit'], environment);
  run('npm', ['exec', '--', 'vite', 'build'], environment);
  run('bash', ['scripts/verify-build-output.sh', 'dist'], environment);
}

function verifyBuildOutput(directory: string): void {
  let value: unknown;
  try {
    value = JSON.parse(readFileSync(resolve(directory, 'axon-runtime-build.json'), 'utf8'));
  } catch (error) {
    throw new Error(`browser runtime build manifest could not be read from '${directory}'`, {
      cause: error,
    });
  }
  verifyBrowserRuntimeBuildManifest(value);
}

function run(command: string, args: string[], environment: NodeJS.ProcessEnv): void {
  const result = spawnSync(command, args, { env: environment, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with status ${String(result.status)}`);
  }
}

function main(): void {
  const action = process.argv[2];
  if (action === 'build') {
    if (process.argv.length !== 3) {
      throw new TypeError('usage: browser-runtime-build.ts build');
    }
    runBuild();
    return;
  }
  if (action === 'verify') {
    const directory = process.argv[3];
    if (!directory || process.argv.length !== 4) {
      throw new TypeError('usage: browser-runtime-build.ts verify <directory>');
    }
    verifyBuildOutput(directory);
    return;
  }
  throw new TypeError('usage: browser-runtime-build.ts <build|verify> [directory]');
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}

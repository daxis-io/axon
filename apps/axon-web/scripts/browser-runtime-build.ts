import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export type BrowserRuntimeBuildManifest = {
  schema_version: 1;
  tier: 'external-memory';
  browser_external_memory: true;
};

export function browserRuntimeBuildManifest(): BrowserRuntimeBuildManifest {
  return {
    schema_version: 1,
    tier: 'external-memory',
    browser_external_memory: true,
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
    value.browser_external_memory !== expected.browser_external_memory
  ) {
    throw new Error('browser runtime build manifest did not match the spill-capable artifact');
  }
}

function runBuild(): void {
  const environment = process.env;
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

import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export type BrowserRuntimeBuildTier = 'standard' | 'external-memory';

export type BrowserRuntimeBuildManifest = {
  schema_version: 1;
  tier: BrowserRuntimeBuildTier;
  browser_external_memory: boolean;
};

export function resolveBrowserRuntimeBuildTier(value: string | undefined): BrowserRuntimeBuildTier {
  if (value === undefined || value === 'standard') return 'standard';
  if (value === 'external-memory') return 'external-memory';
  throw new TypeError(
    `unsupported browser runtime build tier '${value}'; expected standard or external-memory`,
  );
}

export function browserRuntimeBuildManifest(
  tier: BrowserRuntimeBuildTier,
): BrowserRuntimeBuildManifest {
  return {
    schema_version: 1,
    tier,
    browser_external_memory: tier === 'external-memory',
  };
}

export function verifyBrowserRuntimeBuildManifest(
  value: unknown,
  expectedTier: BrowserRuntimeBuildTier,
): asserts value is BrowserRuntimeBuildManifest {
  const expected = browserRuntimeBuildManifest(expectedTier);
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
    throw new Error(
      `browser runtime build manifest did not match expected ${expectedTier} artifact`,
    );
  }
}

function runBuild(tier: BrowserRuntimeBuildTier): void {
  const environment = {
    ...process.env,
    AXON_BROWSER_RUNTIME_BUILD_TIER: tier,
  };
  run('npm', ['run', 'build:fixture'], environment);
  run(
    'npm',
    ['run', tier === 'external-memory' ? 'build:wasm:external-memory' : 'build:wasm'],
    environment,
  );
  run('npm', ['exec', '--', 'tsc', '--noEmit'], environment);
  run('npm', ['exec', '--', 'vite', 'build'], environment);
  run('bash', ['scripts/verify-build-output.sh', 'dist', tier], environment);
}

function verifyBuildOutput(directory: string, tier: BrowserRuntimeBuildTier): void {
  let value: unknown;
  try {
    value = JSON.parse(readFileSync(resolve(directory, 'axon-runtime-build.json'), 'utf8'));
  } catch (error) {
    throw new Error(`browser runtime build manifest could not be read from '${directory}'`, {
      cause: error,
    });
  }
  verifyBrowserRuntimeBuildManifest(value, tier);
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
    const tier = resolveBrowserRuntimeBuildTier(
      process.argv[3] ?? process.env.AXON_BROWSER_RUNTIME_BUILD_TIER,
    );
    runBuild(tier);
    return;
  }
  if (action === 'verify') {
    const directory = process.argv[3];
    if (!directory) throw new TypeError('browser runtime build verification requires a directory');
    const tier = resolveBrowserRuntimeBuildTier(
      process.argv[4] ?? process.env.AXON_BROWSER_RUNTIME_BUILD_TIER,
    );
    verifyBuildOutput(directory, tier);
    return;
  }
  throw new TypeError('usage: browser-runtime-build.ts <build|verify> [directory] [tier]');
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}

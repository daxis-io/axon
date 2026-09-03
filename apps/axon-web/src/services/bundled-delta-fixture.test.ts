import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { afterEach, describe, expect, it } from 'vitest';

const verifier = resolve(
  import.meta.dirname,
  '..',
  '..',
  'scripts',
  'verify-bundled-delta-fixture.mjs',
);
const buildVerifier = resolve(import.meta.dirname, '..', '..', 'scripts', 'verify-build-output.sh');
const temporaryRoots: string[] = [];

afterEach(() => {
  for (const root of temporaryRoots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

describe('bundled Delta sample fixture', () => {
  it('verifies committed bytes during builds and regenerates only on explicit request', () => {
    const packageJson = JSON.parse(
      readFileSync(resolve(import.meta.dirname, '..', '..', 'package.json'), 'utf8'),
    ) as { scripts: Record<string, string> };

    expect(packageJson.scripts['build:fixture']).toBe(
      'node scripts/verify-bundled-delta-fixture.mjs public',
    );
    expect(packageJson.scripts['regenerate:fixture']).toContain('generate-prod-fixture');
  });

  it('ships a complete real Delta table within the static asset budget', () => {
    const result = runVerifier(resolve(import.meta.dirname, '..', '..', 'public'));

    expect(result, result.stderr).toMatchObject({
      status: 0,
      stderr: '',
    });
    expect(result.stdout).toMatch(/verified bundled Delta fixture: .* bytes, 14 files/);
  });

  it('reports a missing fixture as a fixture contract failure', () => {
    const emptyStaticRoot = mkdtempSync(resolve(tmpdir(), 'axon-empty-static-'));
    temporaryRoots.push(emptyStaticRoot);

    const result = runVerifier(emptyStaticRoot);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain(
      "bundled Delta fixture manifest is missing at 'fixtures/prod-like/delta-log-manifest.json'",
    );
  });

  it('rejects a production build that omitted the bundled Delta table', () => {
    const buildRoot = mkdtempSync(resolve(tmpdir(), 'axon-build-without-fixture-'));
    temporaryRoots.push(buildRoot);
    const assets = resolve(buildRoot, 'assets');
    mkdirSync(assets);
    writeFileSync(
      resolve(assets, 'sandbox-query-worker-test.js'),
      'new URL("sandbox-query-child-worker-test.js", import.meta.url);',
    );
    writeFileSync(resolve(assets, 'sandbox-query-child-worker-test.js'), 'export {};');
    writeFileSync(resolve(assets, 'axon-test.wasm'), 'wasm');

    const result = spawnSync('bash', [buildVerifier, buildRoot], { encoding: 'utf8' });

    expect(result.status).toBe(1);
    expect(result.stderr).toContain(
      "bundled Delta fixture manifest is missing at 'fixtures/prod-like/delta-log-manifest.json'",
    );
  });
});

function runVerifier(staticRoot: string): {
  status: number | null;
  stdout: string;
  stderr: string;
} {
  const result = spawnSync(process.execPath, [verifier, staticRoot], {
    encoding: 'utf8',
  });
  return {
    status: result.status,
    stdout: result.stdout,
    stderr: result.stderr,
  };
}

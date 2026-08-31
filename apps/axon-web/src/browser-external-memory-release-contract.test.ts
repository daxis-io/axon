import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const editorSmokeSource = readFileSync(
  fileURLToPath(new URL('../tests/editor-smoke.spec.ts', import.meta.url)),
  'utf8',
);
const nativeOracleSource = readFileSync(
  fileURLToPath(
    new URL(
      '../../../crates/native-query-runtime/examples/generate_stress_aggregate_oracle.rs',
      import.meta.url,
    ),
  ),
  'utf8',
);
const ciWorkflowSource = readFileSync(
  fileURLToPath(new URL('../../../.github/workflows/ci.yml', import.meta.url)),
  'utf8',
);
const pageIndexFixtureTestSource = readFileSync(
  fileURLToPath(new URL('../scripts/verify-page-index-v2-fixture.test.sh', import.meta.url)),
  'utf8',
);
const packageJson = JSON.parse(
  readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8'),
) as { scripts: Record<string, string> };
const deployWorkflowSource = readFileSync(
  fileURLToPath(new URL('../../../.github/workflows/deploy-axon-web.yml', import.meta.url)),
  'utf8',
);
const deploymentVerifierSource = readFileSync(
  fileURLToPath(new URL('../scripts/verify-deployment.sh', import.meta.url)),
  'utf8',
);
const webCargoSource = readFileSync(
  fileURLToPath(new URL('../Cargo.toml', import.meta.url)),
  'utf8',
);
const sessionCargoSource = readFileSync(
  fileURLToPath(new URL('../../../crates/wasm-datafusion-session/Cargo.toml', import.meta.url)),
  'utf8',
);
const datafusionCargoSource = readFileSync(
  fileURLToPath(new URL('../../../crates/wasm-datafusion-poc/Cargo.toml', import.meta.url)),
  'utf8',
);
const queryServiceSource = readFileSync(
  fileURLToPath(new URL('./services/query.ts', import.meta.url)),
  'utf8',
);
const queryWorkerSource = readFileSync(
  fileURLToPath(new URL('./sandbox-query-worker.ts', import.meta.url)),
  'utf8',
);
const queryChildWorkerSource = readFileSync(
  fileURLToPath(new URL('./sandbox-query-child-worker.ts', import.meta.url)),
  'utf8',
);
const memoryPolicySource = readFileSync(
  fileURLToPath(new URL('./browser-datafusion-memory-policy.ts', import.meta.url)),
  'utf8',
);

describe('browser external-memory release contract', () => {
  it('builds the single spill-capable artifact before browser unit tests', () => {
    const artifactJob = ciWorkflowSource.slice(
      ciWorkflowSource.indexOf('  browser-external-memory-artifact:'),
      ciWorkflowSource.indexOf('  browser-datafusion-wasm-size:'),
    );
    const buildIndex = artifactJob.indexOf(
      '- name: Build and verify the spill-capable browser artifact',
    );
    const testIndex = artifactJob.indexOf('- name: Run browser unit tests');

    expect(buildIndex).toBeGreaterThanOrEqual(0);
    expect(testIndex).toBeGreaterThan(buildIndex);
    expect(artifactJob.match(/npm run build:wasm(?::external-memory)?/g)).toBeNull();
    expect(artifactJob).toContain('run: npm run build');
  });

  it('keeps the page-index fixture gate independent of uninstalled ripgrep', () => {
    expect(pageIndexFixtureTestSource).not.toMatch(/\brg\b/);
    expect(pageIndexFixtureTestSource).toContain('grep -Fq');
  });

  it('matches literal atomic API names in the documentation gate', () => {
    expect(ciWorkflowSource).toContain(
      "rg -Uq 'accepted browser failure never transparently becomes[[:space:]]+native execution'",
    );
    expect(ciWorkflowSource).toContain(
      "rg -Fq 'Existing `sql()` and its `single_buffer` / `chunked_buffers` delivery modes remain atomic'",
    );
    expect(ciWorkflowSource).toContain("rg -Fq '`sqlProgressive()` is a separate API'");
  });

  it('exposes only the normal build commands for the fixed runtime', () => {
    expect(packageJson.scripts.build).toBe(
      'node --experimental-strip-types scripts/browser-runtime-build.ts build',
    );
    expect(packageJson.scripts['build:vercel']).toBe(packageJson.scripts.build);
    expect(packageJson.scripts['build:wasm']).not.toContain('--features');
    expect(packageJson.scripts).not.toHaveProperty('build:external-memory');
    expect(packageJson.scripts).not.toHaveProperty('build:wasm:external-memory');
  });

  it('has no public build-tier selector or production prohibition', () => {
    expect(deployWorkflowSource).not.toContain('browser_runtime_tier');
    expect(deployWorkflowSource).not.toContain('AXON_BROWSER_RUNTIME_BUILD_TIER');
    expect(deployWorkflowSource).not.toContain('preview-canary only');
  });

  it('allows the production alias time to publish newly hashed assets', () => {
    expect(deploymentVerifierSource).toContain('VERIFY_DEPLOYMENT_ATTEMPTS');
    expect(deploymentVerifierSource).toContain('VERIFY_DEPLOYMENT_RETRY_DELAY_SECONDS');
    expect(deploymentVerifierSource).toContain('retrying');
  });

  it('has no legacy OPFS canary cap override in live runtime source', () => {
    const liveRuntimeSources = [
      queryServiceSource,
      queryWorkerSource,
      queryChildWorkerSource,
      memoryPolicySource,
    ];
    for (const source of liveRuntimeSources) {
      expect(source).not.toContain('axon_datafusion_spill_cap_mib');
      expect(source).not.toContain('datafusion_spill_cap_mib');
      expect(source).not.toContain('browserExternalMemoryCanaryCapBytes');
    }
    expect(queryChildWorkerSource).toContain(
      'productionCapBytes: BROWSER_SPILL_PRODUCTION_CAP_BYTES',
    );
  });

  it('has no Cargo feature selector for browser external memory', () => {
    for (const cargoSource of [webCargoSource, sessionCargoSource, datafusionCargoSource]) {
      expect(cargoSource).not.toMatch(/^browser-external-memory\s*=/m);
    }
  });

  it('uses the same ordered SQL source for browser execution and the native oracle', () => {
    expect(editorSmokeSource).toContain('STRESS_AGGREGATE_SQL');
    expect(nativeOracleSource).toContain('stress-aggregate.sql');
    const sql = readFileSync(
      fileURLToPath(
        new URL('../tests/fixtures/browser-external-memory/stress-aggregate.sql', import.meta.url),
      ),
      'utf8',
    );
    expect(sql).toMatch(/GROUP BY event_id\s+ORDER BY event_id\s*$/i);
  });
});

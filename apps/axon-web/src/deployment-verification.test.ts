import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { afterAll, describe, expect, it } from 'vitest';

const temporaryRoot = mkdtempSync(join(tmpdir(), 'axon-deployment-verification-'));
const dist = join(temporaryRoot, 'dist');
const bin = join(temporaryRoot, 'bin');
const manifest = '{"source_commit":"qualified-commit","source_dirty":false}';
mkdirSync(join(dist, 'assets'), { recursive: true });
mkdirSync(bin);
for (const name of [
  'sandbox-query-worker-test.js',
  'sandbox-query-child-worker-test.js',
  'test.wasm',
]) {
  writeFileSync(join(dist, 'assets', name), 'fixture');
}
writeFileSync(join(dist, 'axon-runtime-build.json'), manifest);
writeFileSync(
  join(bin, 'curl'),
  `#!/usr/bin/env bash
set -euo pipefail
mode="\${AXON_TEST_CURL_MODE}"
url="\${!#}"
if [[ "$*" == *'%{redirect_url}'* ]]; then
  if [[ "$mode" == protected || ( "$mode" == bypass && "$*" != *'x-vercel-protection-bypass: test-bypass'* ) ]]; then
    printf '%s' 'https://vercel.com/sso-api'
  fi
  exit 0
fi
if [[ "$url" == */axon-runtime-build.json && "$*" != *'%{http_code}'* ]]; then
  if [[ "$mode" == transient-manifest && ! -e "$AXON_TEST_MANIFEST_STATE" ]]; then
    touch "$AXON_TEST_MANIFEST_STATE"
    printf '%s' '{}'
  elif [[ "$mode" == manifest-mismatch ]]; then printf '%s' '{}'; else printf '%s' '${manifest}'; fi
elif [[ "$url" == */assets/deliberately-missing-asset.js ]]; then
  if [[ "$mode" == missing-asset-html ]]; then printf '%s' '200 text/html'; else printf '%s' '404 text/plain'; fi
elif [[ "$url" == *.wasm ]]; then
  printf '%s' '200 application/wasm'
elif [[ "$url" == *.js ]]; then
  if [[ "$mode" == wrong-mime ]]; then printf '%s' '200 text/html'; else printf '%s' '200 application/javascript'; fi
elif [[ "$url" == */axon-runtime-build.json ]]; then
  printf '%s' '200 application/json'
else
  printf '%s' '200 text/html'
fi
`,
  { mode: 0o755 },
);

afterAll(() => rmSync(temporaryRoot, { recursive: true, force: true }));

function verify(mode: string, bypass = '', attempts = 1) {
  return spawnSync(
    'bash',
    [
      resolve(import.meta.dirname, '../scripts/verify-deployment.sh'),
      'https://staged.example',
      dist,
    ],
    {
      encoding: 'utf8',
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH}`,
        AXON_TEST_CURL_MODE: mode,
        AXON_TEST_MANIFEST_STATE: join(temporaryRoot, 'manifest-state'),
        VERCEL_AUTOMATION_BYPASS_SECRET: bypass,
        VERIFY_DEPLOYMENT_ATTEMPTS: String(attempts),
        VERIFY_DEPLOYMENT_RETRY_DELAY_SECONDS: '0',
      },
    },
  );
}

describe('deployed artifact verification', () => {
  it('accepts accessible assets and the exact qualified runtime manifest', () => {
    expect(verify('accessible').status).toBe(0);
  });

  it.each(['', 'rejected-bypass'])('fails blocked verification with bypass %j', (bypass) => {
    const result = verify('protected', bypass);
    expect(result.status).not.toBe(0);
    expect(result.stdout + result.stderr).toContain('VERCEL_AUTOMATION_BYPASS_SECRET');
  });

  it('passes the configured bypass header to deployment requests', () => {
    expect(verify('bypass', 'test-bypass').status).toBe(0);
  });

  it('rejects a runtime manifest from another build', () => {
    expect(verify('manifest-mismatch', '', 2).status).not.toBe(0);
  });

  it('retries a stale edge manifest until it matches the qualified build', () => {
    expect(verify('transient-manifest', '', 2).status).toBe(0);
  });

  it.each(['wrong-mime', 'missing-asset-html'])('rejects %s', (mode) => {
    expect(verify(mode).status).not.toBe(0);
  });
});

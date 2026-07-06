import { spawnSync } from 'node:child_process';

for (const script of ['scripts/check-config-codegen.mjs', 'scripts/check-contract-codegen.mjs']) {
  const result = spawnSync(process.execPath, [script], { stdio: 'inherit' });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

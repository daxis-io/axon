import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { GENERATED_CONFIG_DIR, runConfigCodegen } from './config-codegen.mjs';

const tempRoot = mkdtempSync(join(tmpdir(), 'axon-config-codegen-'));
const actualDir = GENERATED_CONFIG_DIR;
const generatedDir = join(tempRoot, actualDir);

try {
  runConfigCodegen({ output: tempRoot });

  if (!existsSync(actualDir)) {
    console.error(`Missing checked-in generated config directory: ${actualDir}`);
    process.exit(1);
  }

  const diff = spawnSync('diff', ['-ru', actualDir, generatedDir], { stdio: 'inherit' });
  if (diff.error) {
    throw diff.error;
  }
  if (diff.status !== 0) {
    console.error('Config codegen output is stale. Run npm run codegen:config.');
    process.exitCode = diff.status ?? 1;
  }
} finally {
  rmSync(tempRoot, { recursive: true, force: true });
}

import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { GENERATED_RUST_CONTRACTS_DIR, runRustContractsCodegen } from './contract-codegen.mjs';

const tempRoot = mkdtempSync(join(tmpdir(), 'axon-rust-contract-codegen-'));
const actualDir = GENERATED_RUST_CONTRACTS_DIR;

try {
  runRustContractsCodegen({ output: tempRoot });

  if (!existsSync(actualDir)) {
    console.error(`Missing checked-in generated Rust contracts directory: ${actualDir}`);
    process.exitCode = 1;
  } else {
    const diff = spawnSync('diff', ['-ru', actualDir, tempRoot], { stdio: 'inherit' });
    if (diff.error) {
      throw diff.error;
    }
    if (diff.status !== 0) {
      console.error('Rust contract codegen output is stale. Run npm run codegen:contracts:rust.');
      process.exitCode = diff.status ?? 1;
    }
  }
} finally {
  rmSync(tempRoot, { recursive: true, force: true });
}

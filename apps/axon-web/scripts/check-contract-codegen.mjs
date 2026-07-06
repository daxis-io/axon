import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  GENERATED_CONTRACTS_DIR,
  listContractProtoFiles,
  runContractsCodegen,
} from './contract-codegen.mjs';

const tempRoot = mkdtempSync(join(tmpdir(), 'axon-contract-codegen-'));
const actualDir = GENERATED_CONTRACTS_DIR;
const generatedDir = join(tempRoot, actualDir);

try {
  const protoFiles = listContractProtoFiles();

  if (protoFiles.length === 0) {
    if (existsSync(actualDir)) {
      console.error(
        `Found checked-in generated contracts without contract proto files: ${actualDir}`,
      );
      process.exitCode = 1;
    }
  } else {
    runContractsCodegen({ output: tempRoot });

    if (!existsSync(actualDir)) {
      console.error(`Missing checked-in generated contracts directory: ${actualDir}`);
      process.exitCode = 1;
    } else {
      const diff = spawnSync('diff', ['-ru', actualDir, generatedDir], { stdio: 'inherit' });
      if (diff.error) {
        throw diff.error;
      }
      if (diff.status !== 0) {
        console.error('Contract codegen output is stale. Run npm run codegen:contracts.');
        process.exitCode = diff.status ?? 1;
      }
    }
  }
} finally {
  rmSync(tempRoot, { recursive: true, force: true });
}

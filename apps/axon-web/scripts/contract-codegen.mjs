import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { normalizeGeneratedTypescript } from './config-codegen.mjs';

export const GENERATED_CONTRACTS_DIR = 'src/generated/contracts/protobuf';
export const CONTRACT_PROTO_ROOT = 'proto/axon';

export function listContractProtoFiles(root = CONTRACT_PROTO_ROOT) {
  if (!existsSync(root)) {
    return [];
  }

  return findProtoFiles(root)
    .filter((path) => !path.startsWith('proto/axon/config/'))
    .sort();
}

export function runContractsCodegen({ output = '.' } = {}) {
  const protoFiles = listContractProtoFiles();
  if (protoFiles.length === 0) {
    return false;
  }

  execFileSync(
    'buf',
    [
      'generate',
      '--template',
      'buf.gen.contracts.yaml',
      '--output',
      output,
      ...protoFiles.flatMap((path) => ['--path', path]),
    ],
    { stdio: 'inherit' },
  );
  normalizeGeneratedTypescript(join(output, GENERATED_CONTRACTS_DIR));
  return true;
}

function findProtoFiles(root) {
  const files = [];

  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const child = join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...findProtoFiles(child));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith('.proto')) {
      files.push(child);
    }
  }

  return files;
}

import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { normalizeGeneratedTypescript } from './config-codegen.mjs';

export const GENERATED_CONTRACTS_DIR = 'src/generated/contracts/protobuf';
export const GENERATED_RUST_CONTRACTS_DIR = '../../crates/contract-proto/src/generated';
export const CONTRACT_PROTO_ROOT = 'proto/axon';
export const RUST_CONTRACT_PROTO_PATHS = [
  'proto/axon/common/v1/common.proto',
  'proto/axon/dataaccess/v1/dataaccess.proto',
  'proto/axon/exec/v1/exec.proto',
  'proto/axon/fs/v1/fs.proto',
];

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

export function runRustContractsCodegen({ output = GENERATED_RUST_CONTRACTS_DIR } = {}) {
  const missingProtoFiles = RUST_CONTRACT_PROTO_PATHS.filter((path) => !existsSync(path));
  if (missingProtoFiles.length > 0) {
    throw new Error(`Missing Rust contract proto files: ${missingProtoFiles.join(', ')}`);
  }

  execFileSync(
    'buf',
    [
      'generate',
      '--template',
      'buf.gen.contracts.rust.yaml',
      '--output',
      output,
      ...RUST_CONTRACT_PROTO_PATHS.flatMap((path) => ['--path', path]),
    ],
    { stdio: 'inherit' },
  );
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

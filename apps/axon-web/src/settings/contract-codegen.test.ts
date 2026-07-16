import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const appRoot = fileURLToPath(new URL('../../', import.meta.url));
const rustCodegenTempPrefix = 'axon-rust-contract-codegen-';
const rustContractProtoPaths = [
  'proto/axon/common/v1/common.proto',
  'proto/axon/dataaccess/v1/dataaccess.proto',
  'proto/axon/exec/v1/exec.proto',
  'proto/axon/fs/v1/fs.proto',
];

function listRustCodegenTempDirs() {
  return new Set(readdirSync(tmpdir()).filter((name) => name.startsWith(rustCodegenTempPrefix)));
}

describe('contract codegen', () => {
  it('pins Buffa generation and includes Rust drift in the aggregate gate', () => {
    const packageJson = JSON.parse(readFileSync(join(appRoot, 'package.json'), 'utf8')) as {
      scripts?: Record<string, string>;
    };
    const templatePath = join(appRoot, 'buf.gen.contracts.rust.yaml');

    expect(packageJson.scripts?.['codegen:contracts:rust']).toBe(
      'node scripts/generate-rust-contracts.mjs',
    );
    expect(packageJson.scripts?.['codegen:contracts:rust:check']).toBe(
      'node scripts/check-rust-contract-codegen.mjs',
    );
    expect(readFileSync(join(appRoot, 'scripts/codegen-check.mjs'), 'utf8')).toContain(
      'scripts/check-rust-contract-codegen.mjs',
    );
    expect(existsSync(templatePath)).toBe(true);
    if (!existsSync(templatePath)) return;

    const template = readFileSync(templatePath, 'utf8');
    expect(template).toContain('clean: true');
    expect(template).toContain('remote: buf.build/anthropics/buffa:v0.8.0');
    expect(template).toContain('revision: 1');
  });

  it('fails on stale Rust output and cleans temporary generation', () => {
    const tempRepo = mkdtempSync(join(tmpdir(), 'axon-rust-contract-codegen-test-'));
    const tempApp = join(tempRepo, 'apps/axon-web');
    const actualDir = join(tempRepo, 'crates/contract-proto/src/generated');
    const fakeBin = join(tempRepo, 'bin');
    const fakeBuf = join(fakeBin, 'buf');
    const before = listRustCodegenTempDirs();
    let leakedCodegenDirs: string[] = [];

    try {
      mkdirSync(tempApp, { recursive: true });
      mkdirSync(actualDir, { recursive: true });
      mkdirSync(fakeBin);
      for (const path of rustContractProtoPaths) {
        const protoPath = join(tempApp, path);
        mkdirSync(dirname(protoPath), { recursive: true });
        writeFileSync(protoPath, 'syntax = "proto3";\n');
      }
      writeFileSync(join(tempApp, 'buf.gen.contracts.rust.yaml'), 'version: v2\n');
      writeFileSync(join(actualDir, 'axon.common.v1.common.rs'), 'checked-in\n');
      writeFileSync(
        fakeBuf,
        [
          '#!/bin/sh',
          'set -eu',
          'output=',
          'while [ "$#" -gt 0 ]; do',
          '  case "$1" in',
          '    --output) output="$2"; shift 2 ;;',
          '    *) shift ;;',
          '  esac',
          'done',
          'test -n "$output"',
          'mkdir -p "$output"',
          'printf "%s\\n" "$FAKE_RUST_CODEGEN_CONTENT" > "$output/axon.common.v1.common.rs"',
          '',
        ].join('\n'),
      );
      chmodSync(fakeBuf, 0o755);

      const result = spawnSync(
        process.execPath,
        [join(appRoot, 'scripts/check-rust-contract-codegen.mjs')],
        {
          cwd: tempApp,
          encoding: 'utf8',
          env: {
            ...process.env,
            FAKE_RUST_CODEGEN_CONTENT: 'freshly-generated',
            PATH: `${fakeBin}:${process.env.PATH ?? ''}`,
          },
        },
      );

      const after = listRustCodegenTempDirs();
      leakedCodegenDirs = [...after].filter((name) => !before.has(name));

      expect(result.status).toBe(1);
      expect(`${result.stdout}${result.stderr}`).toContain(
        'Rust contract codegen output is stale. Run npm run codegen:contracts:rust.',
      );
      expect(leakedCodegenDirs).toEqual([]);
    } finally {
      rmSync(tempRepo, { recursive: true, force: true });
      for (const name of leakedCodegenDirs) {
        rmSync(join(tmpdir(), name), { recursive: true, force: true });
      }
    }
  });
});

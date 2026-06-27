import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  cpSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { SettingsSchema } from '../generated/config/protobuf/axon/config/v1/settings_pb.ts';

const appRoot = fileURLToPath(new URL('../../', import.meta.url));
const configCodegenTempPrefix = 'axon-config-codegen-';

function listConfigCodegenTempDirs() {
  return new Set(readdirSync(tmpdir()).filter((name) => name.startsWith(configCodegenTempPrefix)));
}

describe('config codegen', () => {
  it('generates the settings protobuf and strict JSON Schema outputs', () => {
    const settings = create(SettingsSchema, {});

    expect(SettingsSchema.typeName).toBe('axon.config.v1.Settings');
    expect(settings.$typeName).toBe('axon.config.v1.Settings');

    const schema = JSON.parse(
      readFileSync(
        new URL(
          '../generated/config/jsonschema/axon.config.v1.Settings.jsonschema.strict.json',
          import.meta.url,
        ),
        'utf8',
      ),
    ) as { additionalProperties?: unknown; properties?: Record<string, unknown> };

    expect(schema.additionalProperties).toBe(false);
    expect(Object.keys(schema.properties ?? {})).toEqual(['appearance', 'editor', 'execution']);

    const appearanceSchema = JSON.parse(
      readFileSync(
        new URL(
          '../generated/config/jsonschema/axon.config.v1.AppearanceDefaults.jsonschema.strict.json',
          import.meta.url,
        ),
        'utf8',
      ),
    ) as { properties?: Record<string, unknown>; required?: string[] };

    expect(Object.keys(appearanceSchema.properties ?? {})).toContain('accentColor');
    expect(Object.keys(appearanceSchema.properties ?? {})).not.toContain('accent_color');
    expect(appearanceSchema.required).toContain('accentColor');
  });

  it('cleans temporary codegen output when checked-in generated output is missing', () => {
    const tempProject = mkdtempSync(join(tmpdir(), 'axon-config-codegen-test-'));
    const fakeBin = join(tempProject, 'bin');
    const fakeBuf = join(fakeBin, 'buf');
    const before = listConfigCodegenTempDirs();
    let leakedCodegenDirs: string[] = [];

    try {
      for (const path of ['buf.yaml', 'buf.gen.yaml', 'buf.lock', 'proto']) {
        cpSync(join(appRoot, path), join(tempProject, path), { recursive: true });
      }
      mkdirSync(fakeBin);
      writeFileSync(
        fakeBuf,
        [
          '#!/bin/sh',
          'set -eu',
          'if [ "$1" != "generate" ] || [ "$2" != "--output" ]; then exit 64; fi',
          'mkdir -p "$3/src/generated/config/protobuf"',
          'printf "export {};\\n" > "$3/src/generated/config/protobuf/fake_pb.ts"',
          '',
        ].join('\n'),
      );
      chmodSync(fakeBuf, 0o755);

      const result = spawnSync(
        process.execPath,
        [join(appRoot, 'scripts/check-config-codegen.mjs')],
        {
          cwd: tempProject,
          encoding: 'utf8',
          env: { ...process.env, PATH: `${fakeBin}:${process.env.PATH ?? ''}` },
        },
      );

      const after = listConfigCodegenTempDirs();
      leakedCodegenDirs = [...after].filter((name) => !before.has(name));
      for (const name of leakedCodegenDirs) {
        rmSync(join(tmpdir(), name), { recursive: true, force: true });
      }

      expect(result.status).toBe(1);
      expect(`${result.stdout}${result.stderr}`).toContain(
        'Missing checked-in generated config directory',
      );
      expect(leakedCodegenDirs).toEqual([]);
    } finally {
      rmSync(tempProject, { recursive: true, force: true });
      for (const name of leakedCodegenDirs) {
        rmSync(join(tmpdir(), name), { recursive: true, force: true });
      }
    }
  });
});

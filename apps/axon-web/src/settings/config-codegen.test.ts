import { readFileSync } from 'node:fs';
import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { SettingsSchema } from '../generated/config/protobuf/axon/config/v1/settings_pb.ts';

describe('config codegen', () => {
  it('generates the settings protobuf and strict JSON Schema outputs', () => {
    const settings = create(SettingsSchema, {});

    expect(SettingsSchema.typeName).toBe('axon.config.v1.Settings');
    expect(settings.$typeName).toBe('axon.config.v1.Settings');

    const schema = JSON.parse(
      readFileSync(
        new URL(
          '../generated/config/jsonschema/axon.config.v1.Settings.schema.strict.json',
          import.meta.url,
        ),
        'utf8',
      ),
    ) as { additionalProperties?: unknown; properties?: Record<string, unknown> };

    expect(schema.additionalProperties).toBe(false);
    expect(Object.keys(schema.properties ?? {})).toEqual(['appearance', 'editor', 'execution']);
  });
});

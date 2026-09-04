import { describe, expect, it } from 'vitest';
import {
  browserRuntimeBuildManifest,
  verifyQualificationRuntimeBuildManifest,
  verifyBrowserRuntimeBuildManifest,
} from '../scripts/browser-runtime-build.ts';

describe('browser runtime build identity', () => {
  it('emits and verifies the fixed spill-capable artifact manifest', () => {
    const manifest = browserRuntimeBuildManifest({
      sourceCommit: 'a'.repeat(40),
      sourceDirty: false,
    });

    expect(manifest).toEqual({
      schema_version: 1,
      tier: 'external-memory',
      browser_external_memory: true,
      source_commit: 'a'.repeat(40),
      source_dirty: false,
    });
    expect(() => verifyBrowserRuntimeBuildManifest(manifest)).not.toThrow();
  });

  it('rejects dirty, missing, or mismatched qualification runtime provenance', () => {
    const manifest = browserRuntimeBuildManifest({
      sourceCommit: 'a'.repeat(40),
      sourceDirty: false,
    });
    expect(() => verifyQualificationRuntimeBuildManifest(manifest, 'a'.repeat(40))).not.toThrow();
    expect(() =>
      verifyQualificationRuntimeBuildManifest({ ...manifest, source_dirty: true }, 'a'.repeat(40)),
    ).toThrow(/dirty/i);
    expect(() => verifyQualificationRuntimeBuildManifest(manifest, 'b'.repeat(40))).toThrow(
      /commit/i,
    );
    expect(() =>
      verifyQualificationRuntimeBuildManifest({ ...manifest, source_commit: null }, 'a'.repeat(40)),
    ).toThrow(/commit/i);
  });

  it('rejects malformed artifact manifests instead of trusting a marker string', () => {
    expect(() =>
      verifyBrowserRuntimeBuildManifest({
        schema_version: 1,
        tier: 'external-memory',
        browser_external_memory: false,
      }),
    ).toThrow(/manifest/i);
  });
});

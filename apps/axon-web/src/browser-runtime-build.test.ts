import { describe, expect, it } from 'vitest';
import {
  browserRuntimeBuildManifest,
  verifyBrowserRuntimeBuildManifest,
} from '../scripts/browser-runtime-build.ts';

describe('browser runtime build identity', () => {
  it('emits and verifies the fixed spill-capable artifact manifest', () => {
    const manifest = browserRuntimeBuildManifest();

    expect(manifest).toEqual({
      schema_version: 1,
      tier: 'external-memory',
      browser_external_memory: true,
    });
    expect(() => verifyBrowserRuntimeBuildManifest(manifest)).not.toThrow();
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

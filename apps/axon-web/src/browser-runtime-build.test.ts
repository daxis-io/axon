import { describe, expect, it } from 'vitest';
import {
  browserRuntimeBuildManifest,
  resolveBrowserRuntimeBuildTier,
  verifyBrowserRuntimeBuildManifest,
} from '../scripts/browser-runtime-build.ts';

describe('browser runtime build tier', () => {
  it('keeps the default production artifact on the standard runtime', () => {
    expect(resolveBrowserRuntimeBuildTier(undefined)).toBe('standard');
  });

  it('selects the external-memory runtime only when explicitly requested', () => {
    expect(resolveBrowserRuntimeBuildTier('external-memory')).toBe('external-memory');
  });

  it.each(['', 'external_memory', 'true', 'experimental'])(
    'rejects unknown explicit runtime tier %j',
    (value) => {
      expect(() => resolveBrowserRuntimeBuildTier(value)).toThrow(/browser runtime build tier/i);
    },
  );

  it('emits and verifies a feature-specific artifact manifest', () => {
    const manifest = browserRuntimeBuildManifest('external-memory');

    expect(manifest).toEqual({
      schema_version: 1,
      tier: 'external-memory',
      browser_external_memory: true,
    });
    expect(() => verifyBrowserRuntimeBuildManifest(manifest, 'external-memory')).not.toThrow();
    expect(() => verifyBrowserRuntimeBuildManifest(manifest, 'standard')).toThrow(
      /expected standard/i,
    );
  });

  it('rejects malformed artifact manifests instead of trusting a marker string', () => {
    expect(() =>
      verifyBrowserRuntimeBuildManifest(
        { schema_version: 1, tier: 'external-memory', browser_external_memory: false },
        'external-memory',
      ),
    ).toThrow(/manifest/i);
  });
});

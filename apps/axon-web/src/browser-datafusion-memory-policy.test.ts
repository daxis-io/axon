import { describe, expect, it } from 'vitest';
import {
  BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB,
  BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB,
  PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB,
  browserDataFusionMemoryOverrideBytes,
  browserExternalMemoryCanaryCapBytes,
} from './browser-datafusion-memory-policy.ts';

const MIB = 1024 * 1024;

describe('browser DataFusion interim memory policy', () => {
  it('uses only the approved measured candidates and keeps 64 MiB as the kill switch', () => {
    expect(BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB).toEqual([96, 128, 160, 192, 256]);
    expect(PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB).toBe(64);
    expect(browserDataFusionMemoryOverrideBytes('64')).toBe(64 * MIB);
    expect(browserDataFusionMemoryOverrideBytes('128')).toBe(128 * MIB);
    expect(() => browserDataFusionMemoryOverrideBytes('512')).toThrow(
      'unsupported browser DataFusion memory profile',
    );
  });
});

describe('browser external-memory canary spill cap', () => {
  it('accepts explicit 64 MiB multiples without allowing canary input to raise the hard cap', () => {
    expect(BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB).toBe(576);
    expect(browserExternalMemoryCanaryCapBytes(null)).toBeUndefined();
    expect(browserExternalMemoryCanaryCapBytes('64')).toBe(64 * MIB);
    expect(browserExternalMemoryCanaryCapBytes('256')).toBe(256 * MIB);
    expect(browserExternalMemoryCanaryCapBytes('576')).toBe(576 * MIB);
    expect(browserExternalMemoryCanaryCapBytes('640')).toBe(576 * MIB);
    expect(browserExternalMemoryCanaryCapBytes('4096')).toBe(576 * MIB);
  });

  it.each(['', 'garbage', '65', '0', '4097'])(
    'rejects an explicitly configured invalid cap %j instead of disabling spill',
    (value) => {
      expect(() => browserExternalMemoryCanaryCapBytes(value)).toThrow(
        /browser external-memory spill cap/i,
      );
    },
  );

  it('distinguishes an absent canary override from an explicit invalid value', () => {
    expect(browserExternalMemoryCanaryCapBytes(null)).toBeUndefined();
  });
});

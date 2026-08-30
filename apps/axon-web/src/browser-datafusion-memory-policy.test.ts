import { describe, expect, it } from 'vitest';
import {
  BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB,
  PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB,
  browserDataFusionMemoryOverrideBytes,
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

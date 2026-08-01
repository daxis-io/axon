import { describe, expect, it } from 'vitest';
import {
  BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB,
  BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB,
  PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB,
  browserDataFusionMemoryOverrideBytes,
  browserExternalMemoryCanaryCapBytes,
  selectBrowserDataFusionMemoryProfile,
  type BrowserDataFusionMemoryProfileObservation,
} from './browser-datafusion-memory-policy.ts';

const MIB = 1024 * 1024;
const BROWSERS = ['chromium', 'firefox', 'webkit'] as const;

function observation(
  browser: (typeof BROWSERS)[number],
  profileMiB: number,
  peakMiB: number,
  overrides: Partial<BrowserDataFusionMemoryProfileObservation> = {},
): BrowserDataFusionMemoryProfileObservation {
  return {
    browser,
    profileMiB,
    completedRuns: 10,
    peakRegisteredBytes: peakMiB * MIB,
    physicalMemoryPlateaued: true,
    ...overrides,
  };
}

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

  it('selects the lowest all-browser profile with ten runs, a plateau, and 20 percent headroom', () => {
    const observations = [
      ...BROWSERS.map((browser) =>
        observation(browser, 96, 70, {
          completedRuns: browser === 'firefox' ? 9 : 10,
        }),
      ),
      ...BROWSERS.map((browser) => observation(browser, 128, 104)),
      ...BROWSERS.map((browser) => observation(browser, 160, 120)),
      ...BROWSERS.map((browser) => observation(browser, 192, 120)),
    ];

    expect(selectBrowserDataFusionMemoryProfile(observations, BROWSERS)).toBe(160);
  });

  it('refuses to select through 256 MiB when any browser fails or does not plateau', () => {
    const observations = BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB.flatMap((profileMiB) =>
      BROWSERS.map((browser) =>
        observation(browser, profileMiB, profileMiB * 0.75, {
          physicalMemoryPlateaued: browser !== 'webkit',
        }),
      ),
    );

    expect(selectBrowserDataFusionMemoryProfile(observations, BROWSERS)).toBeUndefined();
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
    expect(browserExternalMemoryCanaryCapBytes('65')).toBeUndefined();
    expect(browserExternalMemoryCanaryCapBytes('0')).toBeUndefined();
    expect(browserExternalMemoryCanaryCapBytes('4097')).toBeUndefined();
  });
});

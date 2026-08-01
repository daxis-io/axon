const MIB = 1024 * 1024;

export const PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB = 64;
export const BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB = [96, 128, 160, 192, 256] as const;
export const BROWSER_EXTERNAL_MEMORY_SPILL_CAP_GRANULARITY_MIB = 64;
export const BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB = 576;
export const BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_BYTES =
  BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB * MIB;

const ALLOWED_OVERRIDE_MIB = new Set<number>([
  PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB,
  ...BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB,
]);

export type BrowserDataFusionMemoryProfileObservation = {
  browser: string;
  profileMiB: number;
  completedRuns: number;
  peakRegisteredBytes: number;
  physicalMemoryPlateaued: boolean;
};

export function browserDataFusionMemoryOverrideBytes(
  profileMiB: string | null | undefined,
): number | undefined {
  if (profileMiB === null || profileMiB === undefined || profileMiB.length === 0) {
    return undefined;
  }
  if (!/^[0-9]+$/.test(profileMiB)) {
    throw new TypeError('browser DataFusion memory profile must be an integer MiB value');
  }
  const parsed = Number(profileMiB);
  if (!ALLOWED_OVERRIDE_MIB.has(parsed)) {
    throw new RangeError(`unsupported browser DataFusion memory profile: ${profileMiB} MiB`);
  }
  return parsed * MIB;
}

/**
 * Parses an explicitly qualified canary spill cap.
 *
 * The canary may lower the corpus-derived product cap but can never raise it.
 */
export function browserExternalMemoryCanaryCapBytes(value: string | null): number | undefined {
  if (value === null || !/^[1-9]\d*$/.test(value)) return undefined;
  const mebibytes = Number(value);
  if (
    !Number.isSafeInteger(mebibytes) ||
    mebibytes < BROWSER_EXTERNAL_MEMORY_SPILL_CAP_GRANULARITY_MIB ||
    mebibytes > 4096 ||
    mebibytes % BROWSER_EXTERNAL_MEMORY_SPILL_CAP_GRANULARITY_MIB !== 0
  ) {
    return undefined;
  }
  return Math.min(mebibytes, BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB) * MIB;
}

export function selectBrowserDataFusionMemoryProfile(
  observations: readonly BrowserDataFusionMemoryProfileObservation[],
  qualifyingBrowsers: readonly string[],
): number | undefined {
  if (
    qualifyingBrowsers.length === 0 ||
    new Set(qualifyingBrowsers).size !== qualifyingBrowsers.length
  ) {
    throw new TypeError('qualifying browsers must be a non-empty unique list');
  }

  for (const profileMiB of BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB) {
    const limitBytes = profileMiB * MIB;
    const qualifies = qualifyingBrowsers.every((browser) => {
      const observation = observations.find(
        (candidate) => candidate.browser === browser && candidate.profileMiB === profileMiB,
      );
      return (
        observation !== undefined &&
        Number.isSafeInteger(observation.completedRuns) &&
        observation.completedRuns >= 10 &&
        Number.isSafeInteger(observation.peakRegisteredBytes) &&
        observation.peakRegisteredBytes >= 0 &&
        observation.peakRegisteredBytes * 5 <= limitBytes * 4 &&
        observation.physicalMemoryPlateaued
      );
    });
    if (qualifies) return profileMiB;
  }

  return undefined;
}

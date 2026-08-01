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
  if (value === null) return undefined;
  if (!/^[1-9]\d*$/.test(value)) {
    throw new TypeError('browser external-memory spill cap must be a positive integer MiB value');
  }
  const mebibytes = Number(value);
  if (
    !Number.isSafeInteger(mebibytes) ||
    mebibytes < BROWSER_EXTERNAL_MEMORY_SPILL_CAP_GRANULARITY_MIB ||
    mebibytes > 4096 ||
    mebibytes % BROWSER_EXTERNAL_MEMORY_SPILL_CAP_GRANULARITY_MIB !== 0
  ) {
    throw new RangeError(
      'browser external-memory spill cap must be a 64 MiB multiple between 64 and 4096 MiB',
    );
  }
  return Math.min(mebibytes, BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_MIB) * MIB;
}

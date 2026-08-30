const MIB = 1024 * 1024;

export const PREVIOUS_BROWSER_DATAFUSION_MEMORY_PROFILE_MIB = 64;
export const BROWSER_DATAFUSION_MEMORY_CANDIDATE_MIB = [96, 128, 160, 192, 256] as const;
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

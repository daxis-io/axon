export const BROWSER_MEMORY_PROFILE_QUERY_PARAM = 'browser_memory_profile_mib';
export const PREVIOUS_BROWSER_MEMORY_PROFILE_MIB = 64;
export const EXTERNAL_MEMORY_WORKING_SET_MIB = 128;

const ALLOWED_BROWSER_MEMORY_PROFILES_MIB = new Set<number>([
  PREVIOUS_BROWSER_MEMORY_PROFILE_MIB,
  EXTERNAL_MEMORY_WORKING_SET_MIB,
]);

export function parseBrowserMemoryProfileMib(searchParams: URLSearchParams): number {
  const raw = searchParams.get(BROWSER_MEMORY_PROFILE_QUERY_PARAM);
  if (raw === null) {
    return searchParams.get('browser_external_memory') === 'enabled'
      ? EXTERNAL_MEMORY_WORKING_SET_MIB
      : PREVIOUS_BROWSER_MEMORY_PROFILE_MIB;
  }
  const profile = Number(raw);
  if (!Number.isSafeInteger(profile) || !ALLOWED_BROWSER_MEMORY_PROFILES_MIB.has(profile)) {
    throw new Error(`unsupported browser memory profile '${raw}'`);
  }
  return profile;
}

export function forwardBrowserMemoryProfile(searchParams: URLSearchParams, workerUrl: URL): void {
  const profile = parseBrowserMemoryProfileMib(searchParams);
  workerUrl.searchParams.set(BROWSER_MEMORY_PROFILE_QUERY_PARAM, String(profile));
}

export function browserQueryWorkerName(searchParams: URLSearchParams): string {
  const config = new URLSearchParams();
  config.set(
    BROWSER_MEMORY_PROFILE_QUERY_PARAM,
    String(parseBrowserMemoryProfileMib(searchParams)),
  );
  const externalMemory = searchParams.get('browser_external_memory');
  if (externalMemory !== null) {
    if (externalMemory !== 'enabled' && externalMemory !== 'disabled') {
      throw new Error(`unsupported browser external-memory mode '${externalMemory}'`);
    }
    config.set('browser_external_memory', externalMemory);
  }
  return `axon-editor-query-worker?${config.toString()}`;
}

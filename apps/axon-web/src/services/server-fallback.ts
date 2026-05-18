type ViteEnv = Record<string, string | boolean | undefined>;

const env = ((import.meta as ImportMeta & { readonly env?: ViteEnv }).env ?? {}) as ViteEnv;
const rawMode = String(env.VITE_AXON_SERVER_QUERY_FALLBACK ?? '').toLowerCase();

export const SERVER_QUERY_FALLBACK_ENABLED =
  rawMode === 'server' || rawMode === 'enabled' || rawMode === 'true';

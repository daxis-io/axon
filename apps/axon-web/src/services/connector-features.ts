type ViteEnv = Record<string, string | boolean | undefined>;

export type ConnectorFeatureFlags = {
  bffAuthServiceConnectors: boolean;
};

const env = ((import.meta as ImportMeta & { readonly env?: ViteEnv }).env ?? {}) as ViteEnv;

export function connectorFeaturesFromEnv(input: ViteEnv): ConnectorFeatureFlags {
  const rawMode = String(input.VITE_AXON_BFF_AUTH_SERVICE_CONNECTORS ?? '').toLowerCase();

  return {
    bffAuthServiceConnectors: rawMode === 'enabled',
  };
}

export const CONNECTOR_FEATURES = connectorFeaturesFromEnv(env);

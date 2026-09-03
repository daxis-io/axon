import { parsePublicObjectStorageTableRoot } from './object-storage.ts';

export type PublicR2DemoPreset = Readonly<{
  tableUri: string;
  endpoint: string;
}>;

type PublicR2DemoEnvironment = Readonly<{
  VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI?: unknown;
  VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT?: unknown;
}>;

const PRODUCTION_PUBLIC_R2_ENDPOINT = 'https://data.axon.daxistech.io';

export function publicR2DemoPresetFromEnv(
  env: PublicR2DemoEnvironment,
): PublicR2DemoPreset | undefined {
  const tableUri = environmentText(env.VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI);
  const endpoint = environmentText(env.VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT);
  if (!tableUri && !endpoint) return undefined;
  if (!tableUri || !endpoint) {
    throw new Error(
      'VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI and VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT must be set together',
    );
  }

  try {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'r2',
      tableUri,
      endpoint,
    });
    if (root.provider !== 'r2') throw new Error('invalid public R2 demo build settings');
    if (root.endpoint !== PRODUCTION_PUBLIC_R2_ENDPOINT) return undefined;
    return {
      tableUri: root.tableUri,
      endpoint: root.endpoint,
    };
  } catch {
    throw new Error('invalid public R2 demo build settings');
  }
}

export const PUBLIC_R2_DEMO_PRESET = publicR2DemoPresetFromEnv(
  (import.meta as ImportMeta & { env: PublicR2DemoEnvironment }).env,
);

function environmentText(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

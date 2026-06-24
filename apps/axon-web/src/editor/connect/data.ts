// Connect-Catalog workflow data.
// Models the source picker and browser-local discovery payload.

import type { ConnectorFeatureFlags } from '../../services/connector-features.ts';
import type { PublicObjectStorageDescriptorResolutionMetrics } from '../../services/object-storage.ts';

export type SourceId = 'local' | 'object_store' | 'unity_catalog' | 'delta_share';

export type SourceCard = {
  id: SourceId;
  title: string;
  blurb: string;
  examples: string;
  owners: {
    access: string;
    snapshot: string;
    query: string;
  };
  glyph: string;
  glyphTone: 'neutral' | 'blue' | 'violet' | 'teal';
  tags: string[];
  requiresBffAuthService?: boolean;
};

export const SOURCES: SourceCard[] = [
  {
    id: 'local',
    title: 'Local files',
    blurb: 'A selected Delta table directory on this machine.',
    examples: '_delta_log path · File System Access handle',
    owners: {
      access: 'Browser',
      snapshot: 'Browser',
      query: 'Browser',
    },
    glyph: 'L',
    glyphTone: 'neutral',
    tags: ['browser-runtime', 'folder'],
  },
  {
    id: 'object_store',
    title: 'Object storage',
    blurb: 'Read public GCS Delta logs from the browser.',
    examples: 'public gs:// table root · browser range reads',
    owners: {
      access: 'Browser',
      snapshot: 'Browser',
      query: 'Browser',
    },
    glyph: 'OS',
    glyphTone: 'blue',
    tags: ['browser-local'],
  },
  {
    id: 'unity_catalog',
    title: 'Unity Catalog',
    blurb: 'Governed reads through an authenticated Axon broker.',
    examples: 'ReadAccessPlan · object grants · SQL fallback',
    owners: {
      access: 'UC brokered',
      snapshot: 'Browser',
      query: 'Browser',
    },
    glyph: 'UC',
    glyphTone: 'violet',
    tags: ['governed'],
    requiresBffAuthService: true,
  },
  {
    id: 'delta_share',
    title: 'Delta Sharing',
    blurb: 'Open-protocol shares through a trusted profile broker.',
    examples: 'file actions · provider-vended URLs',
    owners: {
      access: 'Provider brokered',
      snapshot: 'Browser materialized',
      query: 'Browser',
    },
    glyph: 'DS',
    glyphTone: 'teal',
    tags: ['brokered', 'read-only'],
    requiresBffAuthService: true,
  },
];

export type SourceAvailability = {
  enabled: boolean;
  label?: string;
  reason?: string;
};

export function availabilityForSource(
  source: SourceId | SourceCard,
  connectorFeatures: ConnectorFeatureFlags,
): SourceAvailability {
  const card =
    typeof source === 'string' ? SOURCES.find((candidate) => candidate.id === source) : source;

  if (card?.requiresBffAuthService && !connectorFeatures.bffAuthServiceConnectors) {
    return {
      enabled: false,
      label: 'Coming soon',
      reason: 'Requires the Axon auth service.',
    };
  }

  return { enabled: true };
}

export type ObjectStoreProviderId = 's3' | 'gcs' | 'abfss' | 'r2';

export type ObjectStoreProvider = {
  id: ObjectStoreProviderId;
  label: string;
  scheme: string;
  placeholder: string;
  regions: string[];
};

export const OBJECT_STORE_PROVIDERS: ObjectStoreProvider[] = [
  {
    id: 's3',
    label: 'AWS S3',
    scheme: 's3://',
    placeholder: 's3://acme-lake/silver/orders',
    regions: ['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1', 'ap-south-1'],
  },
  {
    id: 'gcs',
    label: 'Google Cloud Storage',
    scheme: 'gs://',
    placeholder: 'gs://acme-lake/silver/orders',
    regions: ['us-central1', 'us-east1', 'europe-west1', 'asia-northeast1'],
  },
  {
    id: 'abfss',
    label: 'Azure ADLS Gen2',
    scheme: 'abfss://',
    placeholder: 'abfss://lake@acme.dfs.core.windows.net/silver/orders',
    regions: ['eastus', 'westus2', 'westeurope', 'southeastasia'],
  },
  {
    id: 'r2',
    label: 'Cloudflare R2',
    scheme: 'r2://',
    placeholder: 'r2://acme-lake/silver/orders',
    regions: ['auto'],
  },
];

export type DiscoveredTable = {
  name: string;
  snapshot: number;
  rows: number;
  files: number;
  size: string;
  protocol: string;
  features?: string[];
  uri?: string;
  manifestUrl?: string;
  descriptorResolutionMetrics?: PublicObjectStorageDescriptorResolutionMetrics;
  governed?: boolean;
  shared?: boolean;
  perm?: string;
  expires?: string;
  columns?: { name: string; type: string; pk?: boolean; fk?: boolean; part?: boolean }[];
};

export type DiscoveredSchema = {
  name: string;
  tableCount: number;
  included: boolean;
  governed?: boolean;
  shared?: boolean;
  tables: DiscoveredTable[];
};

export type DiscoveryPayload = {
  summary: string;
  schemas: DiscoveredSchema[];
};

export const LOCAL_DISCOVERY: DiscoveryPayload = {
  summary: 'Detected 1 Delta table',
  schemas: [
    {
      name: 'default',
      tableCount: 1,
      included: true,
      tables: [
        {
          name: 'orders',
          snapshot: 318,
          rows: 1_284_932,
          files: 412,
          size: '38.2 GB',
          protocol: 'r2/w5',
          columns: [
            { name: 'id', type: 'long', pk: true },
            { name: 'customer_id', type: 'long', fk: true },
            { name: 'status', type: 'string' },
            { name: 'total_cents', type: 'integer' },
            { name: 'placed_at', type: 'timestamp' },
            { name: 'placed_month', type: 'string', part: true },
          ],
        },
      ],
    },
  ],
};

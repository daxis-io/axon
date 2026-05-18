// Connect-Catalog workflow data.
// Models the source picker and local-only discovery payload. Live providers
// require trusted resolver/BFF contracts before discovery can run.

export type SourceId = 'local' | 'object_store' | 'unity_catalog' | 'delta_share';

export type SourceCard = {
  id: SourceId;
  title: string;
  blurb: string;
  examples: string;
  glyph: string;
  glyphTone: 'neutral' | 'blue' | 'violet' | 'teal';
  tags: string[];
};

export const SOURCES: SourceCard[] = [
  {
    id: 'local',
    title: 'Local files',
    blurb: 'A Delta table directory on this machine.',
    examples: '~/datasets · /Volumes/data · uploaded .zip',
    glyph: 'L',
    glyphTone: 'neutral',
    tags: ['dev', 'fastest'],
  },
  {
    id: 'object_store',
    title: 'Object storage',
    blurb: 'Use a trusted resolver for AWS, GCP, Azure, or R2 tables.',
    examples: 'BrowserHttpSnapshotDescriptor · BFF-issued access',
    glyph: 'OS',
    glyphTone: 'blue',
    tags: ['resolver'],
  },
  {
    id: 'unity_catalog',
    title: 'Unity Catalog',
    blurb: 'Governed reads through an authenticated Axon broker.',
    examples: 'ReadAccessPlan · object grants · SQL fallback',
    glyph: 'UC',
    glyphTone: 'violet',
    tags: ['governed'],
  },
  {
    id: 'delta_share',
    title: 'Delta Sharing',
    blurb: 'Open-protocol shares through a trusted profile broker.',
    examples: 'BFF profile handle · provider-vended URLs',
    glyph: 'DS',
    glyphTone: 'teal',
    tags: ['brokered', 'read-only'],
  },
];

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

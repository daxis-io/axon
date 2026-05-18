// Connect-Catalog workflow data.
// Models the four sources, object-store providers, auth methods, and the
// discovery payloads returned by each source's "test connection" path.

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
    blurb: 'Connect a bucket on AWS, GCP, Azure, or R2.',
    examples: 's3://… · gs://… · abfss://… · r2://…',
    glyph: 'OS',
    glyphTone: 'blue',
    tags: ['production'],
  },
  {
    id: 'unity_catalog',
    title: 'Unity Catalog',
    blurb: 'Databricks-hosted or open-source UC server.',
    examples: 'workspace · UC-OSS · catalog scoping',
    glyph: 'UC',
    glyphTone: 'violet',
    tags: ['governed'],
  },
  {
    id: 'delta_share',
    title: 'Delta Sharing',
    blurb: 'Open protocol — a profile file from a data provider.',
    examples: 'config.share · share://server/share',
    glyph: 'DS',
    glyphTone: 'teal',
    tags: ['open', 'read-only'],
  },
];

export type ObjectStoreProviderId = 's3' | 'gcs' | 'abfss' | 'r2';

export type AuthMethod =
  | 'access_key'
  | 'iam_role'
  | 'env'
  | 'anonymous'
  | 'service_account'
  | 'workload_identity'
  | 'adc'
  | 'sas_token'
  | 'service_principal'
  | 'managed_identity';

export type ObjectStoreProvider = {
  id: ObjectStoreProviderId;
  label: string;
  scheme: string;
  auths: AuthMethod[];
  placeholder: string;
  regions: string[];
};

export const OBJECT_STORE_PROVIDERS: ObjectStoreProvider[] = [
  {
    id: 's3',
    label: 'AWS S3',
    scheme: 's3://',
    auths: ['access_key', 'iam_role', 'env', 'anonymous'],
    placeholder: 's3://acme-lake/silver/orders',
    regions: ['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1', 'ap-south-1'],
  },
  {
    id: 'gcs',
    label: 'Google Cloud Storage',
    scheme: 'gs://',
    auths: ['service_account', 'workload_identity', 'adc', 'anonymous'],
    placeholder: 'gs://acme-lake/silver/orders',
    regions: ['us-central1', 'us-east1', 'europe-west1', 'asia-northeast1'],
  },
  {
    id: 'abfss',
    label: 'Azure ADLS Gen2',
    scheme: 'abfss://',
    auths: ['sas_token', 'service_principal', 'managed_identity', 'anonymous'],
    placeholder: 'abfss://lake@acme.dfs.core.windows.net/silver/orders',
    regions: ['eastus', 'westus2', 'westeurope', 'southeastasia'],
  },
  {
    id: 'r2',
    label: 'Cloudflare R2',
    scheme: 'r2://',
    auths: ['access_key', 'anonymous'],
    placeholder: 'r2://acme-lake/silver/orders',
    regions: ['auto'],
  },
];

export type AuthLabel = { label: string; fields: string[] };
export const AUTH_LABELS: Record<AuthMethod, AuthLabel> = {
  access_key: { label: 'Access key + secret', fields: ['access_key_id', 'secret_access_key'] },
  iam_role: { label: 'IAM role (assume)', fields: ['role_arn', 'external_id?'] },
  env: { label: 'Environment / credential file', fields: [] },
  anonymous: { label: 'Public · no credentials', fields: [] },
  service_account: { label: 'Service account JSON', fields: ['sa_json'] },
  workload_identity: { label: 'Workload identity', fields: ['audience'] },
  adc: { label: 'Application Default Credentials', fields: [] },
  sas_token: { label: 'SAS token', fields: ['sas_token'] },
  service_principal: {
    label: 'Service principal',
    fields: ['tenant_id', 'client_id', 'client_secret'],
  },
  managed_identity: { label: 'Managed identity', fields: [] },
};

export type DiscoveredTable = {
  name: string;
  snapshot: number;
  rows: number;
  files: number;
  size: string;
  protocol: string;
  features?: string[];
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

export const DISCOVERED: Record<SourceId, DiscoveryPayload> = {
  local: {
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
  },
  object_store: {
    summary: 'Found 8 Delta tables across 2 prefixes',
    schemas: [
      {
        name: 'silver',
        tableCount: 5,
        included: true,
        tables: [
          {
            name: 'orders',
            snapshot: 318,
            rows: 1_284_932,
            files: 412,
            size: '38.2 GB',
            protocol: 'r2/w5',
          },
          {
            name: 'order_items',
            snapshot: 318,
            rows: 4_112_903,
            files: 184,
            size: '14.9 GB',
            protocol: 'r2/w5',
          },
          {
            name: 'customers',
            snapshot: 87,
            rows: 184_221,
            files: 24,
            size: '1.1 GB',
            protocol: 'r2/w5',
          },
          {
            name: 'shipments',
            snapshot: 204,
            rows: 1_102_441,
            files: 122,
            size: '8.3 GB',
            protocol: 'r3/w7',
            features: ['deletionVectors'],
          },
          {
            name: 'sessions',
            snapshot: 92,
            rows: 3_294_112,
            files: 802,
            size: '19.2 GB',
            protocol: 'r3/w7',
            features: ['columnMapping', 'timestampNtz'],
          },
        ],
      },
      {
        name: 'bronze',
        tableCount: 3,
        included: true,
        tables: [
          {
            name: 'events',
            snapshot: 142,
            rows: 9_248_192_310,
            files: 8421,
            size: '184 GB',
            protocol: 'r3/w7',
            features: ['columnMapping', 'timestampNtz', 'deletionVectors'],
          },
          {
            name: 'events_raw',
            snapshot: 882,
            rows: 11_204_881_002,
            files: 12842,
            size: '412 GB',
            protocol: 'r3/w7',
          },
          {
            name: 'ingest_checkpoints',
            snapshot: 12,
            rows: 18_402,
            files: 2,
            size: '412 KB',
            protocol: 'r1/w2',
          },
        ],
      },
    ],
  },
  delta_share: {
    summary: 'Resolved profile · <b>2 shares</b>, 4 schemas, 9 tables',
    schemas: [
      {
        name: 'acme_partner/silver',
        tableCount: 4,
        included: true,
        shared: true,
        tables: [
          {
            name: 'orders',
            snapshot: 318,
            rows: 1_284_932,
            files: 412,
            size: '38.2 GB',
            protocol: 'r2/w5',
            shared: true,
          },
          {
            name: 'order_items',
            snapshot: 318,
            rows: 4_112_903,
            files: 184,
            size: '14.9 GB',
            protocol: 'r2/w5',
            shared: true,
          },
          {
            name: 'customers',
            snapshot: 87,
            rows: 184_221,
            files: 24,
            size: '1.1 GB',
            protocol: 'r2/w5',
            shared: true,
          },
          {
            name: 'products',
            snapshot: 41,
            rows: 8_294,
            files: 4,
            size: '14.9 MB',
            protocol: 'r2/w5',
            shared: true,
          },
        ],
      },
      {
        name: 'acme_partner/gold',
        tableCount: 2,
        included: true,
        shared: true,
        tables: [
          {
            name: 'daily_revenue',
            snapshot: 412,
            rows: 18_402,
            files: 18,
            size: '412 MB',
            protocol: 'r3/w7',
            shared: true,
          },
          {
            name: 'weekly_revenue',
            snapshot: 92,
            rows: 2_812,
            files: 8,
            size: '88 MB',
            protocol: 'r3/w7',
            shared: true,
          },
        ],
      },
      {
        name: 'market_data/refdata',
        tableCount: 3,
        included: true,
        shared: true,
        tables: [
          {
            name: 'fx_rates',
            snapshot: 9821,
            rows: 8_204_182,
            files: 88,
            size: '212 MB',
            protocol: 'r3/w7',
            shared: true,
          },
          {
            name: 'equity_prices',
            snapshot: 8821,
            rows: 88_204_188,
            files: 412,
            size: '1.2 GB',
            protocol: 'r3/w7',
            shared: true,
          },
          {
            name: 'holidays',
            snapshot: 42,
            rows: 3_812,
            files: 2,
            size: '88 KB',
            protocol: 'r3/w7',
            shared: true,
            expires: 'in 14 days',
          },
        ],
      },
    ],
  },
  unity_catalog: {
    summary: '12 tables across 3 schemas in <b>main</b>',
    schemas: [
      {
        name: 'analytics',
        tableCount: 5,
        included: true,
        governed: true,
        tables: [
          {
            name: 'daily_revenue',
            snapshot: 1042,
            rows: 18_402,
            files: 18,
            size: '412 MB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'weekly_revenue',
            snapshot: 204,
            rows: 2_812,
            files: 8,
            size: '88 MB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'cohort_retention',
            snapshot: 89,
            rows: 84_120,
            files: 12,
            size: '118 MB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'ltv_predictions',
            snapshot: 18,
            rows: 192_000,
            files: 20,
            size: '204 MB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'marketing_spend',
            snapshot: 412,
            rows: 8_204,
            files: 4,
            size: '22 MB',
            protocol: 'r3/w7',
            governed: true,
          },
        ],
      },
      {
        name: 'raw',
        tableCount: 4,
        included: true,
        governed: true,
        tables: [
          {
            name: 'stripe_events',
            snapshot: 18402,
            rows: 412_002_148,
            files: 1842,
            size: '88 GB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'segment_events',
            snapshot: 8421,
            rows: 884_120_998,
            files: 4042,
            size: '212 GB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'salesforce_log',
            snapshot: 482,
            rows: 2_481_002,
            files: 88,
            size: '18 GB',
            protocol: 'r3/w7',
            governed: true,
          },
          {
            name: 'zendesk_log',
            snapshot: 281,
            rows: 412_881,
            files: 18,
            size: '2 GB',
            protocol: 'r3/w7',
            governed: true,
          },
        ],
      },
      {
        name: 'ml_models',
        tableCount: 3,
        included: false,
        governed: true,
        tables: [
          {
            name: 'feature_store',
            snapshot: 1842,
            rows: 2_410_000,
            files: 88,
            size: '1.2 GB',
            protocol: 'r3/w7',
            governed: true,
            perm: 'no read',
          },
          {
            name: 'training_runs',
            snapshot: 402,
            rows: 18_402,
            files: 8,
            size: '112 MB',
            protocol: 'r3/w7',
            governed: true,
            perm: 'no read',
          },
          {
            name: 'predictions_v3',
            snapshot: 12,
            rows: 412_002,
            files: 12,
            size: '188 MB',
            protocol: 'r3/w7',
            governed: true,
            perm: 'no read',
          },
        ],
      },
    ],
  },
};

export const SAMPLE_DS_PROFILE = `{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.acme.io/delta-sharing",
  "bearerToken": "••••••••••••••••••••••••••••••••",
  "expirationTime": "2026-05-31T00:00:00Z"
}`;

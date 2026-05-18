import type { AuthMethod, DiscoveryPayload, ObjectStoreProviderId, SourceId } from './data.ts';

export type SchemaSelection = 'all' | 'none' | { only: string[] } | { except: string[] };

export type ConnectForm = {
  // local
  path: string;
  detected: null | {
    name: string;
    snapshot: number;
    rowsLabel: string;
    files: number;
    size: string;
    protocol: string;
  };
  // object store
  provider: ObjectStoreProviderId;
  uri: string;
  region: string;
  endpoint: string;
  auth: AuthMethod;
  creds: Record<string, string>;
  // unity catalog
  uc_mode: 'databricks' | 'oss';
  uc_host: string;
  uc_auth: 'pat' | 'oauth';
  uc_token: string;
  uc_client_id: string;
  uc_client_secret: string;
  uc_catalog: string;
  uc_schema_filter: string;
  // delta sharing
  ds_mode: 'profile' | 'manual';
  ds_profile_name: string;
  ds_profile_json: string;
  ds_endpoint: string;
  ds_token: string;
  ds_share: string;
};

export type ConnectedCatalogSchema = {
  name: string;
  tables: {
    name: string;
    snapshot: number;
    rows: number;
    files: number;
    size: string;
    protocol: string;
    features?: string[];
  }[];
};

export type ConnectedCatalog = {
  id: string;
  alias: string;
  kind: SourceId;
  provider?: ObjectStoreProviderId;
  storage: string;
  host?: string;
  path?: string;
  region: string;
  status: 'connected';
  connectedAt: string;
  schemas: ConnectedCatalogSchema[];
};

export type ConnectResult = {
  source: SourceId;
  form: ConnectForm;
  alias: string;
  selection: Record<string, SchemaSelection>;
  discovered: DiscoveryPayload;
};

export type TestState = null | 'running' | 'ok' | 'err';

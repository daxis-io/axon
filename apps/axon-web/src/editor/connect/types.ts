import type { DiscoveryPayload, ObjectStoreProviderId, SourceId } from './data.ts';
import type { LocalDeltaPersistenceMode, LocalDeltaRuntime } from '../../services/local-delta.ts';

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
    persistenceLabel: string;
  };
  localDelta: LocalDeltaRuntime | null;
  // object store
  provider: ObjectStoreProviderId;
  uri: string;
  region: string;
  endpoint: string;
  // unity catalog
  uc_mode: 'databricks' | 'oss';
  uc_host: string;
  uc_bff_url: string;
  uc_session_label: string;
  uc_catalog: string;
  uc_schema_filter: string;
  // delta sharing
  ds_mode: 'profile' | 'manual';
  ds_profile_name: string;
  ds_endpoint: string;
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
    uri?: string;
    manifestUrl?: string;
    localRegistryId?: string;
    localPersistence?: LocalDeltaPersistenceMode;
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

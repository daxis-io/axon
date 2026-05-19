import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  AxonWorkerError,
  createDeltaSharingClient,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
  type AxonBrowserClient,
  type BrowserDeltaSource,
  type BrowserBundleManifest,
  type BrowserHttpFileDescriptor,
  type BrowserHttpSnapshotDescriptor,
  type BrowserObjectGrantDescriptor,
  type DeltaLocationResolutionMode,
  type DeltaSharingFileAction,
  type DeltaSharingReadPlan,
  type DeltaSharingResponseFormat,
  type DeltaLocationResolveResponse,
  type FallbackReason,
  type PlatformFeatures,
  type QueryRequest,
  type ResolverActualAccessMode,
  type ResolverRequestedAccessMode,
} from '../src/axon-browser-sdk';

const snapshot: BrowserHttpSnapshotDescriptor = {
  table_uri: 'gs://axon-fixtures/partitioned-table',
  snapshot_version: 7,
  partition_column_types: {},
  browser_compatibility: { capabilities: {} },
  required_capabilities: { capabilities: {} },
  active_files: [
    {
      path: 'part-000.parquet',
      url: 'https://example.invalid/part-000.parquet',
      size_bytes: 128,
      partition_values: {},
    },
  ],
};

async function sdkShapeCompiles(client: AxonBrowserClient): Promise<Uint8Array> {
  await client.openDeltaTable('events', snapshot);

  const request: QueryRequest = {
    table_uri: snapshot.table_uri,
    snapshot_version: snapshot.snapshot_version,
    sql: 'SELECT COUNT(*) AS row_count FROM events',
    preferred_target: 'browser_wasm',
    options: {
      include_explain: false,
      collect_metrics: true,
    },
  };

  const result = await client.query('events', request, { requestId: 'req-query-shape' });
  result.result.bytes satisfies Uint8Array;
  result.response.metrics.bytes_fetched satisfies number;

  // @ts-expect-error explicit QueryRequest calls only accept request lifecycle options.
  await client.query('events', request, { tableUri: 'gs://axon-fixtures/other-table' });

  const shorthandResult = await client.query('events', 'SELECT id FROM events LIMIT 10');
  shorthandResult.fallbackReason satisfies FallbackReason | undefined;

  const resolverResponse: DeltaLocationResolveResponse = {
    descriptor: snapshot,
    provider: 'gcs',
    table_uri: 'gs://axon-fixtures/partitioned-table',
    requested_snapshot_version: 7,
    resolved_snapshot_version: 7,
    requested_access_mode: 'auto',
    actual_access_mode: 'signed_url',
    expires_at_epoch_ms: Date.now() + 60_000,
  };
  resolverResponse.requested_access_mode satisfies ResolverRequestedAccessMode | undefined;
  resolverResponse.actual_access_mode satisfies ResolverActualAccessMode;
  const openedLocation = await client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    resolveDeltaLocation: async () => resolverResponse,
  });
  openedLocation.location.resolved_snapshot_version satisfies number;
  openedLocation.location.actual_access_mode satisfies ResolverActualAccessMode;

  const objectGrant: BrowserObjectGrantDescriptor = {
    grantId: 'grant-browser-readable',
    tableRootUrl: 'https://storage.example.test/tables/events',
    expiresAtEpochMs: Date.now() + 60_000,
    capabilities: {
      list: true,
      head: true,
      get: true,
      rangeGet: true,
      batchSign: true,
      proxyRange: false,
    },
  };
  const sharedFiles: DeltaSharingFileAction[] = [
    {
      path: 'part-000.parquet',
      url: 'https://sharing.example.test/files/part-000.parquet',
      size_bytes: 128,
      partition_values: {},
    },
  ];
  const browserSources: BrowserDeltaSource[] = [
    { kind: 'local_files', files: [] as File[] },
    { kind: 'http_manifest', manifestUrl: 'https://data.example.test/events/manifest.json' },
    { kind: 'cors_http_table', tableRootUrl: 'https://data.example.test/events' },
    {
      kind: 'brokered_manifest',
      manifestUrl: 'https://broker.example.test/grants/grant-browser-readable/manifest.json',
      grantId: 'grant-browser-readable',
    },
    { kind: 'brokered_object_grants', grant: objectGrant },
    { kind: 'delta_sharing_url_files', files: sharedFiles },
    { kind: 'trusted_descriptor', descriptor: snapshot },
  ];
  const browserDefault = await client.openDeltaLocation('events_from_descriptor', {
    source: browserSources[6],
  });
  browserDefault.location.resolution_mode satisfies DeltaLocationResolutionMode;
  browserDefault.location.source_kind satisfies BrowserDeltaSource['kind'];

  const explicitServerSnapshot = await client.openDeltaLocation('events_server_snapshot', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    resolveDeltaLocation: async () => resolverResponse,
  });
  explicitServerSnapshot.location.actual_access_mode satisfies ResolverActualAccessMode;

  // @ts-expect-error browser sources cannot use the explicit server snapshot mode.
  await client.openDeltaLocation('events_invalid_server_source', {
    source: { kind: 'trusted_descriptor', descriptor: snapshot },
    resolutionMode: 'server_snapshot',
  });

  const brokeredAccess = await client.openDeltaLocation('events_brokered', {
    source: { kind: 'brokered_object_grants', grant: objectGrant },
    resolutionMode: 'brokered_access',
    resolveBrowserSnapshot: async (source) => {
      source.kind satisfies BrowserDeltaSource['kind'];
      return snapshot;
    },
  });
  brokeredAccess.location.resolution_mode satisfies DeltaLocationResolutionMode;

  const deltaSharingSession = await createDeltaSharingClient({
    fetch: async () =>
      new Response(
        [
          JSON.stringify({ protocol: { minReaderVersion: 1 } }),
          JSON.stringify({
            file: {
              id: 'part-000.parquet',
              url: 'https://example.invalid/part-000.parquet',
              size: 128,
              partitionValues: {},
            },
          }),
        ].join('\n'),
        { headers: { 'delta-table-version': '7' } },
      ),
  }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'shape-token',
    },
  });
  const responseFormat: DeltaSharingResponseFormat = 'auto';
  const readPlan = await deltaSharingSession.resolveTable(
    { share: 'retail', schema: 'sales', table: 'orders' },
    { responseFormat },
  );
  readPlan satisfies DeltaSharingReadPlan;
  const openedShare = await client.openDeltaShare('shared_orders', {
    session: deltaSharingSession,
    table: { share: 'retail', schema: 'sales', table: 'orders' },
    responseFormat,
  });
  openedShare.deltaSharing.resolvedVersion satisfies number;

  await client.dispose('events');
  client.terminate();

  return result.result.bytes;
}

function workerUrlShapeCompiles(workerUrl: URL): AxonBrowserClient {
  return createAxonBrowserClient({ workerUrl });
}

function workerFactoryShapeCompiles(worker: Worker): AxonBrowserClient {
  return createAxonBrowserClient({ worker: () => worker });
}

function bundleSelectionShapeCompiles(): AxonBrowserClient {
  const features: PlatformFeatures = getPlatformFeatures();
  const manifest: BrowserBundleManifest = AXON_BROWSER_BUNDLE_MANIFEST;
  const selected = selectBundle(manifest, features);

  selected.bundle.workerUrl satisfies string | URL;
  selected.bundle.wasmUrl satisfies string | URL | undefined;
  selected.features.wasmSIMD satisfies boolean;

  return createAxonBrowserClient({ bundleManifest: manifest, platformFeatures: features });
}

function errorShapeCompiles(error: unknown): FallbackReason | undefined {
  if (error instanceof AxonWorkerError) {
    return error.fallbackReason;
  }
  return undefined;
}

function deltaSharingPersistenceShapeIsRejected(): Promise<unknown> {
  return createDeltaSharingClient().connect({
    source: 'json',
    value: {},
    // @ts-expect-error bearer metadata persistence is intentionally not exposed in the MVP SDK input.
    persistNonSecretMetadata: true,
  });
}

// @ts-expect-error active file descriptors must include browser-safe URLs.
const missingBrowserUrl: BrowserHttpFileDescriptor = {
  path: 'part-000.parquet',
  size_bytes: 128,
  partition_values: {},
};

void sdkShapeCompiles;
void workerUrlShapeCompiles;
void workerFactoryShapeCompiles;
void bundleSelectionShapeCompiles;
void errorShapeCompiles;
void deltaSharingPersistenceShapeIsRejected;
void missingBrowserUrl;

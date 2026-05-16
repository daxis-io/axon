import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  AxonWorkerError,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
  type AxonBrowserClient,
  type BrowserBundleManifest,
  type BrowserHttpFileDescriptor,
  type BrowserHttpSnapshotDescriptor,
  type FallbackReason,
  type PlatformFeatures,
  type QueryRequest,
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
void missingBrowserUrl;

import {
  DeltaLocationResolverError,
  type AxonBrowserClient,
  type CredentialProfileRef,
  type DeltaLocationResolveRequest,
  type DeltaLocationResolveResponse,
  type DeltaLocationResolverErrorCode,
  type DeltaLocationServerSnapshotOpenedEnvelope,
  type ResolverRequestedAccessMode,
} from '../src/axon-browser-sdk';

type DaxisResolverErrorBody = {
  code?: unknown;
  message?: unknown;
  correlation_id?: unknown;
};

const DELTA_LOCATION_RESOLVER_ERROR_CODES = new Set<DeltaLocationResolverErrorCode>([
  'invalid_table_uri',
  'invalid_snapshot_version',
  'credential_profile_required',
  'credential_profile_not_found',
  'provider_not_supported',
  'access_mode_not_supported',
  'snapshot_version_not_found',
  'not_a_delta_table',
  'resolver_unavailable',
  'storage_auth_failed',
  'storage_cors_failed',
  'descriptor_expired',
  'policy_blocked',
  'proxy_required',
  'signed_url_unavailable',
]);

export type DaxisResolvedDeltaTableOpenOptions = {
  tableName: string;
  tableUri: string;
  credentialProfileId: string;
  credentialProfileDisplayName?: string;
  resolverUrl?: string | URL;
  requestedAccessMode?: ResolverRequestedAccessMode;
  snapshotVersion?: number;
  requestId?: string;
};

const DEFAULT_DAXIS_DESCRIPTOR_RESOLVER_URL = '/v1/query/delta/snapshot-descriptor';
const DAXIS_REQUEST_ID_HEADER = 'x-daxis-request-id';

export async function openDaxisResolvedDeltaTable(
  client: AxonBrowserClient,
  options: DaxisResolvedDeltaTableOpenOptions,
): Promise<DeltaLocationServerSnapshotOpenedEnvelope> {
  const credentialProfile: CredentialProfileRef = {
    id: options.credentialProfileId,
  };
  if (options.credentialProfileDisplayName) {
    credentialProfile.display_name = options.credentialProfileDisplayName;
  }

  return client.openDeltaLocation(options.tableName, {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: options.tableUri,
    credentialProfile,
    requestedAccessMode: options.requestedAccessMode ?? 'auto',
    resolveDeltaLocation: (request) => resolveDaxisDeltaLocation(request, options),
    snapshotVersion: options.snapshotVersion,
    requestId: options.requestId,
  });
}

async function resolveDaxisDeltaLocation(
  request: DeltaLocationResolveRequest,
  options: DaxisResolvedDeltaTableOpenOptions,
): Promise<DeltaLocationResolveResponse> {
  const response = await fetch(options.resolverUrl ?? DEFAULT_DAXIS_DESCRIPTOR_RESOLVER_URL, {
    method: 'POST',
    headers: daxisJsonHeaders(options.requestId),
    body: JSON.stringify(request),
    credentials: 'same-origin',
  });
  if (!response.ok) {
    const body = await readDaxisResolverErrorBody(response);
    throw daxisResolverErrorFromResponse(body, response.status);
  }
  return response.json();
}

async function readDaxisResolverErrorBody(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch (error) {
    throw new DeltaLocationResolverError(
      'resolver_unavailable',
      `Daxis descriptor resolver returned non-JSON response (${response.status}): ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function daxisResolverErrorFromResponse(body: unknown, status: number): DeltaLocationResolverError {
  if (isDaxisResolverErrorBody(body)) {
    return new DeltaLocationResolverError(
      daxisResolverErrorCode(body.code),
      typeof body.message === 'string'
        ? body.message
        : `Daxis descriptor resolver returned ${status}`,
      {
        correlationId: typeof body.correlation_id === 'string' ? body.correlation_id : undefined,
      },
    );
  }

  return new DeltaLocationResolverError(
    'resolver_unavailable',
    `Daxis descriptor resolver returned ${status}`,
  );
}

function isDaxisResolverErrorBody(body: unknown): body is DaxisResolverErrorBody {
  return typeof body === 'object' && body !== null;
}

function daxisResolverErrorCode(code: unknown): DeltaLocationResolverErrorCode {
  if (
    typeof code === 'string' &&
    DELTA_LOCATION_RESOLVER_ERROR_CODES.has(code as DeltaLocationResolverErrorCode)
  ) {
    return code as DeltaLocationResolverErrorCode;
  }
  return 'resolver_unavailable';
}

function daxisJsonHeaders(requestId: string | undefined): Record<string, string> {
  const headers: Record<string, string> = {
    accept: 'application/json',
    'content-type': 'application/json',
  };
  if (requestId) {
    headers[DAXIS_REQUEST_ID_HEADER] = requestId;
  }
  return headers;
}

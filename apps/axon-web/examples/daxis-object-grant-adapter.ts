import {
  redactUrlSecrets,
  type BrokeredDeltaPlanAdapter,
  type BrokeredDeltaReadAccessPlan,
} from '../src/axon-browser-sdk';

export type DaxisObjectGrantAdapterOptions = {
  objectGrantBaseUrl?: string | URL;
  requestId?: string;
  resolveSnapshot: BrokeredDeltaPlanAdapter['resolveSnapshot'];
};

export type DaxisObjectGrantClientOptions = {
  objectGrantBaseUrl?: string | URL;
  requestId?: string;
};

export type DaxisObjectGrantObject = {
  path: string;
  sizeBytes: number;
  etag?: string;
  lastModifiedEpochMs?: number;
  contentType?: string;
};

export type DaxisObjectGrantListResponse = {
  objects: DaxisObjectGrantObject[];
};

export type DaxisObjectGrantRangeRequest = {
  path: string;
  start: number;
  end: number;
};

export type DaxisObjectGrantSignedUrl = {
  path: string;
  url: string;
  expiresAtEpochMs: number;
};

export type DaxisObjectGrantBatchSignResponse = {
  signedUrls: DaxisObjectGrantSignedUrl[];
};

export type DaxisObjectGrantClient = {
  list(plan: BrokeredDeltaReadAccessPlan, prefix: string): Promise<DaxisObjectGrantListResponse>;
  head(plan: BrokeredDeltaReadAccessPlan, path: string): Promise<DaxisObjectGrantObject>;
  batchSign(
    plan: BrokeredDeltaReadAccessPlan,
    paths: string[],
  ): Promise<DaxisObjectGrantBatchSignResponse>;
  range(
    plan: BrokeredDeltaReadAccessPlan,
    request: DaxisObjectGrantRangeRequest,
  ): Promise<Uint8Array>;
};

type DaxisObjectGrantErrorBody = {
  code?: unknown;
  message?: unknown;
  correlation_id?: unknown;
};

const DEFAULT_DAXIS_OBJECT_GRANT_BASE_URL = '/object-grants';
const DAXIS_REQUEST_ID_HEADER = 'x-daxis-request-id';

export class DaxisObjectGrantError extends Error {
  readonly name = 'DaxisObjectGrantError';
  readonly code: string;
  readonly status?: number;
  readonly correlationId?: string;

  constructor(code: string, message: string, options: { status?: number; correlationId?: string }) {
    super(redactUrlSecrets(message));
    this.code = code;
    this.status = options.status;
    this.correlationId = options.correlationId;
  }
}

export function createDaxisObjectGrantAdapter(
  options: DaxisObjectGrantAdapterOptions,
): BrokeredDeltaPlanAdapter {
  const client = createDaxisObjectGrantClient(options);
  return {
    resolveSnapshot: options.resolveSnapshot,
    batchSign: (plan, paths) => client.batchSign(plan, paths),
  };
}

export function createDaxisObjectGrantClient(
  options: DaxisObjectGrantClientOptions = {},
): DaxisObjectGrantClient {
  return {
    list: async (plan, prefix) => {
      assertDaxisObjectGrantLive(plan);
      assertDaxisObjectGrantRouteAllowed(
        plan.objectAccess.list,
        'object_grant_list_unavailable',
        'Daxis object grant does not allow list requests',
      );
      return requestDaxisObjectGrantJson<DaxisObjectGrantListResponse>(
        objectGrantRoute(options.objectGrantBaseUrl, plan.grantId, 'list'),
        'Daxis object grant list response',
        normalizeDaxisObjectGrantListResponse,
        {
          method: 'POST',
          headers: daxisJsonHeaders(options.requestId),
          body: JSON.stringify({ prefix }),
          credentials: 'same-origin',
        },
      );
    },
    head: async (plan, path) => {
      assertDaxisObjectGrantLive(plan);
      assertDaxisObjectGrantRouteAllowed(
        plan.objectAccess.head,
        'object_grant_head_unavailable',
        'Daxis object grant does not allow head requests',
      );
      return requestDaxisObjectGrantJson<DaxisObjectGrantObject>(
        objectGrantRoute(options.objectGrantBaseUrl, plan.grantId, 'head'),
        'Daxis object grant head response',
        normalizeDaxisObjectGrantObject,
        {
          method: 'POST',
          headers: daxisJsonHeaders(options.requestId),
          body: JSON.stringify({ path }),
          credentials: 'same-origin',
        },
      );
    },
    batchSign: async (plan, paths) => {
      assertDaxisObjectGrantLive(plan);
      assertDaxisObjectGrantRouteAllowed(
        plan.objectAccess.batchSign,
        'batch_sign_unavailable',
        'Daxis object grant does not allow batch signing',
      );
      return requestDaxisObjectGrantJson<DaxisObjectGrantBatchSignResponse>(
        objectGrantRoute(options.objectGrantBaseUrl, plan.grantId, 'batch-sign'),
        'Daxis object grant batch-sign response',
        normalizeDaxisObjectGrantBatchSignResponse,
        {
          method: 'POST',
          headers: daxisJsonHeaders(options.requestId),
          body: JSON.stringify({ paths }),
          credentials: 'same-origin',
        },
      );
    },
    range: async (plan, request) => {
      assertDaxisObjectGrantLive(plan);
      assertDaxisProxyRangeAllowed(plan);
      assertDaxisObjectGrantRangeRequest(request);
      return requestDaxisObjectGrantRange(
        objectGrantRangeRoute(options.objectGrantBaseUrl, plan.grantId, request),
        options.requestId,
        request,
      );
    },
  };
}

function assertDaxisObjectGrantLive(plan: BrokeredDeltaReadAccessPlan): void {
  if (plan.expiresAtEpochMs <= Date.now()) {
    throw new DaxisObjectGrantError(
      'grant_expired',
      'Daxis object grant expired before the route call was issued',
      {},
    );
  }
}

function assertDaxisObjectGrantRouteAllowed(allowed: boolean, code: string, message: string): void {
  if (!allowed) {
    throw new DaxisObjectGrantError(code, message, {});
  }
}

function assertDaxisProxyRangeAllowed(plan: BrokeredDeltaReadAccessPlan): void {
  if (!plan.objectAccess.proxyRange) {
    throw new DaxisObjectGrantError(
      'proxy_range_unavailable',
      'Daxis object grant does not allow proxy range reads',
      {},
    );
  }
}

async function requestDaxisObjectGrantJson<T>(
  url: string,
  path: string,
  normalize: (body: unknown, path: string, status: number) => T,
  init: RequestInit,
): Promise<T> {
  if (typeof fetch !== 'function') {
    throw new DaxisObjectGrantError(
      'object_grant_unavailable',
      'global fetch is not available for Daxis object grant requests',
      {},
    );
  }

  const response = await fetch(url, init);
  const body = await readDaxisObjectGrantJson(response);
  if (!response.ok) {
    throw daxisObjectGrantErrorFromResponse(body, response.status);
  }
  return normalize(body, path, response.status);
}

async function requestDaxisObjectGrantRange(
  url: string,
  requestId: string | undefined,
  request: DaxisObjectGrantRangeRequest,
): Promise<Uint8Array> {
  if (typeof fetch !== 'function') {
    throw new DaxisObjectGrantError(
      'object_grant_unavailable',
      'global fetch is not available for Daxis object grant requests',
      {},
    );
  }

  const response = await fetch(url, {
    method: 'GET',
    headers: daxisRangeHeaders(requestId),
    credentials: 'same-origin',
  });
  if (!response.ok) {
    const body = await readDaxisObjectGrantJson(response);
    throw daxisObjectGrantErrorFromResponse(body, response.status);
  }
  if (response.status !== 206) {
    throw new DaxisObjectGrantError(
      'object_grant_unavailable',
      `Daxis object grant range request returned HTTP ${response.status}`,
      { status: response.status },
    );
  }
  const contentType = response.headers.get('content-type')?.toLowerCase() ?? '';
  if (!contentType.split(';', 1)[0].trim().includes('application/octet-stream')) {
    throw invalidDaxisObjectGrantResponse(
      'Daxis object grant range response must use application/octet-stream',
      response.status,
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  const expectedLength = request.end - request.start;
  if (bytes.byteLength !== expectedLength) {
    throw invalidDaxisObjectGrantResponse(
      `Daxis object grant range response returned ${bytes.byteLength} bytes for a ${expectedLength}-byte request`,
      response.status,
    );
  }
  return bytes;
}

async function readDaxisObjectGrantJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch (error) {
    throw new DaxisObjectGrantError(
      'object_grant_unavailable',
      `Daxis object grant request returned non-JSON response (${response.status}): ${
        error instanceof Error ? error.message : String(error)
      }`,
      { status: response.status },
    );
  }
}

function daxisObjectGrantErrorFromResponse(body: unknown, status: number): DaxisObjectGrantError {
  if (isDaxisObjectGrantErrorBody(body)) {
    return new DaxisObjectGrantError(
      typeof body.code === 'string' ? body.code : 'object_grant_unavailable',
      typeof body.message === 'string'
        ? body.message
        : `Daxis object grant request returned ${status}`,
      {
        status,
        correlationId: typeof body.correlation_id === 'string' ? body.correlation_id : undefined,
      },
    );
  }

  return new DaxisObjectGrantError(
    'object_grant_unavailable',
    `Daxis object grant request returned ${status}`,
    { status },
  );
}

function isDaxisObjectGrantErrorBody(body: unknown): body is DaxisObjectGrantErrorBody {
  return typeof body === 'object' && body !== null;
}

function assertDaxisObjectGrantRangeRequest(request: DaxisObjectGrantRangeRequest): void {
  if (typeof request.path !== 'string' || request.path.length === 0) {
    throw new DaxisObjectGrantError(
      'invalid_object_grant_request',
      'Daxis object grant range path must be a non-empty string',
      {},
    );
  }
  if (!Number.isSafeInteger(request.start) || request.start < 0) {
    throw new DaxisObjectGrantError(
      'invalid_object_grant_request',
      'Daxis object grant range start must be a non-negative integer',
      {},
    );
  }
  if (!Number.isSafeInteger(request.end) || request.end < 1) {
    throw new DaxisObjectGrantError(
      'invalid_object_grant_request',
      'Daxis object grant range end must be a positive integer',
      {},
    );
  }
  if (request.start >= request.end) {
    throw new DaxisObjectGrantError(
      'invalid_object_grant_request',
      'Daxis object grant range start must be less than end',
      {},
    );
  }
}

function normalizeDaxisObjectGrantListResponse(
  body: unknown,
  path: string,
  status: number,
): DaxisObjectGrantListResponse {
  const record = daxisObjectGrantRecord(body, path, status);
  const objects = daxisObjectGrantArray(record.objects, `${path}.objects`, status).map(
    (object, index) => normalizeDaxisObjectGrantObject(object, `${path}.objects[${index}]`, status),
  );
  return { objects };
}

function normalizeDaxisObjectGrantObject(
  body: unknown,
  path: string,
  status: number,
): DaxisObjectGrantObject {
  const record = daxisObjectGrantRecord(body, path, status);
  const object: DaxisObjectGrantObject = {
    path: daxisObjectGrantString(record.path, `${path}.path`, status),
    sizeBytes: daxisObjectGrantNonNegativeInteger(record.sizeBytes, `${path}.sizeBytes`, status),
  };
  if (record.etag !== undefined) {
    object.etag = daxisObjectGrantString(record.etag, `${path}.etag`, status);
  }
  if (record.lastModifiedEpochMs !== undefined) {
    object.lastModifiedEpochMs = daxisObjectGrantNonNegativeInteger(
      record.lastModifiedEpochMs,
      `${path}.lastModifiedEpochMs`,
      status,
    );
  }
  if (record.contentType !== undefined) {
    object.contentType = daxisObjectGrantString(record.contentType, `${path}.contentType`, status);
  }
  return object;
}

function normalizeDaxisObjectGrantBatchSignResponse(
  body: unknown,
  path: string,
  status: number,
): DaxisObjectGrantBatchSignResponse {
  const record = daxisObjectGrantRecord(body, path, status);
  const signedUrls = daxisObjectGrantArray(record.signedUrls, `${path}.signedUrls`, status).map(
    (signedUrl, index) =>
      normalizeDaxisObjectGrantSignedUrl(signedUrl, `${path}.signedUrls[${index}]`, status),
  );
  return { signedUrls };
}

function normalizeDaxisObjectGrantSignedUrl(
  body: unknown,
  path: string,
  status: number,
): DaxisObjectGrantSignedUrl {
  const record = daxisObjectGrantRecord(body, path, status);
  const url = daxisObjectGrantString(record.url, `${path}.url`, status);
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:') {
      throw new Error('URL must use HTTPS');
    }
  } catch (error) {
    throw invalidDaxisObjectGrantResponse(
      `${path}.url must be an HTTPS URL: ${error instanceof Error ? error.message : String(error)}`,
      status,
    );
  }
  return {
    path: daxisObjectGrantString(record.path, `${path}.path`, status),
    url,
    expiresAtEpochMs: daxisObjectGrantNonNegativeInteger(
      record.expiresAtEpochMs,
      `${path}.expiresAtEpochMs`,
      status,
    ),
  };
}

function daxisObjectGrantRecord(
  body: unknown,
  path: string,
  status: number,
): Record<string, unknown> {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    throw invalidDaxisObjectGrantResponse(`${path} must be an object`, status);
  }
  return body as Record<string, unknown>;
}

function daxisObjectGrantArray(body: unknown, path: string, status: number): unknown[] {
  if (!Array.isArray(body)) {
    throw invalidDaxisObjectGrantResponse(`${path} must be an array`, status);
  }
  return body;
}

function daxisObjectGrantString(body: unknown, path: string, status: number): string {
  if (typeof body !== 'string' || body.length === 0) {
    throw invalidDaxisObjectGrantResponse(`${path} must be a non-empty string`, status);
  }
  return body;
}

function daxisObjectGrantNonNegativeInteger(body: unknown, path: string, status: number): number {
  if (typeof body !== 'number' || !Number.isSafeInteger(body) || body < 0) {
    throw invalidDaxisObjectGrantResponse(`${path} must be a non-negative integer`, status);
  }
  return body;
}

function invalidDaxisObjectGrantResponse(message: string, status: number): DaxisObjectGrantError {
  return new DaxisObjectGrantError('invalid_object_grant_response', message, { status });
}

function objectGrantRoute(
  baseUrl: string | URL | undefined,
  grantId: string,
  action: 'list' | 'head' | 'batch-sign' | 'range',
): string {
  const base = String(baseUrl ?? DEFAULT_DAXIS_OBJECT_GRANT_BASE_URL).replace(/\/+$/g, '');
  return `${base}/${encodeURIComponent(grantId)}/${action}`;
}

function objectGrantRangeRoute(
  baseUrl: string | URL | undefined,
  grantId: string,
  request: DaxisObjectGrantRangeRequest,
): string {
  const params = new URLSearchParams({
    path: request.path,
    start: String(request.start),
    end: String(request.end),
  });
  return `${objectGrantRoute(baseUrl, grantId, 'range')}?${params}`;
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

function daxisRangeHeaders(requestId: string | undefined): Record<string, string> {
  const headers: Record<string, string> = {
    accept: 'application/octet-stream',
  };
  if (requestId) {
    headers[DAXIS_REQUEST_ID_HEADER] = requestId;
  }
  return headers;
}

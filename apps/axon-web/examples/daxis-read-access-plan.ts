import {
  redactUrlSecrets,
  type AxonBrowserClient,
  type BrokeredDeltaPlanAdapter,
  type SqlFallbackExecutor,
  type UcSessionRef,
  type UnityCatalogOpenResult,
} from '../src/axon-browser-sdk';

export type DaxisReadAccessPlanOpenOptions = {
  tableName: string;
  fullName: string;
  session?: UcSessionRef;
  readAccessPlanUrl?: string | URL;
  requestId?: string;
  brokeredDelta?: BrokeredDeltaPlanAdapter;
  serverFallbackEnabled?: boolean;
  executeSqlFallback?: SqlFallbackExecutor;
};

const DEFAULT_DAXIS_READ_ACCESS_PLAN_URL = '/v1/catalog/read-access-plan';
const DAXIS_REQUEST_ID_HEADER = 'x-daxis-request-id';

type DaxisReadAccessPlanErrorBody = {
  code?: unknown;
  message?: unknown;
  correlation_id?: unknown;
};

export class DaxisReadAccessPlanError extends Error {
  readonly name = 'DaxisReadAccessPlanError';
  readonly code: string;
  readonly status: number;
  readonly correlationId?: string;

  constructor(code: string, message: string, options: { status: number; correlationId?: string }) {
    super(redactUrlSecrets(message));
    this.code = code;
    this.status = options.status;
    this.correlationId = options.correlationId;
  }
}

export async function openDaxisReadAccessPlanTable(
  client: AxonBrowserClient,
  options: DaxisReadAccessPlanOpenOptions,
): Promise<UnityCatalogOpenResult> {
  return client.openUnityCatalogTable(options.tableName, {
    fullName: options.fullName,
    session: options.session,
    requestId: options.requestId,
    brokeredDelta: options.brokeredDelta,
    serverFallbackEnabled: options.serverFallbackEnabled,
    executeSqlFallback: options.executeSqlFallback,
    resolveReadAccessPlan: async (request) => {
      const response = await fetch(
        options.readAccessPlanUrl ?? DEFAULT_DAXIS_READ_ACCESS_PLAN_URL,
        {
          method: 'POST',
          headers: daxisJsonHeaders(options.requestId),
          body: JSON.stringify(request),
          credentials: 'same-origin',
        },
      );
      if (!response.ok) {
        const body = await readDaxisReadAccessPlanErrorBody(response);
        throw daxisReadAccessPlanErrorFromResponse(body, response.status);
      }
      return response.json();
    },
  });
}

async function readDaxisReadAccessPlanErrorBody(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch (error) {
    throw new DaxisReadAccessPlanError(
      'read_access_plan_unavailable',
      `Daxis read-access plan request returned non-JSON response (${response.status}): ${
        error instanceof Error ? error.message : String(error)
      }`,
      { status: response.status },
    );
  }
}

function daxisReadAccessPlanErrorFromResponse(
  body: unknown,
  status: number,
): DaxisReadAccessPlanError {
  if (isDaxisReadAccessPlanErrorBody(body)) {
    return new DaxisReadAccessPlanError(
      typeof body.code === 'string' ? body.code : 'read_access_plan_unavailable',
      typeof body.message === 'string'
        ? body.message
        : `Daxis read-access plan request returned ${status}`,
      {
        status,
        correlationId: typeof body.correlation_id === 'string' ? body.correlation_id : undefined,
      },
    );
  }

  return new DaxisReadAccessPlanError(
    'read_access_plan_unavailable',
    `Daxis read-access plan request returned ${status}`,
    { status },
  );
}

function isDaxisReadAccessPlanErrorBody(body: unknown): body is DaxisReadAccessPlanErrorBody {
  return typeof body === 'object' && body !== null;
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

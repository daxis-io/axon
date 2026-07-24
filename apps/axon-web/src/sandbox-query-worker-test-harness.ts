type CoordinatorTestConfig = {
  deadlineMs?: number;
  watchdogMs?: number;
  maxRequests?: number;
  crashOnCommandNumber?: number;
  firstChildUrl?: string;
};

type CoordinatorHarnessScope = {
  location: { href: string };
  __AXON_SANDBOX_QUERY_COORDINATOR_TEST_CONFIG__?: CoordinatorTestConfig;
  postMessage(message: { coordinator_test_ready: true }): void;
};

const scope = self as unknown as CoordinatorHarnessScope;
const parameters = new URL(scope.location.href).searchParams;
const firstChild = parameters.get('first_child');
const firstChildUrl =
  firstChild === 'hang'
    ? new URL('./sandbox-query-child-hang-test-worker.ts', import.meta.url).href
    : undefined;

scope.__AXON_SANDBOX_QUERY_COORDINATOR_TEST_CONFIG__ = {
  ...positiveIntegerParameter(parameters, 'deadline_ms', 'deadlineMs'),
  ...positiveIntegerParameter(parameters, 'watchdog_ms', 'watchdogMs'),
  ...positiveIntegerParameter(parameters, 'max_requests', 'maxRequests'),
  ...(firstChild === 'crash-on-command' ? { crashOnCommandNumber: 1 } : {}),
  ...positiveIntegerParameter(parameters, 'crash_on_command', 'crashOnCommandNumber'),
  ...(firstChildUrl ? { firstChildUrl } : {}),
};

await import('./sandbox-query-worker');
scope.postMessage({ coordinator_test_ready: true });

function positiveIntegerParameter(
  parameters: URLSearchParams,
  parameter: string,
  property: 'deadlineMs' | 'watchdogMs' | 'maxRequests' | 'crashOnCommandNumber',
): Partial<CoordinatorTestConfig> {
  const raw = parameters.get(parameter);
  if (raw === null) return {};
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`coordinator test parameter '${parameter}' must be a positive safe integer`);
  }
  return { [property]: value };
}

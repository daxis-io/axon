import { afterEach, describe, expect, it, vi } from 'vitest';
import type { QueryError } from './axon-browser-sdk.ts';
import { isQuerySessionInvalidation } from './services/query-session-invalidation.ts';

type Listener = (event: unknown) => void;
type CoordinatorPageMessage = {
  error?: { request_id: string; error: QueryError };
  log?: { message: string };
};

// Stands in for the private child worker. The coordinator only talks to it through the Worker
// interface, so a scripted child exercises the real coordinator recycle path.
class FakeChild {
  static all: FakeChild[] = [];
  readonly inbox: Array<{ kind: string; command?: Record<string, { request_id: string }> }> = [];
  readonly listeners = new Map<string, Listener[]>();
  terminated = false;

  constructor() {
    FakeChild.all.push(this);
  }

  addEventListener(type: string, listener: Listener): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  removeEventListener(): void {}

  postMessage(message: FakeChild['inbox'][number]): void {
    this.inbox.push(message);
  }

  terminate(): void {
    this.terminated = true;
  }

  deliver(data: unknown): void {
    for (const listener of this.listeners.get('message') ?? []) listener({ data });
  }

  crash(message: string): void {
    for (const listener of this.listeners.get('error') ?? []) {
      listener({ message, filename: '', lineno: 0, colno: 0, preventDefault() {} });
    }
  }

  commandIds(kind: string): string[] {
    return this.inbox.flatMap((message) =>
      message.kind === 'command' && message.command?.[kind]
        ? [message.command[kind].request_id]
        : [],
    );
  }
}

const tick = (ms = 0) => new Promise((resolve) => setTimeout(resolve, ms));

async function bootCoordinator(config: { watchdogMs?: number } = {}) {
  vi.resetModules();
  vi.spyOn(console, 'error').mockImplementation(() => undefined);
  FakeChild.all = [];
  const toPage: CoordinatorPageMessage[] = [];
  let coordinatorListener: Listener | undefined;
  vi.stubGlobal('self', {
    name: '',
    location: { href: 'http://localhost/sandbox-query-worker.js' },
    __AXON_SANDBOX_QUERY_COORDINATOR_TEST_CONFIG__: config,
    addEventListener(type: string, listener: Listener) {
      if (type === 'message') coordinatorListener = listener;
    },
    postMessage(message: CoordinatorPageMessage) {
      toPage.push(message);
    },
  });
  vi.stubGlobal('location', { href: 'http://localhost/' });
  vi.stubGlobal('Worker', FakeChild);
  await import('./sandbox-query-worker.ts');
  FakeChild.all[0]!.deliver({ kind: 'ready', version: 1 });
  return {
    send: (command: unknown) => coordinatorListener!({ data: command }),
    errorsFor: (requestId: string) =>
      toPage.flatMap((message) =>
        message.error?.request_id === requestId ? [message.error.error] : [],
      ),
  };
}

function openTable(requestId: string) {
  return {
    open_delta_table: {
      request_id: requestId,
      name: 't',
      snapshot: { table_uri: 'https://example.test/t', snapshot_version: 0 },
    },
  };
}

function sql(requestId: string) {
  return {
    sql: {
      request_id: requestId,
      name: 't',
      query: { sql: 'SELECT 1', options: {} },
      browser_safe_defaults: true,
    },
  };
}

function opened(child: FakeChild, requestId: string): void {
  child.deliver({
    kind: 'public',
    version: 1,
    envelope: { opened: { request_id: requestId, name: 't' } },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('sandbox query coordinator child recycle', () => {
  it('invalidates the session on the next command after a drain watchdog recycle', async () => {
    const coordinator = await bootCoordinator({ watchdogMs: 20 });
    const first = FakeChild.all[0]!;
    coordinator.send(openTable('open-1'));
    opened(first, 'open-1');
    coordinator.send(sql('query-1'));
    coordinator.send({ cancel: { request_id: 'cancel-1', query_id: 'query-1' } });
    expect(coordinator.errorsFor('query-1').map(isQuerySessionInvalidation)).toEqual([false]);

    // The child never drains the cancelled query, so the watchdog replaces it.
    await tick(60);
    expect(first.terminated).toBe(true);
    const replacement = FakeChild.all[1]!;
    replacement.deliver({ kind: 'ready', version: 1 });

    // The replacement has an empty session. The page still believes its table is open, so the
    // next query must tell it to discard the session rather than reach the empty child.
    coordinator.send(sql('query-2'));
    const [invalidation] = coordinator.errorsFor('query-2');
    expect(invalidation).toBeDefined();
    expect(isQuerySessionInvalidation(invalidation!)).toBe(true);
    expect(invalidation!.message).toContain("did not drain 'query-1'");
    expect(replacement.commandIds('sql')).toEqual([]);

    // The reopened session reaches the replacement child normally.
    coordinator.send(openTable('open-2'));
    opened(replacement, 'open-2');
    coordinator.send(sql('query-3'));
    expect(replacement.commandIds('open_delta_table')).toEqual(['open-2']);
    expect(replacement.commandIds('sql')).toEqual(['query-3']);
    expect(coordinator.errorsFor('open-2')).toEqual([]);
    expect(coordinator.errorsFor('query-3')).toEqual([]);
  });

  it('invalidates the session on the next command after an idle child that held tables crashes', async () => {
    const coordinator = await bootCoordinator();
    const first = FakeChild.all[0]!;
    coordinator.send(openTable('open-1'));
    opened(first, 'open-1');

    first.crash('idle trap');
    expect(first.terminated).toBe(true);
    FakeChild.all[1]!.deliver({ kind: 'ready', version: 1 });

    coordinator.send(sql('query-1'));
    expect(coordinator.errorsFor('query-1').map(isQuerySessionInvalidation)).toEqual([true]);
    coordinator.send(sql('query-2'));
    expect(FakeChild.all[1]!.commandIds('sql')).toEqual(['query-2']);
  });

  it('does not invalidate twice when the recycle already failed in-flight work', async () => {
    const coordinator = await bootCoordinator();
    const first = FakeChild.all[0]!;
    coordinator.send(openTable('open-1'));
    opened(first, 'open-1');
    coordinator.send(sql('query-1'));

    first.crash('trap during query');
    expect(coordinator.errorsFor('query-1').map(isQuerySessionInvalidation)).toEqual([true]);
    FakeChild.all[1]!.deliver({ kind: 'ready', version: 1 });

    coordinator.send(openTable('open-2'));
    expect(coordinator.errorsFor('open-2')).toEqual([]);
    expect(FakeChild.all[1]!.commandIds('open_delta_table')).toEqual(['open-2']);
  });

  it('does not invalidate work sent after a child that never received any crashes', async () => {
    const coordinator = await bootCoordinator();
    FakeChild.all[0]!.crash('idle trap before any command');
    FakeChild.all[1]!.deliver({ kind: 'ready', version: 1 });

    coordinator.send(openTable('open-1'));
    expect(coordinator.errorsFor('open-1')).toEqual([]);
    expect(FakeChild.all[1]!.commandIds('open_delta_table')).toEqual(['open-1']);
  });
});

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// The real child worker runs against a scripted session, so these tests cover how it sets up
// external memory around a query without building the Wasm artifact.
const wasm = vi.hoisted(() => ({
  log: [] as string[],
  executionId: undefined as number | undefined,
  createSpillFile: false,
  terminal: {} as Record<string, unknown>,
}));

vi.mock('./wasm/axon_web_wasm', () => {
  class SandboxSqlStream {
    #done = false;

    async next() {
      if (this.#done) return null;
      this.#done = true;
      // Give a spill writer started by the query time to land before the terminal.
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { terminal: { metadata_json: JSON.stringify(wasm.terminal) } };
    }

    cancel() {}

    cancel_for_deadline() {}

    async close() {}
  }

  class SandboxQuerySession {
    set_external_memory_execution(executionId: number) {
      wasm.executionId = executionId;
      wasm.log.push(`set_external_memory_execution(${executionId === 0 ? 0 : 'opfs'})`);
    }

    clear_external_memory_execution() {
      wasm.executionId = undefined;
    }

    async start_sql_stream() {
      wasm.log.push('start_sql_stream');
      if (wasm.createSpillFile && wasm.executionId) {
        const bridge = globalThis as unknown as {
          axonSpillStartCreateWriter: (executionId: number, scopeId: number) => number;
        };
        bridge.axonSpillStartCreateWriter(wasm.executionId, 1);
      }
      return new SandboxSqlStream();
    }
  }

  return {
    default: async () => undefined,
    SandboxQuerySession,
    SandboxSqlStream,
    axon_spill_wake: () => undefined,
  };
});

type ChildMessage = {
  kind: string;
  query_id?: string;
  error?: { code: string; message: string; resource_details?: unknown };
  metadata?: {
    status?: string;
    error?: { code: string; message: string; resource_details?: unknown };
    response?: { capabilities?: { capabilities?: Record<string, unknown> } };
  };
};

type FakeEntry = FakeDirectory | FakeFile;

class FakeHandle {
  bytes = new Uint8Array(0);

  write(source: Uint8Array, options?: { at?: number }) {
    const at = options?.at ?? 0;
    const next = new Uint8Array(Math.max(this.bytes.length, at + source.length));
    next.set(this.bytes);
    next.set(source, at);
    this.bytes = next;
    return source.byteLength;
  }

  read(target: Uint8Array, options?: { at?: number }) {
    const slice = this.bytes.subarray(options?.at ?? 0, (options?.at ?? 0) + target.length);
    target.set(slice);
    return slice.length;
  }

  flush() {}

  truncate(size: number) {
    this.bytes = this.bytes.slice(0, size);
  }

  close() {}
}

class FakeFile {
  async createSyncAccessHandle() {
    return new FakeHandle();
  }
}

class FakeDirectory {
  readonly children = new Map<string, FakeEntry>();

  constructor(private readonly opfs: FakeOpfs) {}

  async getDirectoryHandle(name: string, options?: { create?: boolean }) {
    return this.#child(name, options, () => new FakeDirectory(this.opfs)) as FakeDirectory;
  }

  async getFileHandle(name: string, options?: { create?: boolean }) {
    if (name === 'probe') this.opfs.probes += 1;
    return this.#child(name, options, () => new FakeFile()) as FakeFile;
  }

  async removeEntry(name: string) {
    const entry = this.children.get(name);
    if (entry instanceof FakeDirectory && entry.children.has('lease') && this.opfs.failNamespace) {
      throw new DOMException('transient delete failure', 'InvalidModificationError');
    }
    this.children.delete(name);
  }

  async *entries() {
    yield* this.children.entries();
  }

  #child(name: string, options: { create?: boolean } | undefined, create: () => FakeEntry) {
    let entry = this.children.get(name);
    if (!entry) {
      if (!options?.create) throw new DOMException('missing', 'NotFoundError');
      entry = create();
      this.children.set(name, entry);
    }
    return entry;
  }
}

class FakeOpfs {
  probes = 0;
  failNamespace = false;
  readonly root = new FakeDirectory(this);
}

function installStorage(storage: {
  opfs?: FakeOpfs;
  estimate?: { quota: number; usage: number };
}): void {
  vi.stubGlobal('navigator', {
    storage: {
      ...(storage.opfs ? { getDirectory: async () => storage.opfs!.root } : {}),
      estimate: async () => storage.estimate ?? { quota: 10_000_000_000, usage: 0 },
    },
  });
}

async function bootChild() {
  const posted: ChildMessage[] = [];
  let listener: ((event: { data: unknown }) => void) | undefined;
  vi.stubGlobal('self', {
    name: 'axon-sandbox-query-child?browser_memory_profile_mib=128',
    addEventListener: (_type: string, next: (event: { data: unknown }) => void) => {
      listener = next;
    },
    postMessage: (message: ChildMessage) => posted.push(message),
  });
  vi.resetModules();
  await import('./sandbox-query-child-worker.ts');
  return {
    async query(queryId: string): Promise<ChildMessage> {
      listener!({
        data: {
          kind: 'command',
          version: 1,
          command: {
            sql: {
              request_id: queryId,
              name: 'events',
              query: { sql: 'SELECT MIN(event_id) FROM events', preferred_target: 'browser_wasm' },
              delivery: 'chunked_buffers',
              browser_safe_defaults: true,
            },
          },
        },
      });
      for (let attempt = 0; attempt < 200; attempt += 1) {
        const outcome = posted.find(
          (message) =>
            message.query_id === queryId &&
            ['stream_start_failed', 'stream_fault', 'stream_terminal'].includes(message.kind),
        );
        if (outcome) return outcome;
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      throw new Error(`query ${queryId} produced no outcome`);
    },
  };
}

function succeededTerminal(error?: unknown) {
  return {
    metadata_version: 1,
    status: error ? 'failed' : 'succeeded',
    ...(error ? { error } : {}),
    arrow_ipc_byte_length: '0',
    row_count: '1',
    response: { metrics: {}, capabilities: { capabilities: {} } },
  };
}

beforeEach(() => {
  wasm.log.length = 0;
  wasm.executionId = undefined;
  wasm.createSpillFile = false;
  wasm.terminal = succeededTerminal();
  vi.spyOn(console, 'warn').mockImplementation(() => undefined);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('sandbox query child external memory', () => {
  it('runs non-spilling queries without OPFS and reports external memory unsupported', async () => {
    installStorage({});
    const child = await bootChild();

    for (const queryId of ['query-1', 'query-2']) {
      const outcome = await child.query(queryId);
      expect(outcome.kind).toBe('stream_terminal');
      expect(outcome.metadata?.status).toBe('succeeded');
      expect(outcome.metadata?.response?.capabilities?.capabilities).toMatchObject({
        browser_external_memory: 'unsupported',
      });
    }
    expect(wasm.log).toEqual([
      'set_external_memory_execution(0)',
      'start_sql_stream',
      'set_external_memory_execution(0)',
      'start_sql_stream',
    ]);
  });

  it('reports the storage setup reason when a query without spill storage needs to spill', async () => {
    installStorage({ opfs: new FakeOpfs(), estimate: { quota: 1_000_000, usage: 999_999 } });
    wasm.terminal = succeededTerminal({
      code: 'resource_exhausted',
      message: 'browser aggregate spill storage is unavailable',
      target: 'browser_wasm',
      resource_details: { resource: 'spill_storage', reason: 'unavailable' },
    });
    const child = await bootChild();

    const outcome = await child.query('query-1');

    expect(wasm.log).toEqual(['set_external_memory_execution(0)', 'start_sql_stream']);
    expect(outcome.kind).toBe('stream_terminal');
    expect(outcome.metadata?.error).toMatchObject({
      code: 'resource_exhausted',
      resource_details: { resource: 'spill_storage', reason: 'quota_exceeded' },
    });
  });

  it('probes OPFS once per child and installs a spill scope for every query', async () => {
    const opfs = new FakeOpfs();
    installStorage({ opfs });
    const child = await bootChild();

    for (const queryId of ['query-1', 'query-2', 'query-3']) {
      const outcome = await child.query(queryId);
      expect(outcome.kind).toBe('stream_terminal');
      expect(outcome.metadata?.response?.capabilities?.capabilities).toMatchObject({
        browser_external_memory: 'supported',
      });
    }
    expect(opfs.probes).toBe(1);
    expect(
      wasm.log.filter((entry) => entry === 'set_external_memory_execution(opfs)'),
    ).toHaveLength(3);
  });

  it('keeps a successful terminal when only the empty spill namespace cleanup fails', async () => {
    const opfs = new FakeOpfs();
    opfs.failNamespace = true;
    installStorage({ opfs });
    const child = await bootChild();

    const outcome = await child.query('query-1');

    expect(outcome.kind).toBe('stream_terminal');
    expect(outcome.metadata?.status).toBe('succeeded');
  });

  it('still fails the query when cleanup of spilled files fails', async () => {
    const opfs = new FakeOpfs();
    opfs.failNamespace = true;
    installStorage({ opfs });
    wasm.createSpillFile = true;
    const child = await bootChild();

    const outcome = await child.query('query-1');

    expect(outcome.kind).toBe('stream_fault');
    expect(outcome.error).toMatchObject({
      code: 'resource_exhausted',
      resource_details: { resource: 'spill_storage', reason: 'io_failure' },
    });
  });
});

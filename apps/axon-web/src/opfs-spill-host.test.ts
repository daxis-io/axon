import { describe, expect, it } from 'vitest';
import * as opfsSpillHostModule from './opfs-spill-host.ts';
import {
  OpfsSpillHost,
  probeBrowserExternalMemory,
  type OpfsDirectory,
  type OpfsFile,
  type OpfsStorage,
  type OpfsSyncAccess,
} from './opfs-spill-host.ts';

type MemoryStorageHooks = {
  beforeGetFileHandle?: (name: string) => Promise<void> | void;
  beforeCreateSyncAccessHandle?: (name: string) => Promise<void> | void;
  beforeRemoveEntry?: (name: string) => Promise<void> | void;
  closeError?: (name: string) => unknown;
  flushError?: (name: string) => unknown;
  readLength?: (name: string, requested: number) => number;
  writeLength?: (name: string, requested: number) => number;
};

class MemoryFile implements OpfsFile {
  bytes = new Uint8Array();
  locked = false;
  lastModified = Date.now();

  constructor(
    private readonly name: string,
    private readonly hooks: MemoryStorageHooks,
  ) {}

  async createSyncAccessHandle(): Promise<OpfsSyncAccess> {
    await this.hooks.beforeCreateSyncAccessHandle?.(this.name);
    if (this.locked) throw new DOMException('locked', 'NoModificationAllowedError');
    this.locked = true;
    let closed = false;
    return {
      read: (target, options) => {
        if (closed) throw new DOMException('closed', 'InvalidStateError');
        const source = this.bytes.subarray(options.at, options.at + target.byteLength);
        const read = this.hooks.readLength?.(this.name, source.byteLength) ?? source.byteLength;
        target.set(source.subarray(0, read));
        return read;
      },
      write: (source, options) => {
        if (closed) throw new DOMException('closed', 'InvalidStateError');
        const written = this.hooks.writeLength?.(this.name, source.byteLength) ?? source.byteLength;
        const required = options.at + written;
        if (required > this.bytes.byteLength) {
          const next = new Uint8Array(required);
          next.set(this.bytes);
          this.bytes = next;
        }
        this.bytes.set(source.subarray(0, written), options.at);
        this.lastModified = Date.now();
        return written;
      },
      truncate: (size) => {
        this.bytes = this.bytes.slice(0, size);
        this.lastModified = Date.now();
      },
      flush: () => {
        const error = this.hooks.flushError?.(this.name);
        if (error) throw error;
      },
      getSize: () => this.bytes.byteLength,
      close: () => {
        if (!closed) this.locked = false;
        closed = true;
        const error = this.hooks.closeError?.(this.name);
        if (error) throw error;
      },
    };
  }

  async getLastModified(): Promise<number> {
    return this.lastModified;
  }
}

class MemoryDirectory implements OpfsDirectory {
  readonly directories = new Map<string, MemoryDirectory>();
  readonly files = new Map<string, MemoryFile>();

  constructor(private readonly hooks: MemoryStorageHooks = {}) {}

  async getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<OpfsDirectory> {
    const existing = this.directories.get(name);
    if (existing) return existing;
    if (!options?.create) throw new DOMException('missing', 'NotFoundError');
    const directory = new MemoryDirectory(this.hooks);
    this.directories.set(name, directory);
    return directory;
  }

  async getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFile> {
    await this.hooks.beforeGetFileHandle?.(name);
    const existing = this.files.get(name);
    if (existing) return existing;
    if (!options?.create) throw new DOMException('missing', 'NotFoundError');
    const file = new MemoryFile(name, this.hooks);
    this.files.set(name, file);
    return file;
  }

  async removeEntry(name: string, options?: { recursive?: boolean }): Promise<void> {
    await this.hooks.beforeRemoveEntry?.(name);
    if (this.files.delete(name)) return;
    const directory = this.directories.get(name);
    if (!directory) throw new DOMException('missing', 'NotFoundError');
    if (!options?.recursive && (directory.files.size > 0 || directory.directories.size > 0)) {
      throw new DOMException('not empty', 'InvalidModificationError');
    }
    this.directories.delete(name);
  }

  async *directoryEntries(): AsyncIterable<[string, OpfsDirectory]> {
    yield* this.directories.entries();
  }
}

function memoryStorage(
  quota = 1024 * 1024,
  hooks: MemoryStorageHooks = {},
): OpfsStorage & { root: MemoryDirectory } {
  const root = new MemoryDirectory(hooks);
  return {
    root,
    getDirectory: async () => root,
    estimate: async () => ({ quota, usage: 0 }),
  };
}

function deferred(): {
  promise: Promise<void>;
  resolve: () => void;
} {
  let resolve!: () => void;
  const promise = new Promise<void>((innerResolve) => {
    resolve = innerResolve;
  });
  return { promise, resolve };
}

describe('browser OPFS spill host', () => {
  it('degrades to unavailable when the real probe succeeds but host initialization fails', async () => {
    const initialize = (
      opfsSpillHostModule as typeof opfsSpillHostModule & {
        initializeBrowserExternalMemory?: (
          storage: OpfsStorage,
          options: { productionCapBytes: number },
        ) => Promise<{ available: boolean; reason?: string; host?: OpfsSpillHost }>;
      }
    ).initializeBrowserExternalMemory;
    expect(initialize).toBeTypeOf('function');
    if (!initialize) return;
    const storage = memoryStorage();
    storage.estimate = async () => {
      throw new DOMException('transient estimate failure', 'UnknownError');
    };

    await expect(initialize(storage, { productionCapBytes: 64 * 1024 })).resolves.toMatchObject({
      available: false,
      reason: 'unavailable',
    });
  });

  it('uses a real write, flush, read, truncate, close, and delete probe', async () => {
    const storage = memoryStorage();

    expect(await probeBrowserExternalMemory(storage)).toEqual({ available: true });
    const axon = storage.root.directories.get('axon-spill');
    const v1 = axon?.directories.get('v1');
    expect(v1?.directories.size).toBe(0);
    expect(v1?.files.size).toBe(0);
  });

  it('owns opaque query scopes, sequential handles, accounting, and deterministic cleanup', async () => {
    const storage = memoryStorage();
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const scope = await host.createScope();
    const { file, writerId } = await host.createFile(scope);

    await host.append(writerId, new TextEncoder().encode('first '));
    await host.append(writerId, new TextEncoder().encode('second'));
    await host.finalizeWriter(writerId);
    const readerId = await host.openReader(file);
    const chunks = [];
    for (;;) {
      const chunk = host.readNext(readerId, 3);
      if (!chunk) break;
      chunks.push(...chunk);
    }
    host.closeReader(readerId);

    expect(new TextDecoder().decode(Uint8Array.from(chunks))).toBe('first second');
    expect(host.metrics()).toMatchObject({
      bytesWritten: 12,
      bytesRead: 12,
      filesCreated: 1,
      activeBytes: 12,
      activeFiles: 1,
    });
    await host.deleteScope(scope);
    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    });
  });

  it('enforces the lower of the production cap and half the estimated free quota', async () => {
    const host = await OpfsSpillHost.open(memoryStorage(20), { productionCapBytes: 64 * 1024 });
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);

    expect(() => host.append(writerId, new Uint8Array(11))).toThrowError(
      expect.objectContaining({ reason: 'quota_exceeded' }),
    );
  });

  it('accounts partial physical writes until query cleanup reclaims them', async () => {
    let partialWrite = false;
    const storage = memoryStorage(1024 * 1024, {
      writeLength: (name, requested) =>
        partialWrite && name.startsWith('spill-') ? Math.floor(requested / 2) : requested,
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);
    partialWrite = true;

    expect(() => host.append(writerId, new Uint8Array(8))).toThrowError(
      expect.objectContaining({ reason: 'io_failure' }),
    );
    expect(host.metrics().activeBytes).toBe(4);

    await host.releaseQueryScopes(query);
    expect(host.metrics()).toMatchObject({ activeBytes: 0, activeFiles: 0, activeHandles: 0 });
  });

  it('retains a failed writer until terminal cleanup closes and deletes it', async () => {
    let failFlush = false;
    const storage = memoryStorage(1024 * 1024, {
      flushError: (name) =>
        failFlush && name.startsWith('spill-')
          ? new DOMException('flush failed', 'InvalidStateError')
          : undefined,
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);
    host.append(writerId, new Uint8Array(8));
    failFlush = true;

    expect(() => host.finalizeWriter(writerId)).toThrowError(
      expect.objectContaining({ reason: 'io_failure' }),
    );
    expect(host.metrics()).toMatchObject({ activeBytes: 8, activeHandles: 1 });

    await host.releaseQueryScopes(query);
    expect(host.metrics()).toMatchObject({ activeBytes: 0, activeFiles: 0, activeHandles: 0 });
  });

  it('rejects a non-progressing physical read instead of busy-spinning', async () => {
    let zeroRead = false;
    const storage = memoryStorage(1024 * 1024, {
      readLength: (name, requested) => (zeroRead && name.startsWith('spill-') ? 0 : requested),
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const scope = await host.createScope();
    const { file, writerId } = await host.createFile(scope);
    host.append(writerId, new Uint8Array(8));
    host.finalizeWriter(writerId);
    const readerId = await host.openReader(file);
    zeroRead = true;

    expect(() => host.readNext(readerId, 8)).toThrowError(
      expect.objectContaining({ reason: 'io_failure' }),
    );
    await host.deleteScope(scope);
  });

  it('keeps failed scope deletion retryable without losing accounting', async () => {
    let failScopeDelete = true;
    const storage = memoryStorage(1024 * 1024, {
      beforeRemoveEntry: (name) => {
        if (failScopeDelete && !name.startsWith('spill-') && name !== 'scavenge-after') {
          failScopeDelete = false;
          throw new DOMException('delete failed', 'InvalidStateError');
        }
      },
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);
    host.append(writerId, new Uint8Array(8));
    host.finalizeWriter(writerId);

    await expect(host.releaseScope(scope)).rejects.toMatchObject({ reason: 'io_failure' });
    expect(host.metrics()).toMatchObject({ activeBytes: 8, activeFiles: 1, activeScopes: 1 });

    await host.releaseScope(scope);
    expect(host.metrics()).toMatchObject({ activeBytes: 0, activeFiles: 0, activeScopes: 0 });
  });

  it('continues query cleanup when individual handle closes fail', async () => {
    let failCloses = false;
    const storage = memoryStorage(1024 * 1024, {
      closeError: (name) =>
        failCloses && (name === 'lease' || name.startsWith('spill-'))
          ? new DOMException('close failed', 'InvalidStateError')
          : undefined,
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);
    host.append(writerId, new Uint8Array(8));
    failCloses = true;

    await host.releaseQueryScopes(query);
    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeFiles: 0,
      activeHandles: 0,
      activeScopes: 0,
    });
  });

  it('enforces the immutable 576 MiB product cap even when callers and quota allow more', async () => {
    const host = await OpfsSpillHost.open(memoryStorage(8 * 1024 * 1024 * 1024), {
      productionCapBytes: 4 * 1024 * 1024 * 1024,
    });

    expect(host.metrics().storageLimitBytes).toBe(576 * 1024 * 1024);
  });

  it('fails closed to the product cap when quota estimates are non-finite', async () => {
    const storage = memoryStorage();
    storage.estimate = async () => ({ quota: Number.POSITIVE_INFINITY, usage: Number.NaN });

    const host = await OpfsSpillHost.open(storage, {
      productionCapBytes: 4 * 1024 * 1024 * 1024,
    });

    expect(host.metrics().storageLimitBytes).toBe(576 * 1024 * 1024);
  });

  it('reports query-local spill counters and peak bytes without leaking prior runs', async () => {
    const host = await OpfsSpillHost.open(memoryStorage(), {
      productionCapBytes: 64 * 1024,
    });

    const firstStart = host.beginQueryMetrics();
    const firstScope = await host.createScope();
    const { writerId: firstWriter } = await host.createFile(firstScope);
    await host.append(firstWriter, new Uint8Array(12));
    await host.finalizeWriter(firstWriter);
    host.recordMergePass();
    await host.releaseQueryScopes(firstStart);
    expect(host.queryMetricsSince(firstStart)).toMatchObject({
      bytesWritten: 12,
      filesCreated: 1,
      peakActiveBytes: 12,
      activeBytes: 0,
      activeFiles: 0,
      mergePasses: 1,
      scopesDeleted: 1,
    });

    const secondStart = host.beginQueryMetrics();
    const secondScope = await host.createScope();
    const { writerId: secondWriter } = await host.createFile(secondScope);
    await host.append(secondWriter, new Uint8Array(4));
    await host.finalizeWriter(secondWriter);
    await host.releaseQueryScopes(secondStart);
    expect(host.queryMetricsSince(secondStart)).toMatchObject({
      bytesWritten: 4,
      filesCreated: 1,
      peakActiveBytes: 4,
      activeBytes: 0,
      activeFiles: 0,
      mergePasses: 0,
      scopesDeleted: 1,
    });
  });

  it('makes best-effort scope release idempotent for drop and cancellation cleanup', async () => {
    const host = await OpfsSpillHost.open(memoryStorage(), {
      productionCapBytes: 64 * 1024,
    });
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);
    await host.append(writerId, new Uint8Array(8));

    await host.releaseScope(scope);
    await host.releaseScope(scope);

    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
      scopesDeleted: 1,
    });
  });

  it('coalesces concurrent drop and terminal scope cleanup', async () => {
    const host = await OpfsSpillHost.open(memoryStorage(), {
      productionCapBytes: 64 * 1024,
    });
    const snapshot = host.beginQueryMetrics();
    const scope = await host.createScope();
    const { writerId } = await host.createFile(scope);
    await host.append(writerId, new Uint8Array(8));

    await Promise.all([host.releaseScope(scope), host.releaseQueryScopes(snapshot)]);

    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
      scopesDeleted: 1,
    });
  });

  it('closes a query generation before a pending scope acquisition can publish ownership', async () => {
    const leaseGate = deferred();
    const leaseEntered = deferred();
    let blockLease = false;
    const storage = memoryStorage(1024 * 1024, {
      beforeCreateSyncAccessHandle: async (name) => {
        if (blockLease && name === 'lease') {
          leaseEntered.resolve();
          await leaseGate.promise;
        }
      },
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    blockLease = true;

    const pendingScope = host.createScope();
    await leaseEntered.promise;
    const cleanup = host.releaseQueryScopes(query);
    leaseGate.resolve();

    await expect(pendingScope).rejects.toMatchObject({ reason: 'unavailable' });
    await cleanup;
    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    });
    const axon = storage.root.directories.get('axon-spill');
    expect(axon?.directories.get('v1')?.directories.size).toBe(0);
  });

  it('removes a scope namespace when lease acquisition fails', async () => {
    let failLease = false;
    const storage = memoryStorage(1024 * 1024, {
      beforeCreateSyncAccessHandle: (name) => {
        if (failLease && name === 'lease') {
          throw new DOMException('lease failed', 'UnknownError');
        }
      },
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    failLease = true;

    await expect(host.createScope()).rejects.toMatchObject({ reason: 'unavailable' });
    const axon = storage.root.directories.get('axon-spill');
    expect(axon?.directories.get('v1')?.directories.size).toBe(0);
  });

  it('closes late file handles acquired after query cleanup starts', async () => {
    const fileGate = deferred();
    const fileEntered = deferred();
    let blockSpillFile = false;
    const storage = memoryStorage(1024 * 1024, {
      beforeCreateSyncAccessHandle: async (name) => {
        if (blockSpillFile && name.startsWith('spill-')) {
          fileEntered.resolve();
          await fileGate.promise;
        }
      },
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    const scope = await host.createScope();
    blockSpillFile = true;

    const pendingFile = host.createFile(scope);
    await fileEntered.promise;
    const cleanup = host.releaseQueryScopes(query);
    fileGate.resolve();

    await expect(pendingFile).rejects.toMatchObject({ reason: 'unavailable' });
    await cleanup;
    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    });
  });

  it('closes a late reader acquired after query cleanup starts', async () => {
    const readerGate = deferred();
    const readerEntered = deferred();
    let blockReader = false;
    const storage = memoryStorage(1024 * 1024, {
      beforeCreateSyncAccessHandle: async (name) => {
        if (blockReader && name.startsWith('spill-')) {
          readerEntered.resolve();
          await readerGate.promise;
        }
      },
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    const scope = await host.createScope();
    const { file, writerId } = await host.createFile(scope);
    await host.finalizeWriter(writerId);
    blockReader = true;

    const pendingReader = host.openReader(file);
    await readerEntered.promise;
    const cleanup = host.releaseQueryScopes(query);
    readerGate.resolve();

    await expect(pendingReader).rejects.toMatchObject({ reason: 'unavailable' });
    await cleanup;
    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    });
  });

  it('waits for pending storage deletion before closing a query generation', async () => {
    const deleteGate = deferred();
    const deleteEntered = deferred();
    let blockDelete = false;
    const storage = memoryStorage(1024 * 1024, {
      beforeRemoveEntry: async (name) => {
        if (blockDelete && name.startsWith('spill-')) {
          deleteEntered.resolve();
          await deleteGate.promise;
        }
      },
    });
    const host = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    const query = host.beginQueryMetrics();
    const scope = await host.createScope();
    const { file, writerId } = await host.createFile(scope);
    host.append(writerId, new Uint8Array(8));
    await host.finalizeWriter(writerId);
    blockDelete = true;

    const deletion = host.deleteFile(file);
    await deleteEntered.promise;
    let cleanupSettled = false;
    const cleanup = host.releaseQueryScopes(query).then(() => {
      cleanupSettled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(cleanupSettled).toBe(false);
    deleteGate.resolve();
    await deletion;
    await cleanup;
    expect(host.metrics()).toMatchObject({
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    });
  });

  it('never scavenges a live leased scope from another host', async () => {
    const storage = memoryStorage();
    const liveHost = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    await liveHost.createScope();

    const scavenger = await OpfsSpillHost.open(storage, {
      productionCapBytes: 64 * 1024,
      nowMs: Date.now() + 2 * 60 * 60 * 1000,
    });

    expect(liveHost.metrics().activeScopes).toBe(1);
    expect(scavenger.metrics().abandonedScopesDeleted).toBe(0);
  });

  it('quarantines a new lease-less scope before scavenging it after the age threshold', async () => {
    const storage = memoryStorage();
    const root = await storage.getDirectory();
    const axon = await root.getDirectoryHandle('axon-spill', { create: true });
    const v1 = await axon.getDirectoryHandle('v1', { create: true });
    await v1.getDirectoryHandle('interrupted-scope', { create: true });

    const firstSweep = await OpfsSpillHost.open(storage, {
      productionCapBytes: 64 * 1024,
      nowMs: Date.now(),
    });

    expect(firstSweep.metrics().abandonedScopesDeleted).toBe(0);
    expect((v1 as MemoryDirectory).directories.has('interrupted-scope')).toBe(true);

    const secondSweep = await OpfsSpillHost.open(storage, {
      productionCapBytes: 64 * 1024,
      nowMs: Date.now() + 2 * 60 * 60 * 1000,
    });

    expect(secondSweep.metrics().abandonedScopesDeleted).toBe(1);
    expect((v1 as MemoryDirectory).directories.has('interrupted-scope')).toBe(false);
  });

  it('does not scavenge a scope concurrently creating its lease', async () => {
    const leaseGate = deferred();
    const leaseEntered = deferred();
    let blockLeaseLookup = false;
    const storage = memoryStorage(1024 * 1024, {
      beforeGetFileHandle: async (name) => {
        if (blockLeaseLookup && name === 'lease') {
          leaseEntered.resolve();
          await leaseGate.promise;
        }
      },
    });
    const creator = await OpfsSpillHost.open(storage, { productionCapBytes: 64 * 1024 });
    blockLeaseLookup = true;
    const pendingScope = creator.createScope();
    await leaseEntered.promise;
    blockLeaseLookup = false;

    const scavenger = await OpfsSpillHost.open(storage, {
      productionCapBytes: 64 * 1024,
      nowMs: Date.now() + 2 * 60 * 60 * 1000,
    });
    leaseGate.resolve();
    const scope = await pendingScope;

    expect(scavenger.metrics().abandonedScopesDeleted).toBe(0);
    expect(creator.metrics().activeScopes).toBe(1);
    await creator.deleteScope(scope);
  });
});

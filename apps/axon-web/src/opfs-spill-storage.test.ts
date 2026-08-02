import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  BROWSER_SPILL_CORPUS_MAX_OBSERVED_BYTES,
  BROWSER_SPILL_PRODUCTION_CAP_BYTES,
  applyBrowserExternalMemoryTelemetry,
  beginOpfsSpillExecution,
  deriveProductionSpillCap,
  deriveRuntimeSpillLimit,
  probeBrowserExternalMemory,
  spillErrorName,
  SPILL_ERROR_UNAVAILABLE,
  sweepStaleOpfsSpillNamespaces,
  unregisterOpfsSpillExecution,
  type OpfsDirectory,
  type OpfsFile,
  type OpfsSyncAccessHandle,
} from './opfs-spill-storage.ts';

class FakeAccessHandle implements OpfsSyncAccessHandle {
  closed = false;

  constructor(readonly file: FakeFile) {}

  write(source: Uint8Array<ArrayBuffer>, options?: { at?: number }): number {
    if (this.file.writeError) throw this.file.writeError;
    const at = options?.at ?? 0;
    const next = new Uint8Array(Math.max(this.file.bytes.length, at + source.length));
    next.set(this.file.bytes);
    next.set(source, at);
    this.file.bytes = next;
    return source.length;
  }

  read(target: Uint8Array<ArrayBuffer>, options?: { at?: number }): number {
    if (this.file.readError) throw this.file.readError;
    const at = options?.at ?? 0;
    const available = this.file.bytes.subarray(at, at + target.length);
    target.set(available);
    return available.length;
  }

  flush(): void {
    if (this.file.flushError) throw this.file.flushError;
  }

  truncate(size: number): void {
    if (this.file.truncateError) throw this.file.truncateError;
    this.file.bytes = this.file.bytes.slice(0, size);
  }

  close(): void {
    if (this.file.closeError) throw this.file.closeError;
    this.closed = true;
  }
}

class FakeFile implements OpfsFile {
  bytes = new Uint8Array();
  readonly handles: FakeAccessHandle[] = [];
  flushError: Error | undefined;
  writeError: Error | undefined;
  readError: Error | undefined;
  truncateError: Error | undefined;
  closeError: Error | undefined;
  deferredAccessHandle:
    | {
        promise: Promise<OpfsSyncAccessHandle>;
        resolve: (handle: OpfsSyncAccessHandle) => void;
      }
    | undefined;

  deferNextAccessHandle(): void {
    let resolve!: (handle: OpfsSyncAccessHandle) => void;
    const promise = new Promise<OpfsSyncAccessHandle>((accept) => {
      resolve = accept;
    });
    this.deferredAccessHandle = { promise, resolve };
  }

  async createSyncAccessHandle(): Promise<OpfsSyncAccessHandle> {
    if (this.deferredAccessHandle) {
      return this.deferredAccessHandle.promise;
    }
    if (this.handles.some((handle) => !handle.closed)) {
      throw new DOMException('file is locked', 'NoModificationAllowedError');
    }
    const handle = new FakeAccessHandle(this);
    this.handles.push(handle);
    return handle;
  }
}

class FakeDirectory implements OpfsDirectory {
  readonly kind = 'directory' as const;
  readonly directories = new Map<string, FakeDirectory>();
  readonly files = new Map<string, FakeFile>();
  readonly removed: string[] = [];
  getFileError: Error | undefined;
  removeError: Error | undefined;

  constructor(readonly fileFactory: (name: string) => FakeFile = () => new FakeFile()) {}

  async getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<OpfsDirectory> {
    const existing = this.directories.get(name);
    if (existing) return existing;
    if (!options?.create) throw new Error('directory missing');
    const directory = new FakeDirectory(this.fileFactory);
    this.directories.set(name, directory);
    return directory;
  }

  async getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFile> {
    if (this.getFileError) throw this.getFileError;
    const existing = this.files.get(name);
    if (existing) return existing;
    if (!options?.create) throw new Error('file missing');
    const file = this.fileFactory(name);
    this.files.set(name, file);
    return file;
  }

  async removeEntry(name: string, options?: { recursive?: boolean }): Promise<void> {
    if (this.removeError) throw this.removeError;
    if (options?.recursive) this.directories.delete(name);
    else this.files.delete(name);
    this.removed.push(name);
  }

  async *entries(): AsyncIterableIterator<[string, OpfsDirectory | OpfsFile]> {
    yield* this.directories;
    yield* this.files;
  }
}

async function completedOperation(
  execution: Awaited<ReturnType<typeof beginOpfsSpillExecution>>,
  operationId: number,
): Promise<[number, number]> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    await Promise.resolve();
    const status = execution.operationStatus(operationId);
    if (status === 1) {
      return execution.takeOperationResult(operationId);
    }
    if (status === 2) {
      throw new Error(`operation failed with kind ${execution.operationErrorKind(operationId)}`);
    }
  }
  throw new Error('operation did not complete');
}

async function failedOperationKind(
  execution: Awaited<ReturnType<typeof beginOpfsSpillExecution>>,
  operationId: number,
): Promise<number> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    await Promise.resolve();
    const status = execution.operationStatus(operationId);
    if (status === 2) return execution.operationErrorKind(operationId);
    if (status === 1) throw new Error('operation unexpectedly succeeded');
  }
  throw new Error('operation did not complete');
}

describe('browser OPFS spill storage', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('reduces cleanup failures to a non-sensitive error class', () => {
    const error = new Error('signed_url=https://example.test/?token=secret');
    expect(spillErrorName(error)).toBe('Error');
    expect(JSON.stringify({ error_name: spillErrorName(error) })).not.toContain('secret');
  });

  it('probes write, flush, read, truncate, close, and delete behavior', async () => {
    const root = new FakeDirectory();
    await expect(
      probeBrowserExternalMemory({
        getRoot: async () => root,
        probeBytes: 64,
        randomId: () => 'opaque-probe',
      }),
    ).resolves.toEqual({ state: 'supported' });
    expect(root.directories.get('axon-spill')?.directories.get('v1')?.removed).toContain(
      'opaque-probe',
    );
  });

  it('reports a sanitized operation and error name when the real probe is unavailable', async () => {
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    await expect(
      probeBrowserExternalMemory({
        getRoot: async () => {
          throw new DOMException('private details must not be logged', 'NotSupportedError');
        },
        randomId: () => 'sensitive-probe-id',
      }),
    ).resolves.toEqual({ state: 'unsupported', reason: 'unavailable' });

    expect(warning).toHaveBeenCalledWith('[axon] OPFS capability probe failed', {
      operation: 'get_root',
      error_name: 'NotSupportedError',
    });
    expect(JSON.stringify(warning.mock.calls)).not.toContain('private details');
    expect(JSON.stringify(warning.mock.calls)).not.toContain('sensitive-probe-id');
  });

  it('externalizes a file behind numeric ids and cleans the complete execution namespace', async () => {
    const root = new FakeDirectory();
    const woken: number[] = [];
    const execution = await beginOpfsSpillExecution({
      getRoot: async () => root,
      maxBytes: 64,
      randomId: () => 'opaque-execution',
      wakeOperation: (operationId) => woken.push(operationId),
    });

    const [scopeId] = await completedOperation(execution, execution.startCreateScope());
    const [fileId, writerId] = await completedOperation(
      execution,
      execution.startCreateWriter(scopeId),
    );
    expect([scopeId, fileId, writerId].every(Number.isSafeInteger)).toBe(true);

    expect(execution.write(writerId, new Uint8Array([1, 2, 3, 4]), 0)).toBe(4);
    execution.flush(writerId);
    execution.close(writerId);

    const [readerId] = await completedOperation(execution, execution.startOpenReader(fileId));
    const output = new Uint8Array(4);
    expect(execution.read(readerId, output, 0)).toBe(4);
    expect([...output]).toEqual([1, 2, 3, 4]);
    execution.close(readerId);

    await completedOperation(execution, execution.startDeleteFile(fileId));
    expect(execution.accounting()).toMatchObject({
      bytes_written: 4,
      bytes_read: 4,
      active_bytes: 0,
      active_handles: 1,
    });
    expect(woken.length).toBeGreaterThanOrEqual(4);

    await execution.finish();
    expect(execution.accounting().active_handles).toBe(0);
    expect(root.directories.get('axon-spill')?.directories.get('v1')?.removed).toContain(
      'opaque-execution',
    );
  });

  it('makes quota failure authoritative without breaking execution startup', async () => {
    const execution = await beginOpfsSpillExecution({
      getRoot: async () => new FakeDirectory(),
      maxBytes: 4,
      randomId: () => 'opaque-execution',
      wakeOperation: () => undefined,
    });
    const [scopeId] = await completedOperation(execution, execution.startCreateScope());
    const [, writerId] = await completedOperation(execution, execution.startCreateWriter(scopeId));

    expect(() => execution.write(writerId, new Uint8Array(5), 0)).toThrow(
      'spill storage quota exceeded',
    );
    expect(execution.accounting()).toMatchObject({
      error_operation: 'write',
      error_name: 'QuotaExceededError',
    });
    await execution.finish();
  });

  it('classifies create, write, flush, read, and delete failures without identifiers', async () => {
    const root = new FakeDirectory();
    const execution = await beginOpfsSpillExecution({
      getRoot: async () => root,
      maxBytes: 64,
      randomId: () => 'opaque-execution',
      wakeOperation: () => undefined,
    });
    const [scopeId] = await completedOperation(execution, execution.startCreateScope());
    const executionDirectory = root.directories
      .get('axon-spill')
      ?.directories.get('v1')
      ?.directories.get('opaque-execution');
    if (!executionDirectory) throw new Error('execution directory missing');

    executionDirectory.getFileError = new DOMException('secret create detail', 'UnknownError');
    await expect(
      failedOperationKind(execution, execution.startCreateWriter(scopeId)),
    ).resolves.toBe(32);
    executionDirectory.getFileError = undefined;

    const [fileId, writerId] = await completedOperation(
      execution,
      execution.startCreateWriter(scopeId),
    );
    const file = executionDirectory.files.get('spill-2');
    if (!file) throw new Error('spill file missing');

    file.writeError = new DOMException('secret write detail', 'UnknownError');
    expect(() => execution.write(writerId, new Uint8Array([1]), 0)).toThrow(
      'spill_storage/io_failure/write/UnknownError',
    );
    file.writeError = undefined;
    execution.write(writerId, new Uint8Array([1]), 0);

    file.flushError = new DOMException('secret flush detail', 'UnknownError');
    expect(() => execution.flush(writerId)).toThrow('spill_storage/io_failure/flush/UnknownError');
    file.flushError = undefined;
    execution.close(writerId);

    const [readerId] = await completedOperation(execution, execution.startOpenReader(fileId));
    file.readError = new DOMException('secret read detail', 'UnknownError');
    expect(() => execution.read(readerId, new Uint8Array(1), 0)).toThrow(
      'spill_storage/io_failure/read/UnknownError',
    );
    file.readError = undefined;
    execution.close(readerId);

    executionDirectory.removeError = new DOMException('secret delete detail', 'UnknownError');
    await expect(failedOperationKind(execution, execution.startDeleteFile(fileId))).resolves.toBe(
      34,
    );
    expect(execution.accounting()).toMatchObject({
      error_operation: 'delete_file',
      error_name: 'UnknownError',
    });
    expect(JSON.stringify(execution.accounting())).not.toContain('secret');
    executionDirectory.removeError = undefined;
    await execution.finish();
  });

  it('closes a writer handle that resolves after execution cleanup without recreating operation state', async () => {
    const root = new FakeDirectory((name) => {
      const file = new FakeFile();
      if (name === 'spill-1') file.deferNextAccessHandle();
      return file;
    });
    const execution = await beginOpfsSpillExecution({
      getRoot: async () => root,
      maxBytes: 64,
      randomId: () => 'late-writer',
      wakeOperation: () => undefined,
    });
    const [scopeId] = await completedOperation(execution, execution.startCreateScope());
    const operationId = execution.startCreateWriter(scopeId);
    await Promise.resolve();
    const executionDirectory = root.directories
      .get('axon-spill')
      ?.directories.get('v1')
      ?.directories.get('late-writer');
    const file = executionDirectory?.files.get('spill-1');
    if (!file?.deferredAccessHandle) throw new Error('writer handle was not deferred');

    const finishPromise = execution.finish();
    await Promise.resolve();
    expect(execution.operationErrorKind(operationId)).toBe(SPILL_ERROR_UNAVAILABLE);
    expect(
      root.directories.get('axon-spill')?.directories.get('v1')?.directories.has('late-writer'),
    ).toBe(true);

    const lateHandle = new FakeAccessHandle(file);
    file.deferredAccessHandle.resolve(lateHandle);
    await finishPromise;
    execution.releaseOperation(operationId);

    expect(lateHandle.closed).toBe(true);
    expect(execution.accounting()).toMatchObject({ active_files: 0, active_handles: 0 });
    expect(() => execution.operationStatus(operationId)).toThrow('unknown spill operation');
  });

  it('closes a reader handle that resolves after execution cleanup', async () => {
    const root = new FakeDirectory();
    const execution = await beginOpfsSpillExecution({
      getRoot: async () => root,
      maxBytes: 64,
      randomId: () => 'late-reader',
      wakeOperation: () => undefined,
    });
    const [scopeId] = await completedOperation(execution, execution.startCreateScope());
    const [fileId, writerId] = await completedOperation(
      execution,
      execution.startCreateWriter(scopeId),
    );
    execution.write(writerId, new Uint8Array([1]), 0);
    execution.close(writerId);
    const executionDirectory = root.directories
      .get('axon-spill')
      ?.directories.get('v1')
      ?.directories.get('late-reader');
    const file = executionDirectory?.files.get('spill-1');
    if (!file) throw new Error('spill file missing');
    file.deferNextAccessHandle();
    const operationId = execution.startOpenReader(fileId);
    await Promise.resolve();

    const finishPromise = execution.finish();
    await Promise.resolve();
    expect(execution.operationErrorKind(operationId)).toBe(SPILL_ERROR_UNAVAILABLE);
    const deferred = file.deferredAccessHandle;
    if (!deferred) throw new Error('reader handle was not deferred');
    const lateHandle = new FakeAccessHandle(file);
    deferred.resolve(lateHandle);
    await finishPromise;
    execution.releaseOperation(operationId);

    expect(lateHandle.closed).toBe(true);
    expect(execution.accounting()).toMatchObject({ active_files: 0, active_handles: 0 });
  });

  it('leaves no handles or namespaces after repeated cancellation during writer acquisition', async () => {
    const root = new FakeDirectory((name) => {
      const file = new FakeFile();
      if (name.startsWith('spill-')) file.deferNextAccessHandle();
      return file;
    });

    for (let iteration = 0; iteration < 10; iteration += 1) {
      const namespace = `cancelled-${iteration}`;
      const execution = await beginOpfsSpillExecution({
        getRoot: async () => root,
        maxBytes: 64,
        randomId: () => namespace,
        wakeOperation: () => undefined,
      });
      const [scopeId] = await completedOperation(execution, execution.startCreateScope());
      const operationId = execution.startCreateWriter(scopeId);
      await Promise.resolve();
      const version = root.directories.get('axon-spill')?.directories.get('v1');
      const file = version?.directories.get(namespace)?.files.get('spill-1');
      const deferred = file?.deferredAccessHandle;
      if (!file || !deferred) throw new Error('writer handle was not deferred');

      const finishPromise = execution.finish();
      const lateHandle = new FakeAccessHandle(file);
      deferred.resolve(lateHandle);
      await finishPromise;
      execution.releaseOperation(operationId);

      expect(lateHandle.closed).toBe(true);
      expect(execution.accounting()).toMatchObject({ active_files: 0, active_handles: 0 });
      expect(version?.directories.has(namespace)).toBe(false);
      unregisterOpfsSpillExecution(execution.id);
    }
  });

  it('closes the lease and removes the namespace when setup fails', async () => {
    const root = new FakeDirectory((name) => {
      const file = new FakeFile();
      if (name === 'lease') file.flushError = new Error('injected lease flush failure');
      return file;
    });

    await expect(
      beginOpfsSpillExecution({
        getRoot: async () => root,
        maxBytes: 64,
        randomId: () => 'failed-setup',
        wakeOperation: () => undefined,
      }),
    ).rejects.toThrow('injected lease flush failure');

    const version = root.directories.get('axon-spill')?.directories.get('v1');
    expect(version?.directories.has('failed-setup')).toBe(false);
    expect(version?.removed).toContain('failed-setup');
  });

  it('derives the production and runtime caps without trusting quota estimates', () => {
    const mib = 1024 * 1024;
    expect(deriveProductionSpillCap(100 * mib)).toBe(128 * mib);
    expect(deriveProductionSpillCap(129 * mib)).toBe(192 * mib);
    expect(BROWSER_SPILL_CORPUS_MAX_OBSERVED_BYTES).toBe(459_408_216);
    expect(BROWSER_SPILL_PRODUCTION_CAP_BYTES).toBe(576 * mib);
    expect(
      deriveRuntimeSpillLimit(256 * mib, {
        quota: 800 * mib,
        usage: 400 * mib,
      }),
    ).toBe(200 * mib);
    expect(deriveRuntimeSpillLimit(256 * mib, {})).toBe(256 * mib);
  });

  it('reports capability and aggregate-only spill telemetry without identifiers', () => {
    const metadata = {
      response: {
        capabilities: { capabilities: {} },
        metrics: {},
      },
    };
    applyBrowserExternalMemoryTelemetry(
      metadata,
      { state: 'supported' },
      {
        storage_limit_bytes: 256,
        bytes_written: 64,
        bytes_read: 32,
        files_created: 2,
        active_bytes: 0,
        peak_active_bytes: 48,
        active_files: 0,
        active_handles: 1,
        files_deleted: 2,
        scopes_deleted: 1,
        merge_passes: 3,
      },
      128,
      96,
    );
    expect(metadata).toEqual({
      response: {
        capabilities: { capabilities: { browser_external_memory: 'supported' } },
        metrics: {
          spill_backend: 'opfs',
          spill_working_set_limit_bytes: 128,
          spill_peak_reservation_bytes: 96,
          spill_storage_limit_bytes: 256,
          spill_bytes_written: 64,
          spill_bytes_read: 32,
          spill_files_created: 2,
          spill_peak_active_bytes: 48,
          spill_active_files: 0,
          spill_merge_passes: 3,
          spill_cleanup_count: 1,
          spill_abandoned_cleanup_count: 0,
          spill_cleanup_files: 2,
          spill_cleanup_scopes: 1,
        },
      },
    });
    expect(JSON.stringify(metadata)).not.toContain('opaque');
  });

  it('sweeps only stale namespaces whose exclusive lease is no longer held', async () => {
    const root = new FakeDirectory();
    const active = await beginOpfsSpillExecution({
      getRoot: async () => root,
      maxBytes: 64,
      randomId: () => 'active-scope',
      nowMs: () => 0,
      wakeOperation: () => undefined,
    });
    const orphan = await beginOpfsSpillExecution({
      getRoot: async () => root,
      maxBytes: 64,
      randomId: () => 'orphan-scope',
      nowMs: () => 0,
      wakeOperation: () => undefined,
    });
    const version = root.directories.get('axon-spill')?.directories.get('v1');
    const orphanLease = version?.directories.get('orphan-scope')?.files.get('lease');
    orphanLease?.handles.at(-1)?.close();

    await expect(
      sweepStaleOpfsSpillNamespaces({
        getRoot: async () => root,
        nowMs: () => 60 * 60 * 1000 + 1,
      }),
    ).resolves.toBe(1);
    expect(version?.directories.has('active-scope')).toBe(true);
    expect(version?.directories.has('orphan-scope')).toBe(false);

    await active.finish();
    unregisterOpfsSpillExecution(orphan.id);
  });
});

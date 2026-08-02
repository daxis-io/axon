import type { PrivateExternalMemoryMetrics } from './sandbox-query-stream-protocol.ts';

export type BrowserExternalMemoryCapability =
  | { state: 'supported' }
  | { state: 'unsupported'; reason: 'unavailable' };

export interface OpfsSyncAccessHandle {
  write(source: Uint8Array<ArrayBuffer>, options?: { at?: number }): number;
  read(target: Uint8Array<ArrayBuffer>, options?: { at?: number }): number;
  flush(): void;
  truncate(size: number): void;
  close(): void;
}

export interface OpfsFile {
  createSyncAccessHandle(): Promise<OpfsSyncAccessHandle>;
}

export interface OpfsDirectory {
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<OpfsDirectory>;
  getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFile>;
  removeEntry(name: string, options?: { recursive?: boolean }): Promise<void>;
  entries?(): AsyncIterableIterator<[string, OpfsDirectory | OpfsFile]>;
}

type ProbeOptions = {
  getRoot?: () => Promise<OpfsDirectory>;
  probeBytes?: number;
  randomId?: () => string;
};

type BeginOptions = {
  getRoot?: () => Promise<OpfsDirectory>;
  maxBytes: number;
  randomId?: () => string;
  nowMs?: () => number;
  wakeOperation: (operationId: number) => void;
};

type SweepOptions = {
  getRoot?: () => Promise<OpfsDirectory>;
  nowMs?: () => number;
  staleAfterMs?: number;
};

export type OpfsSpillAccounting = {
  storage_limit_bytes: number;
  bytes_written: number;
  bytes_read: number;
  files_created: number;
  active_bytes: number;
  peak_active_bytes: number;
  active_files: number;
  active_handles: number;
  files_deleted: number;
  scopes_deleted: number;
  abandoned_scopes: number;
  merge_passes: number;
  error_operation?: string;
  error_name?: string;
};

export type StorageEstimate = {
  quota?: number;
  usage?: number;
};

type OperationState =
  | { status: 0 }
  | { status: 1; first: number; second: number }
  | { status: 2; errorKind: number };

type SpillFileState = {
  readonly id: number;
  readonly name: string;
  readonly file: OpfsFile;
  size: number;
};

type SpillOperationName =
  | 'create_scope'
  | 'create_writer'
  | 'open_reader'
  | 'delete_file'
  | 'delete_scope';

type RegisteredHandle = {
  readonly handle: OpfsSyncAccessHandle;
  readonly fileId?: number;
};

const DEFAULT_PROBE_BYTES = 4 * 1024 * 1024;
const DEFAULT_STALE_SCOPE_AGE_MS = 60 * 60 * 1000;
const OPERATION_PENDING = 0;
const OPERATION_SUCCEEDED = 1;
const OPERATION_FAILED = 2;
export const SPILL_ERROR_UNAVAILABLE = 1;
export const SPILL_ERROR_QUOTA_EXCEEDED = 2;
export const SPILL_ERROR_IO_FAILURE = 3;

let nextExecutionId = 1;
const executions = new Map<number, OpfsSpillExecution>();

/**
 * Owns one query's OPFS namespace. Browser objects remain in this worker-local
 * object; Rust sees only the numeric ids returned by its methods.
 */
export class OpfsSpillExecution {
  readonly id: number;
  readonly #versionDirectory: OpfsDirectory;
  readonly #executionDirectory: OpfsDirectory;
  readonly #namespace: string;
  readonly #maxBytes: number;
  readonly #wakeOperation: (operationId: number) => void;
  readonly #files = new Map<number, SpillFileState>();
  readonly #handles = new Map<number, RegisteredHandle>();
  readonly #operations = new Map<number, OperationState>();
  readonly #pendingOperationTasks = new Set<Promise<void>>();
  #nextFileId = 1;
  #nextHandleId = 1;
  #nextOperationId = 1;
  #leaseHandleId: number;
  #bytesWritten = 0;
  #bytesRead = 0;
  #filesCreated = 0;
  #activeBytes = 0;
  #peakActiveBytes = 0;
  #filesDeleted = 0;
  #scopesDeleted = 0;
  #abandonedScopes = 0;
  #mergePasses = 0;
  #errorOperation: string | undefined;
  #errorName: string | undefined;
  #finished = false;
  #finishPromise: Promise<void> | undefined;

  constructor(
    versionDirectory: OpfsDirectory,
    executionDirectory: OpfsDirectory,
    namespace: string,
    lease: OpfsSyncAccessHandle,
    maxBytes: number,
    wakeOperation: (operationId: number) => void,
  ) {
    this.id = nextExecutionId;
    nextExecutionId += 1;
    this.#versionDirectory = versionDirectory;
    this.#executionDirectory = executionDirectory;
    this.#namespace = namespace;
    this.#maxBytes = maxBytes;
    this.#wakeOperation = wakeOperation;
    this.#leaseHandleId = this.#registerHandle({ handle: lease });
  }

  startCreateScope(): number {
    return this.#startOperation('create_scope', async () => [1, 0]);
  }

  startCreateWriter(scopeId: number): number {
    return this.#startOperation('create_writer', async () => {
      this.#requireScope(scopeId);
      const fileId = this.#nextFileId;
      this.#nextFileId += 1;
      const name = `spill-${fileId.toString(16)}`;
      const file = await this.#executionDirectory.getFileHandle(name, { create: true });
      if (this.#finished) {
        await this.#removeLateFile(name);
        throw new Error('spill execution is closed');
      }
      const handle = await file.createSyncAccessHandle();
      if (this.#finished) {
        closeHandleBestEffort(handle);
        await this.#removeLateFile(name);
        throw new Error('spill execution is closed');
      }
      this.#files.set(fileId, { id: fileId, name, file, size: 0 });
      this.#filesCreated += 1;
      const handleId = this.#registerHandle({ handle, fileId });
      return [fileId, handleId];
    });
  }

  startOpenReader(fileId: number): number {
    return this.#startOperation('open_reader', async () => {
      const file = this.#requireFile(fileId);
      const handle = await file.file.createSyncAccessHandle();
      if (this.#finished) {
        closeHandleBestEffort(handle);
        throw new Error('spill execution is closed');
      }
      return [this.#registerHandle({ handle, fileId }), 0];
    });
  }

  startDeleteFile(fileId: number): number {
    return this.#startOperation('delete_file', async () => {
      const file = this.#requireFile(fileId);
      if ([...this.#handles.values()].some((registered) => registered.fileId === fileId)) {
        throw new Error('spill file still has an open handle');
      }
      await this.#executionDirectory.removeEntry(file.name);
      this.#files.delete(fileId);
      this.#activeBytes -= file.size;
      this.#filesDeleted += 1;
      return [0, 0];
    });
  }

  startDeleteScope(scopeId: number): number {
    return this.#startOperation('delete_scope', async () => {
      this.#requireScope(scopeId);
      await this.#closeDataHandles();
      for (const file of this.#files.values()) {
        await this.#executionDirectory.removeEntry(file.name);
        this.#filesDeleted += 1;
      }
      this.#files.clear();
      this.#activeBytes = 0;
      this.#scopesDeleted += 1;
      return [0, 0];
    });
  }

  operationStatus(operationId: number): number {
    return this.#requireOperation(operationId).status;
  }

  takeOperationResult(operationId: number): [number, number] {
    const operation = this.#requireOperation(operationId);
    if (operation.status !== OPERATION_SUCCEEDED) {
      throw new Error('spill operation has no successful result');
    }
    this.#operations.delete(operationId);
    return [operation.first, operation.second];
  }

  operationResultFirst(operationId: number): number {
    const operation = this.#requireOperation(operationId);
    if (operation.status !== OPERATION_SUCCEEDED) {
      throw new Error('spill operation has no successful result');
    }
    return operation.first;
  }

  operationResultSecond(operationId: number): number {
    const operation = this.#requireOperation(operationId);
    if (operation.status !== OPERATION_SUCCEEDED) {
      throw new Error('spill operation has no successful result');
    }
    return operation.second;
  }

  operationErrorKind(operationId: number): number {
    const operation = this.#requireOperation(operationId);
    if (operation.status !== OPERATION_FAILED) {
      throw new Error('spill operation has no error');
    }
    return operation.errorKind;
  }

  releaseOperation(operationId: number): void {
    this.#operations.delete(operationId);
  }

  write(handleId: number, bytes: Uint8Array<ArrayBuffer>, at: number): number {
    const registered = this.#requireHandle(handleId);
    const file = this.#requireFileId(registered.fileId);
    const requestedEnd = checkedEnd(at, bytes.byteLength, 'spill write');
    const growth = Math.max(0, requestedEnd - file.size);
    if (this.#activeBytes + growth > this.#maxBytes) {
      const error = new DOMException('spill storage quota exceeded', 'QuotaExceededError');
      this.#recordIoError('write', error);
      throw error;
    }
    let written: number;
    try {
      written = registered.handle.write(bytes, { at });
    } catch (error) {
      this.#recordIoError('write', error);
      throw normalizeSyncIoError(error, 'write');
    }
    requireIoLength(written, bytes.byteLength, 'write');
    const nextSize = Math.max(file.size, at + written);
    this.#activeBytes += nextSize - file.size;
    file.size = nextSize;
    this.#bytesWritten += written;
    this.#peakActiveBytes = Math.max(this.#peakActiveBytes, this.#activeBytes);
    return written;
  }

  read(handleId: number, bytes: Uint8Array<ArrayBuffer>, at: number): number {
    const registered = this.#requireHandle(handleId);
    this.#requireFileId(registered.fileId);
    checkedEnd(at, bytes.byteLength, 'spill read');
    let read: number;
    try {
      read = registered.handle.read(bytes, { at });
    } catch (error) {
      this.#recordIoError('read', error);
      throw normalizeSyncIoError(error, 'read');
    }
    requireIoLength(read, bytes.byteLength, 'read');
    this.#bytesRead += read;
    return read;
  }

  flush(handleId: number): void {
    try {
      this.#requireHandle(handleId).handle.flush();
    } catch (error) {
      this.#recordIoError('flush', error);
      throw normalizeSyncIoError(error, 'flush');
    }
  }

  close(handleId: number): void {
    const registered = this.#requireHandle(handleId);
    try {
      registered.handle.close();
    } catch (error) {
      this.#recordIoError('close', error);
      throw normalizeSyncIoError(error, 'close');
    } finally {
      this.#handles.delete(handleId);
    }
  }

  recordMergePass(): void {
    if (this.#finished) throw new Error('spill execution is closed');
    this.#mergePasses += 1;
  }

  accounting(): OpfsSpillAccounting {
    return {
      storage_limit_bytes: this.#maxBytes,
      bytes_written: this.#bytesWritten,
      bytes_read: this.#bytesRead,
      files_created: this.#filesCreated,
      active_bytes: this.#activeBytes,
      peak_active_bytes: this.#peakActiveBytes,
      active_files: this.#files.size,
      active_handles: this.#handles.size,
      files_deleted: this.#filesDeleted,
      scopes_deleted: this.#scopesDeleted,
      abandoned_scopes: this.#abandonedScopes,
      merge_passes: this.#mergePasses,
      error_operation: this.#errorOperation,
      error_name: this.#errorName,
    };
  }

  finish(): Promise<void> {
    this.#finishPromise ??= this.#finish();
    return this.#finishPromise;
  }

  async #finish(): Promise<void> {
    this.#finished = true;
    const pendingOperationIds: number[] = [];
    for (const [operationId, operation] of this.#operations) {
      if (operation.status !== OPERATION_PENDING) continue;
      this.#operations.set(operationId, {
        status: OPERATION_FAILED,
        errorKind: SPILL_ERROR_UNAVAILABLE,
      });
      pendingOperationIds.push(operationId);
    }
    for (const operationId of pendingOperationIds) this.#wakeOperation(operationId);
    await Promise.allSettled([...this.#pendingOperationTasks]);
    await this.#closeDataHandles();
    if (this.#handles.has(this.#leaseHandleId)) this.close(this.#leaseHandleId);
    try {
      await this.#versionDirectory.removeEntry(this.#namespace, { recursive: true });
    } catch (error) {
      this.#recordIoError('delete_scope', error);
      this.#abandonedScopes = 1;
      throw normalizeSyncIoError(error, 'delete_scope');
    }
    this.#filesDeleted += this.#files.size;
    this.#scopesDeleted = 1;
    this.#files.clear();
    this.#activeBytes = 0;
  }

  #startOperation(
    operationName: SpillOperationName,
    operation: () => Promise<[number, number]>,
  ): number {
    if (this.#finished) throw new Error('spill execution is closed');
    const operationId = this.#nextOperationId;
    this.#nextOperationId += 1;
    this.#operations.set(operationId, { status: OPERATION_PENDING });
    const task = operation().then(
      ([first, second]) => {
        if (this.#finished || !this.#operations.has(operationId)) return;
        this.#operations.set(operationId, {
          status: OPERATION_SUCCEEDED,
          first,
          second,
        });
        this.#wakeOperation(operationId);
      },
      (error: unknown) => {
        if (this.#finished || !this.#operations.has(operationId)) return;
        const classified = classifySpillError(error);
        this.#recordIoError(operationName, error);
        console.warn('[axon] OPFS spill operation failed', {
          operation: operationName,
          error_name: spillErrorName(error),
        });
        this.#operations.set(operationId, {
          status: OPERATION_FAILED,
          errorKind:
            classified === SPILL_ERROR_IO_FAILURE
              ? spillOperationIoErrorKind(operationName)
              : classified,
        });
        this.#wakeOperation(operationId);
      },
    );
    this.#pendingOperationTasks.add(task);
    void task.then(
      () => this.#pendingOperationTasks.delete(task),
      () => this.#pendingOperationTasks.delete(task),
    );
    return operationId;
  }

  async #removeLateFile(name: string): Promise<void> {
    try {
      await this.#executionDirectory.removeEntry(name);
    } catch {
      // The execution namespace may already have been removed by finish().
    }
  }

  #recordIoError(operation: string, error: unknown): void {
    this.#errorOperation = operation;
    this.#errorName = spillErrorName(error);
  }

  #registerHandle(registered: RegisteredHandle): number {
    const id = this.#nextHandleId;
    this.#nextHandleId += 1;
    this.#handles.set(id, registered);
    return id;
  }

  async #closeDataHandles(): Promise<void> {
    for (const [id, registered] of [...this.#handles]) {
      if (registered.fileId !== undefined) {
        try {
          this.close(id);
        } catch {
          this.#handles.delete(id);
        }
      }
    }
  }

  #requireScope(scopeId: number): void {
    if (scopeId !== 1) throw new Error('unknown spill scope');
  }

  #requireFile(fileId: number): SpillFileState {
    const file = this.#files.get(fileId);
    if (!file) throw new Error('unknown spill file');
    return file;
  }

  #requireFileId(fileId: number | undefined): SpillFileState {
    if (fileId === undefined) throw new Error('spill handle is not a data file');
    return this.#requireFile(fileId);
  }

  #requireHandle(handleId: number): RegisteredHandle {
    const handle = this.#handles.get(handleId);
    if (!handle) throw new Error('unknown spill handle');
    return handle;
  }

  #requireOperation(operationId: number): OperationState {
    const operation = this.#operations.get(operationId);
    if (!operation) throw new Error('unknown spill operation');
    return operation;
  }
}

export async function beginOpfsSpillExecution(options: BeginOptions): Promise<OpfsSpillExecution> {
  requireNonNegativeSafeInteger(options.maxBytes, 'spill storage limit');
  const getRoot = options.getRoot ?? browserOpfsRoot;
  const randomId = options.randomId ?? (() => crypto.randomUUID());
  const nowMs = options.nowMs ?? Date.now;
  const namespace = randomId();
  const root = await getRoot();
  const axonDirectory = await root.getDirectoryHandle('axon-spill', { create: true });
  const versionDirectory = await axonDirectory.getDirectoryHandle('v1', { create: true });
  const executionDirectory = await versionDirectory.getDirectoryHandle(namespace, {
    create: true,
  });
  let lease: OpfsSyncAccessHandle | undefined;
  try {
    const leaseFile = await executionDirectory.getFileHandle('lease', { create: true });
    lease = await leaseFile.createSyncAccessHandle();
    const leaseTimestamp = new Uint8Array(new ArrayBuffer(8));
    new DataView(leaseTimestamp.buffer).setFloat64(0, nowMs(), true);
    if (lease.write(leaseTimestamp, { at: 0 }) !== leaseTimestamp.byteLength) {
      throw new Error('spill lease timestamp write was incomplete');
    }
    lease.flush();
    const execution = new OpfsSpillExecution(
      versionDirectory,
      executionDirectory,
      namespace,
      lease,
      options.maxBytes,
      options.wakeOperation,
    );
    lease = undefined;
    executions.set(execution.id, execution);
    return execution;
  } catch (error) {
    try {
      lease?.close();
    } catch {
      // Namespace deletion below remains the authoritative cleanup attempt.
    }
    try {
      await versionDirectory.removeEntry(namespace, { recursive: true });
    } catch {
      // A later worker generation can sweep an unlocked setup remnant.
    }
    throw error;
  }
}

export async function sweepStaleOpfsSpillNamespaces(options: SweepOptions = {}): Promise<number> {
  const getRoot = options.getRoot ?? browserOpfsRoot;
  const nowMs = options.nowMs ?? Date.now;
  const staleAfterMs = options.staleAfterMs ?? DEFAULT_STALE_SCOPE_AGE_MS;
  requireNonNegativeSafeInteger(staleAfterMs, 'stale spill scope age');

  let versionDirectory: OpfsDirectory;
  try {
    const root = await getRoot();
    const axonDirectory = await root.getDirectoryHandle('axon-spill');
    versionDirectory = await axonDirectory.getDirectoryHandle('v1');
  } catch {
    return 0;
  }
  if (!versionDirectory.entries) return 0;

  let deleted = 0;
  for await (const [name, entry] of versionDirectory.entries()) {
    let lease: OpfsSyncAccessHandle | undefined;
    try {
      const directory = entry as OpfsDirectory;
      const leaseFile = await directory.getFileHandle('lease');
      // This succeeds only after a terminated worker releases its exclusive
      // synchronous access handle.
      lease = await leaseFile.createSyncAccessHandle();
      const timestampBytes = new Uint8Array(new ArrayBuffer(8));
      if (lease.read(timestampBytes, { at: 0 }) !== timestampBytes.byteLength) continue;
      const createdAt = new DataView(timestampBytes.buffer).getFloat64(0, true);
      if (!Number.isFinite(createdAt) || createdAt < 0 || nowMs() - createdAt <= staleAfterMs) {
        continue;
      }
      lease.close();
      lease = undefined;
      await versionDirectory.removeEntry(name, { recursive: true });
      deleted += 1;
    } catch {
      // A live lease, malformed same-origin entry, or transient I/O failure
      // must never cause deletion of a scope we cannot prove is stale/unlocked.
    } finally {
      try {
        lease?.close();
      } catch {
        // Best effort; a future generation can retry.
      }
    }
  }
  return deleted;
}

export function unregisterOpfsSpillExecution(executionId: number): void {
  executions.delete(executionId);
}

/**
 * Installs the synchronous numeric bridge consumed by wasm-bindgen imports.
 * It must run before the Wasm module is instantiated.
 */
export function installOpfsSpillBridge(): void {
  const bridge = globalThis as typeof globalThis & Record<string, unknown>;
  bridge.axonSpillStartCreateScope = (executionId: number) =>
    requireExecution(executionId).startCreateScope();
  bridge.axonSpillStartCreateWriter = (executionId: number, scopeId: number) =>
    requireExecution(executionId).startCreateWriter(scopeId);
  bridge.axonSpillStartOpenReader = (executionId: number, fileId: number) =>
    requireExecution(executionId).startOpenReader(fileId);
  bridge.axonSpillStartDeleteFile = (executionId: number, fileId: number) =>
    requireExecution(executionId).startDeleteFile(fileId);
  bridge.axonSpillStartDeleteScope = (executionId: number, scopeId: number) =>
    requireExecution(executionId).startDeleteScope(scopeId);
  bridge.axonSpillOperationStatus = (executionId: number, operationId: number) =>
    requireExecution(executionId).operationStatus(operationId);
  bridge.axonSpillOperationResultFirst = (executionId: number, operationId: number) =>
    requireExecution(executionId).operationResultFirst(operationId);
  bridge.axonSpillOperationResultSecond = (executionId: number, operationId: number) =>
    requireExecution(executionId).operationResultSecond(operationId);
  bridge.axonSpillOperationErrorKind = (executionId: number, operationId: number) =>
    requireExecution(executionId).operationErrorKind(operationId);
  bridge.axonSpillReleaseOperation = (executionId: number, operationId: number) =>
    requireExecution(executionId).releaseOperation(operationId);
  bridge.axonSpillWrite = (
    executionId: number,
    handleId: number,
    bytes: Uint8Array<ArrayBuffer>,
    at: number,
  ) => requireExecution(executionId).write(handleId, bytes, at);
  bridge.axonSpillRead = (
    executionId: number,
    handleId: number,
    bytes: Uint8Array<ArrayBuffer>,
    at: number,
  ) => requireExecution(executionId).read(handleId, bytes, at);
  bridge.axonSpillFlush = (executionId: number, handleId: number) =>
    requireExecution(executionId).flush(handleId);
  bridge.axonSpillClose = (executionId: number, handleId: number) =>
    requireExecution(executionId).close(handleId);
  bridge.axonSpillBytesWritten = (executionId: number) =>
    requireExecution(executionId).accounting().bytes_written;
  bridge.axonSpillBytesRead = (executionId: number) =>
    requireExecution(executionId).accounting().bytes_read;
  bridge.axonSpillFilesCreated = (executionId: number) =>
    requireExecution(executionId).accounting().files_created;
  bridge.axonSpillActiveBytes = (executionId: number) =>
    requireExecution(executionId).accounting().active_bytes;
  bridge.axonSpillPeakActiveBytes = (executionId: number) =>
    requireExecution(executionId).accounting().peak_active_bytes;
  bridge.axonSpillActiveFiles = (executionId: number) =>
    requireExecution(executionId).accounting().active_files;
  bridge.axonSpillActiveHandles = (executionId: number) =>
    requireExecution(executionId).accounting().active_handles;
  bridge.axonSpillMergePasses = (executionId: number) =>
    requireExecution(executionId).accounting().merge_passes;
  bridge.axonSpillRecordMergePass = (executionId: number) =>
    requireExecution(executionId).recordMergePass();
}

export async function probeBrowserExternalMemory(
  options: ProbeOptions = {},
): Promise<BrowserExternalMemoryCapability> {
  const getRoot = options.getRoot ?? browserOpfsRoot;
  const probeBytes = options.probeBytes ?? DEFAULT_PROBE_BYTES;
  const randomId = options.randomId ?? (() => crypto.randomUUID());
  const probeId = randomId();
  let versionDirectory: OpfsDirectory | undefined;
  let handle: OpfsSyncAccessHandle | undefined;
  let operation = 'get_root';

  try {
    const root = await getRoot();
    operation = 'create_namespace';
    const axonDirectory = await root.getDirectoryHandle('axon-spill', { create: true });
    versionDirectory = await axonDirectory.getDirectoryHandle('v1', { create: true });
    const probeDirectory = await versionDirectory.getDirectoryHandle(probeId, { create: true });
    operation = 'create_access_handle';
    const scratchFile = await probeDirectory.getFileHandle('probe', { create: true });
    handle = await scratchFile.createSyncAccessHandle();

    operation = 'write';
    const written = probePattern(probeBytes);
    if (handle.write(written, { at: 0 }) !== written.byteLength) {
      throw new Error('OPFS capability probe performed a short write');
    }
    operation = 'flush';
    handle.flush();
    operation = 'read';
    const read = new Uint8Array(written.byteLength);
    if (handle.read(read, { at: 0 }) !== read.byteLength || !sameBytes(written, read)) {
      throw new Error('OPFS capability probe read did not match its write');
    }
    operation = 'truncate';
    handle.truncate(0);
    operation = 'close';
    handle.close();
    handle = undefined;
    operation = 'delete_namespace';
    await versionDirectory.removeEntry(probeId, { recursive: true });
    return { state: 'supported' };
  } catch (error) {
    console.warn('[axon] OPFS capability probe failed', {
      operation,
      error_name: spillErrorName(error),
    });
    try {
      handle?.close();
    } catch {
      // The unavailable result is authoritative even if cleanup also fails.
    }
    if (versionDirectory) {
      try {
        await versionDirectory.removeEntry(probeId, { recursive: true });
      } catch {
        // Best effort after a failed capability probe.
      }
    }
    return { state: 'unsupported', reason: 'unavailable' };
  }
}

const SPILL_CAP_INCREMENT_BYTES = 64 * 1024 * 1024;

export function deriveProductionSpillCap(maxObservedSpillBytes: number): number {
  requireNonNegativeSafeInteger(maxObservedSpillBytes, 'maximum observed spill volume');
  if (maxObservedSpillBytes === 0) return 0;
  const safetyMargin = Math.ceil(maxObservedSpillBytes / 4);
  const required = maxObservedSpillBytes + safetyMargin;
  if (!Number.isSafeInteger(required)) {
    throw new TypeError('maximum observed spill volume is too large');
  }
  return Math.ceil(required / SPILL_CAP_INCREMENT_BYTES) * SPILL_CAP_INCREMENT_BYTES;
}

export const BROWSER_SPILL_CORPUS_MAX_OBSERVED_BYTES = 459_408_216;
export const BROWSER_SPILL_PRODUCTION_CAP_BYTES = deriveProductionSpillCap(
  BROWSER_SPILL_CORPUS_MAX_OBSERVED_BYTES,
);

export function deriveRuntimeSpillLimit(
  productionCapBytes: number,
  estimate: StorageEstimate,
): number {
  requireNonNegativeSafeInteger(productionCapBytes, 'production spill cap');
  const quota = estimate.quota;
  const usage = estimate.usage;
  if (
    quota === undefined ||
    usage === undefined ||
    !Number.isFinite(quota) ||
    !Number.isFinite(usage) ||
    quota < 0 ||
    usage < 0
  ) {
    return productionCapBytes;
  }
  const estimatedFree = Math.max(0, quota - usage);
  return Math.min(productionCapBytes, Math.floor(estimatedFree / 2));
}

export function applyBrowserExternalMemoryTelemetry(
  metadata: unknown,
  capability: BrowserExternalMemoryCapability,
  accounting?: OpfsSpillAccounting,
  workingSetLimitBytes?: number,
  peakReservationBytes?: number,
): void {
  if (!isRecord(metadata) || !isRecord(metadata.response)) return;
  const response = metadata.response;
  let capabilityReport: Record<string, unknown>;
  if (isRecord(response.capabilities)) {
    capabilityReport = response.capabilities;
  } else {
    capabilityReport = { capabilities: {} };
    response.capabilities = capabilityReport;
  }
  let capabilities: Record<string, unknown>;
  if (isRecord(capabilityReport.capabilities)) {
    capabilities = capabilityReport.capabilities;
  } else {
    capabilities = {};
    capabilityReport.capabilities = capabilities;
  }
  capabilities.browser_external_memory = capability.state;

  if (!accounting || accounting.bytes_written === 0 || !isRecord(response.metrics)) return;
  Object.assign(response.metrics, {
    spill_backend: 'opfs',
    spill_working_set_limit_bytes: workingSetLimitBytes,
    spill_peak_reservation_bytes: peakReservationBytes,
    spill_storage_limit_bytes: accounting.storage_limit_bytes,
    spill_bytes_written: accounting.bytes_written,
    spill_bytes_read: accounting.bytes_read,
    spill_files_created: accounting.files_created,
    spill_peak_active_bytes: accounting.peak_active_bytes,
    spill_active_files: accounting.active_files,
    spill_merge_passes: accounting.merge_passes,
    spill_cleanup_count: accounting.scopes_deleted,
    spill_abandoned_cleanup_count: accounting.abandoned_scopes,
    spill_cleanup_files: accounting.files_deleted,
    spill_cleanup_scopes: accounting.scopes_deleted,
  });
}

export function privateOpfsSpillMetrics(
  accounting: OpfsSpillAccounting,
  workingSetLimitBytes?: number,
  peakReservationBytes?: number,
  errorReason?: 'unavailable' | 'quota_exceeded' | 'io_failure',
): PrivateExternalMemoryMetrics {
  return {
    backend: 'opfs',
    storage_limit_bytes: String(accounting.storage_limit_bytes),
    bytes_written: String(accounting.bytes_written),
    bytes_read: String(accounting.bytes_read),
    files_created: String(accounting.files_created),
    peak_active_bytes: String(accounting.peak_active_bytes),
    active_files: String(accounting.active_files),
    merge_passes: String(accounting.merge_passes),
    cleanup_count: String(accounting.scopes_deleted),
    abandoned_cleanup_count: String(accounting.abandoned_scopes),
    ...(workingSetLimitBytes === undefined
      ? {}
      : { working_set_limit_bytes: String(workingSetLimitBytes) }),
    ...(peakReservationBytes === undefined
      ? {}
      : { peak_reservation_bytes: String(peakReservationBytes) }),
    ...(errorReason === undefined ? {} : { error_reason: errorReason }),
  };
}

async function browserOpfsRoot(): Promise<OpfsDirectory> {
  const storage = navigator.storage as StorageManager & {
    getDirectory?: () => Promise<FileSystemDirectoryHandle>;
  };
  if (typeof storage.getDirectory !== 'function') {
    throw new Error('OPFS getDirectory is unavailable');
  }
  return (await storage.getDirectory()) as unknown as OpfsDirectory;
}

function classifySpillError(error: unknown): number {
  if (error instanceof DOMException && error.name === 'QuotaExceededError') {
    return SPILL_ERROR_QUOTA_EXCEEDED;
  }
  if (error instanceof Error && /quota exceeded/i.test(error.message)) {
    return SPILL_ERROR_QUOTA_EXCEEDED;
  }
  if (error instanceof Error && /unavailable|not supported/i.test(error.message)) {
    return SPILL_ERROR_UNAVAILABLE;
  }
  return SPILL_ERROR_IO_FAILURE;
}

function requireExecution(executionId: number): OpfsSpillExecution {
  const execution = executions.get(executionId);
  if (!execution) throw new Error('spill execution is unavailable');
  return execution;
}

function normalizeSyncIoError(error: unknown, operation: string): Error {
  if (classifySpillError(error) === SPILL_ERROR_QUOTA_EXCEEDED) {
    return new Error('spill storage quota exceeded');
  }
  console.warn('[axon] OPFS spill operation failed', {
    operation,
    error_name: spillErrorName(error),
  });
  return new Error(`spill_storage/io_failure/${operation}/${spillErrorName(error)}`);
}

function closeHandleBestEffort(handle: OpfsSyncAccessHandle): void {
  try {
    handle.close();
  } catch {
    // A late handle must never be installed after execution cleanup.
  }
}

export function spillErrorName(error: unknown): string {
  if (error instanceof DOMException) return error.name || 'DOMException';
  if (error instanceof Error) return error.name || 'Error';
  return 'UnknownError';
}

function spillOperationIoErrorKind(operation: SpillOperationName): number {
  return (
    {
      create_scope: 31,
      create_writer: 32,
      open_reader: 33,
      delete_file: 34,
      delete_scope: 35,
    } as const
  )[operation];
}

function checkedEnd(at: number, length: number, operation: string): number {
  requireNonNegativeSafeInteger(at, `${operation} offset`);
  const end = at + length;
  if (!Number.isSafeInteger(end)) throw new TypeError(`${operation} range is invalid`);
  return end;
}

function requireIoLength(actual: number, maximum: number, operation: string): void {
  if (!Number.isSafeInteger(actual) || actual < 0 || actual > maximum) {
    throw new Error(`spill storage returned an invalid ${operation} length`);
  }
}

function requireNonNegativeSafeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${field} must be a non-negative safe integer`);
  }
}

function probePattern(byteLength: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(new ArrayBuffer(byteLength));
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = (index * 31 + 17) & 0xff;
  }
  return bytes;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

import { BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_BYTES } from './browser-datafusion-memory-policy.ts';

export type OpfsSyncAccess = {
  read(target: Uint8Array, options: { at: number }): number;
  write(source: Uint8Array, options: { at: number }): number;
  truncate(size: number): void;
  flush(): void;
  getSize(): number;
  close(): void;
};

export type OpfsFile = {
  createSyncAccessHandle(): Promise<OpfsSyncAccess>;
  getLastModified(): Promise<number>;
};

export type OpfsDirectory = {
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<OpfsDirectory>;
  getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFile>;
  removeEntry(name: string, options?: { recursive?: boolean }): Promise<void>;
  directoryEntries(): AsyncIterable<[string, OpfsDirectory]>;
};

export type OpfsStorage = {
  getDirectory(): Promise<OpfsDirectory>;
  estimate(): Promise<{ quota?: number; usage?: number }>;
};

export type OpfsSpillErrorReason = 'unavailable' | 'quota_exceeded' | 'io_failure';

export class OpfsSpillError extends Error {
  readonly name = 'OpfsSpillError';

  constructor(
    readonly reason: OpfsSpillErrorReason,
    message: string,
  ) {
    super(message);
  }
}

export type OpfsSpillScopeRef = {
  readonly scopeId: number;
};

export type OpfsSpillFileRef = {
  readonly scopeId: number;
  readonly fileId: number;
};

export type OpfsSpillMetrics = {
  backend: 'opfs';
  bytesWritten: number;
  bytesRead: number;
  filesCreated: number;
  activeBytes: number;
  peakActiveBytes: number;
  storageLimitBytes: number;
  activeScopes: number;
  activeFiles: number;
  activeHandles: number;
  mergePasses: number;
  scopesDeleted: number;
  abandonedScopesDeleted: number;
};

export type OpfsSpillQuerySnapshot = {
  readonly generationId: number;
  readonly metrics: OpfsSpillMetrics;
};

type ScopeState = {
  generationId: number;
  directoryName: string;
  directory: OpfsDirectory;
  lease: OpfsSyncAccess;
  files: Map<number, { name: string; size: number }>;
};

type QueryGeneration = {
  readonly id: number;
  readonly pending: Set<Promise<unknown>>;
  closed: boolean;
};

type HandleState = {
  kind: 'reader' | 'writer';
  scopeId: number;
  fileId: number;
  access: OpfsSyncAccess;
  offset: number;
};

const ROOT_DIRECTORY = 'axon-spill';
const VERSION_DIRECTORY = 'v1';
const LEASE_FILE = 'lease';
const SCAVENGE_MARKER_FILE = 'scavenge-after';
const ABANDONED_SCOPE_AGE_MS = 60 * 60 * 1000;
const PROBE_BYTES = Uint8Array.from([0x41, 0x78, 0x6f, 0x6e]);

export async function probeBrowserExternalMemory(
  storage?: OpfsStorage,
): Promise<{ available: true } | { available: false; reason: 'unavailable' | 'io_failure' }> {
  let versionDirectory: OpfsDirectory | undefined;
  let probeDirectoryName: string | undefined;
  let access: OpfsSyncAccess | undefined;
  try {
    const root = await (storage ?? browserOpfsStorage()).getDirectory();
    const axonDirectory = await root.getDirectoryHandle(ROOT_DIRECTORY, { create: true });
    versionDirectory = await axonDirectory.getDirectoryHandle(VERSION_DIRECTORY, { create: true });
    probeDirectoryName = `probe-${opaqueUuid()}`;
    const probeDirectory = await versionDirectory.getDirectoryHandle(probeDirectoryName, {
      create: true,
    });
    const file = await probeDirectory.getFileHandle('scratch', { create: true });
    access = await file.createSyncAccessHandle();
    if (access.write(PROBE_BYTES, { at: 0 }) !== PROBE_BYTES.byteLength) {
      throw new Error('OPFS probe write was partial');
    }
    access.flush();
    const actual = new Uint8Array(PROBE_BYTES.byteLength);
    if (access.read(actual, { at: 0 }) !== actual.byteLength) {
      throw new Error('OPFS probe read was partial');
    }
    if (!actual.every((byte, index) => byte === PROBE_BYTES[index])) {
      throw new Error('OPFS probe read did not match its write');
    }
    access.truncate(0);
    access.flush();
    if (access.getSize() !== 0) throw new Error('OPFS probe truncate did not persist');
    return { available: true };
  } catch (error) {
    return {
      available: false,
      reason: isUnavailableError(error) ? 'unavailable' : 'io_failure',
    };
  } finally {
    try {
      access?.close();
    } catch {
      // The capability result already records the primary probe failure.
    }
    if (versionDirectory && probeDirectoryName) {
      try {
        await versionDirectory.removeEntry(probeDirectoryName, { recursive: true });
      } catch {
        // Startup scavenging can remove an abandoned probe namespace later.
      }
    }
  }
}

export async function initializeBrowserExternalMemory(
  storage: OpfsStorage,
  options: { productionCapBytes: number },
): Promise<
  { available: true; host: OpfsSpillHost } | { available: false; reason: OpfsSpillErrorReason }
> {
  const probe = await probeBrowserExternalMemory(storage);
  if (!probe.available) return probe;
  try {
    return {
      available: true,
      host: await OpfsSpillHost.open(storage, options),
    };
  } catch (error) {
    return { available: false, reason: mapOpfsError(error).reason };
  }
}

export class OpfsSpillHost {
  private readonly scopes = new Map<number, ScopeState>();
  private readonly handles = new Map<number, HandleState>();
  private readonly releasingScopes = new Map<number, Promise<void>>();
  private readonly queryGenerations = new Map<number, QueryGeneration>();
  private nextScopeId = 1;
  private nextFileId = 1;
  private nextHandleId = 1;
  private nextGenerationId = 1;
  private activeGeneration: QueryGeneration | undefined;
  private activeBytes = 0;
  private peakActiveBytes = 0;
  private queryPeakActiveBytes = 0;
  private bytesWritten = 0;
  private bytesRead = 0;
  private filesCreated = 0;
  private mergePasses = 0;
  private scopesDeleted = 0;
  private abandonedScopesDeleted = 0;

  private constructor(
    private readonly versionDirectory: OpfsDirectory,
    private readonly spillCapBytes: number,
  ) {}

  static async open(
    storage: OpfsStorage = browserOpfsStorage(),
    options: { productionCapBytes: number; nowMs?: number },
  ): Promise<OpfsSpillHost> {
    if (!Number.isSafeInteger(options.productionCapBytes) || options.productionCapBytes <= 0) {
      throw new TypeError('OPFS production spill cap must be a positive safe integer');
    }
    try {
      const root = await storage.getDirectory();
      const axonDirectory = await root.getDirectoryHandle(ROOT_DIRECTORY, { create: true });
      const versionDirectory = await axonDirectory.getDirectoryHandle(VERSION_DIRECTORY, {
        create: true,
      });
      const configuredCapBytes = Math.min(
        options.productionCapBytes,
        BROWSER_EXTERNAL_MEMORY_PRODUCTION_CAP_BYTES,
      );
      const estimate = await storage.estimate();
      const freeBytes =
        typeof estimate.quota === 'number' &&
        Number.isSafeInteger(estimate.quota) &&
        estimate.quota >= 0 &&
        typeof estimate.usage === 'number' &&
        Number.isSafeInteger(estimate.usage) &&
        estimate.usage >= 0
          ? Math.max(0, estimate.quota - estimate.usage)
          : undefined;
      const spillCapBytes =
        freeBytes === undefined
          ? configuredCapBytes
          : Math.min(configuredCapBytes, Math.floor(freeBytes / 2));
      const host = new OpfsSpillHost(versionDirectory, spillCapBytes);
      await host.sweepAbandonedScopes(options.nowMs ?? Date.now());
      return host;
    } catch (error) {
      throw mapOpfsError(error);
    }
  }

  async createScope(): Promise<OpfsSpillScopeRef> {
    return this.trackCurrentOperation(async (generation) => {
      const scopeId = this.nextScopeId++;
      const directoryName = opaqueUuid();
      let directory: OpfsDirectory | undefined;
      let lease: OpfsSyncAccess | undefined;
      try {
        directory = await this.versionDirectory.getDirectoryHandle(directoryName, {
          create: true,
        });
        const leaseFile = await directory.getFileHandle(LEASE_FILE, { create: true });
        lease = await leaseFile.createSyncAccessHandle();
        if (generation?.closed) {
          lease.close();
          lease = undefined;
          await this.versionDirectory.removeEntry(directoryName, { recursive: true });
          throw generationClosedError();
        }
        try {
          await directory.removeEntry(SCAVENGE_MARKER_FILE);
        } catch (error) {
          if (!isNotFoundError(error)) throw error;
        }
        this.scopes.set(scopeId, {
          generationId: generation?.id ?? 0,
          directoryName,
          directory,
          lease,
          files: new Map(),
        });
        lease = undefined;
        return { scopeId };
      } catch (error) {
        try {
          lease?.close();
        } catch {
          // Preserve the primary acquisition failure.
        }
        if (directory) {
          try {
            await this.versionDirectory.removeEntry(directoryName, { recursive: true });
          } catch {
            // Query cleanup or the failed acquisition path may already have removed it.
          }
        }
        throw mapOpfsError(error);
      }
    });
  }

  async createFile(
    scope: OpfsSpillScopeRef,
  ): Promise<{ file: OpfsSpillFileRef; writerId: number }> {
    return this.trackCurrentOperation(async (generation) => {
      const state = this.requireScope(scope.scopeId);
      const fileId = this.nextFileId++;
      const name = `spill-${fileId}`;
      let access: OpfsSyncAccess | undefined;
      try {
        const fileHandle = await state.directory.getFileHandle(name, { create: true });
        access = await fileHandle.createSyncAccessHandle();
        if (
          generation?.closed ||
          !this.scopes.has(scope.scopeId) ||
          state.generationId !== (generation?.id ?? 0)
        ) {
          access.close();
          access = undefined;
          try {
            await state.directory.removeEntry(name);
          } catch {
            // Query cleanup may already have removed the containing directory.
          }
          throw generationClosedError();
        }
        access.truncate(0);
        const writerId = this.nextHandleId++;
        state.files.set(fileId, { name, size: 0 });
        this.handles.set(writerId, {
          kind: 'writer',
          scopeId: scope.scopeId,
          fileId,
          access,
          offset: 0,
        });
        access = undefined;
        this.filesCreated += 1;
        return { file: { scopeId: scope.scopeId, fileId }, writerId };
      } catch (error) {
        try {
          access?.close();
        } catch {
          // Preserve the primary acquisition failure.
        }
        throw mapOpfsError(error);
      }
    });
  }

  append(writerId: number, bytes: Uint8Array): void {
    const handle = this.requireHandle(writerId, 'writer');
    if (bytes.byteLength === 0) return;
    if (this.activeBytes + bytes.byteLength > this.spillCapBytes) {
      throw new OpfsSpillError('quota_exceeded', 'browser spill storage quota exceeded');
    }
    try {
      const written = handle.access.write(bytes, { at: handle.offset });
      if (written !== bytes.byteLength) {
        throw new Error('OPFS spill append was partial');
      }
      handle.offset += written;
      const file = this.requireFile(handle.scopeId, handle.fileId);
      const nextSize = Math.max(file.size, handle.offset);
      const growth = nextSize - file.size;
      file.size = nextSize;
      this.activeBytes += growth;
      this.peakActiveBytes = Math.max(this.peakActiveBytes, this.activeBytes);
      this.queryPeakActiveBytes = Math.max(this.queryPeakActiveBytes, this.activeBytes);
      this.bytesWritten += written;
    } catch (error) {
      try {
        this.reconcileWriterSize(handle);
      } catch {
        // Preserve the primary write failure; query cleanup owns reclamation.
      }
      throw mapOpfsError(error);
    }
  }

  finalizeWriter(writerId: number): void {
    const handle = this.requireHandle(writerId, 'writer');
    try {
      handle.access.flush();
      handle.access.close();
      this.handles.delete(writerId);
    } catch (error) {
      throw mapOpfsError(error);
    }
  }

  async openReader(file: OpfsSpillFileRef): Promise<number> {
    return this.trackCurrentOperation(async (generation) => {
      const scope = this.requireScope(file.scopeId);
      const state = this.requireFile(file.scopeId, file.fileId);
      let access: OpfsSyncAccess | undefined;
      try {
        const fileHandle = await scope.directory.getFileHandle(state.name);
        access = await fileHandle.createSyncAccessHandle();
        if (
          generation?.closed ||
          !this.scopes.has(file.scopeId) ||
          scope.generationId !== (generation?.id ?? 0)
        ) {
          access.close();
          access = undefined;
          throw generationClosedError();
        }
        const readerId = this.nextHandleId++;
        this.handles.set(readerId, {
          kind: 'reader',
          scopeId: file.scopeId,
          fileId: file.fileId,
          access,
          offset: 0,
        });
        access = undefined;
        return readerId;
      } catch (error) {
        try {
          access?.close();
        } catch {
          // Preserve the primary acquisition failure.
        }
        throw mapOpfsError(error);
      }
    });
  }

  readNext(readerId: number, maxBytes: number): Uint8Array | undefined {
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
      throw new TypeError('OPFS spill read size must be a positive safe integer');
    }
    const target = new Uint8Array(maxBytes);
    const read = this.readInto(readerId, target);
    if (read === undefined) return undefined;
    return read === target.byteLength ? target : target.slice(0, read);
  }

  readInto(readerId: number, target: Uint8Array): number | undefined {
    if (target.byteLength === 0) {
      throw new TypeError('OPFS spill read target must not be empty');
    }
    const handle = this.requireHandle(readerId, 'reader');
    const remaining = handle.access.getSize() - handle.offset;
    if (remaining <= 0) return undefined;
    const requested = target.subarray(0, Math.min(target.byteLength, remaining));
    try {
      const read = handle.access.read(requested, { at: handle.offset });
      if (read <= 0 || read > requested.byteLength) {
        throw new OpfsSpillError('io_failure', 'browser spill read made no progress');
      }
      handle.offset += read;
      this.bytesRead += read;
      return read;
    } catch (error) {
      throw mapOpfsError(error);
    }
  }

  closeReader(readerId: number): void {
    const handle = this.requireHandle(readerId, 'reader');
    try {
      handle.access.close();
    } finally {
      this.handles.delete(readerId);
    }
  }

  async deleteFile(file: OpfsSpillFileRef): Promise<void> {
    return this.trackCurrentOperation(async () => {
      const scope = this.requireScope(file.scopeId);
      const state = this.requireFile(file.scopeId, file.fileId);
      this.closeFileHandles(file.scopeId, file.fileId);
      try {
        await scope.directory.removeEntry(state.name);
        scope.files.delete(file.fileId);
        this.activeBytes = Math.max(0, this.activeBytes - state.size);
      } catch (error) {
        throw mapOpfsError(error);
      }
    });
  }

  async deleteScope(scope: OpfsSpillScopeRef): Promise<void> {
    return this.trackCurrentOperation(async () => {
      const state = this.requireScope(scope.scopeId);
      this.closeScopeHandles(scope.scopeId);
      try {
        try {
          state.lease.close();
        } catch {
          // Attempt recursive deletion even when a stale handle reports a
          // close failure. A still-live lock will make deletion fail safely.
        }
        await this.versionDirectory.removeEntry(state.directoryName, { recursive: true });
        for (const file of state.files.values()) {
          this.activeBytes = Math.max(0, this.activeBytes - file.size);
        }
        this.scopes.delete(scope.scopeId);
        this.scopesDeleted += 1;
      } catch (error) {
        throw mapOpfsError(error);
      }
    });
  }

  async releaseScope(scope: OpfsSpillScopeRef): Promise<void> {
    const existing = this.releasingScopes.get(scope.scopeId);
    if (existing) return existing;
    if (!this.scopes.has(scope.scopeId)) return;
    const release = this.deleteScope(scope).finally(() => {
      this.releasingScopes.delete(scope.scopeId);
    });
    this.releasingScopes.set(scope.scopeId, release);
    await release;
  }

  metrics(): OpfsSpillMetrics {
    const activeFiles = Array.from(this.scopes.values()).reduce(
      (count, scope) => count + scope.files.size,
      0,
    );
    return {
      backend: 'opfs',
      bytesWritten: this.bytesWritten,
      bytesRead: this.bytesRead,
      filesCreated: this.filesCreated,
      activeBytes: this.activeBytes,
      peakActiveBytes: this.peakActiveBytes,
      storageLimitBytes: this.spillCapBytes,
      activeScopes: this.scopes.size,
      activeFiles,
      activeHandles: this.handles.size,
      mergePasses: this.mergePasses,
      scopesDeleted: this.scopesDeleted,
      abandonedScopesDeleted: this.abandonedScopesDeleted,
    };
  }

  beginQueryMetrics(): OpfsSpillQuerySnapshot {
    if (this.activeGeneration) {
      throw new OpfsSpillError('io_failure', 'browser spill query generation is already active');
    }
    const generation: QueryGeneration = {
      id: this.nextGenerationId++,
      pending: new Set(),
      closed: false,
    };
    this.activeGeneration = generation;
    this.queryGenerations.set(generation.id, generation);
    this.queryPeakActiveBytes = this.activeBytes;
    return {
      generationId: generation.id,
      metrics: this.metrics(),
    };
  }

  async releaseQueryScopes(start: OpfsSpillQuerySnapshot): Promise<void> {
    const generation = this.queryGenerations.get(start.generationId);
    if (generation) {
      generation.closed = true;
      if (this.activeGeneration === generation) this.activeGeneration = undefined;
      while (generation.pending.size > 0) {
        await Promise.allSettled(Array.from(generation.pending));
      }
    }
    const scopeIds = Array.from(this.scopes.entries())
      .filter(([, scope]) => scope.generationId === start.generationId)
      .map(([scopeId]) => scopeId);
    await Promise.all(scopeIds.map((scopeId) => this.releaseScope({ scopeId })));
    this.queryGenerations.delete(start.generationId);
  }

  queryMetricsSince(start: OpfsSpillQuerySnapshot): OpfsSpillMetrics {
    const current = this.metrics();
    return {
      ...current,
      bytesWritten: Math.max(0, current.bytesWritten - start.metrics.bytesWritten),
      bytesRead: Math.max(0, current.bytesRead - start.metrics.bytesRead),
      filesCreated: Math.max(0, current.filesCreated - start.metrics.filesCreated),
      mergePasses: Math.max(0, current.mergePasses - start.metrics.mergePasses),
      peakActiveBytes: this.queryPeakActiveBytes,
      scopesDeleted: Math.max(0, current.scopesDeleted - start.metrics.scopesDeleted),
      abandonedScopesDeleted: Math.max(
        0,
        current.abandonedScopesDeleted - start.metrics.abandonedScopesDeleted,
      ),
    };
  }

  recordMergePass(): void {
    this.mergePasses += 1;
  }

  private async sweepAbandonedScopes(nowMs: number): Promise<void> {
    for await (const [name, directory] of this.versionDirectory.directoryEntries()) {
      if (name.startsWith('probe-')) {
        await this.sweepProbeDirectory(name, directory);
        continue;
      }
      let lease: OpfsSyncAccess | undefined;
      try {
        const leaseFile = await directory.getFileHandle(LEASE_FILE);
        lease = await leaseFile.createSyncAccessHandle();
        const lastModified = await leaseFile.getLastModified();
        lease.close();
        lease = undefined;
        if (nowMs - lastModified >= ABANDONED_SCOPE_AGE_MS) {
          await this.removeAbandonedDirectory(name);
        }
      } catch (error) {
        try {
          lease?.close();
        } catch {
          // The lease may already be invalid.
        }
        if (isNotFoundError(error)) {
          await this.quarantineOrRemoveLeaseLessDirectory(name, directory, nowMs);
        } else if (!isLockError(error)) {
          throw mapOpfsError(error);
        }
      }
    }
  }

  private async sweepProbeDirectory(name: string, directory: OpfsDirectory): Promise<void> {
    let scratch: OpfsSyncAccess | undefined;
    try {
      const scratchFile = await directory.getFileHandle('scratch');
      scratch = await scratchFile.createSyncAccessHandle();
      scratch.close();
      scratch = undefined;
      await this.removeAbandonedDirectory(name);
    } catch (error) {
      try {
        scratch?.close();
      } catch {
        // The probe handle may already be invalid.
      }
      if (isNotFoundError(error)) {
        await this.quarantineOrRemoveLeaseLessDirectory(name, directory, Date.now());
      } else if (!isLockError(error)) {
        throw mapOpfsError(error);
      }
    }
  }

  private async removeAbandonedDirectory(name: string): Promise<void> {
    await this.versionDirectory.removeEntry(name, { recursive: true });
    this.abandonedScopesDeleted += 1;
  }

  private async quarantineOrRemoveLeaseLessDirectory(
    name: string,
    directory: OpfsDirectory,
    nowMs: number,
  ): Promise<void> {
    try {
      const marker = await directory.getFileHandle(SCAVENGE_MARKER_FILE);
      if (nowMs - (await marker.getLastModified()) >= ABANDONED_SCOPE_AGE_MS) {
        await this.removeAbandonedDirectory(name);
      }
    } catch (error) {
      if (!isNotFoundError(error)) throw error;
      const marker = await directory.getFileHandle(SCAVENGE_MARKER_FILE, { create: true });
      const access = await marker.createSyncAccessHandle();
      try {
        access.truncate(0);
        access.flush();
      } finally {
        access.close();
      }
    }
  }

  private trackCurrentOperation<T>(
    operation: (generation: QueryGeneration | undefined) => Promise<T>,
  ): Promise<T> {
    const generation = this.activeGeneration;
    const pending = operation(generation);
    if (!generation) return pending;
    generation.pending.add(pending);
    void pending.then(
      () => generation.pending.delete(pending),
      () => generation.pending.delete(pending),
    );
    return pending;
  }

  private requireScope(scopeId: number): ScopeState {
    const scope = this.scopes.get(scopeId);
    if (!scope) throw new OpfsSpillError('unavailable', 'browser spill scope is unavailable');
    return scope;
  }

  private requireFile(scopeId: number, fileId: number): { name: string; size: number } {
    const file = this.requireScope(scopeId).files.get(fileId);
    if (!file) throw new OpfsSpillError('unavailable', 'browser spill file is unavailable');
    return file;
  }

  private requireHandle(handleId: number, kind: HandleState['kind']): HandleState {
    const handle = this.handles.get(handleId);
    if (!handle || handle.kind !== kind) {
      throw new OpfsSpillError('unavailable', `browser spill ${kind} is unavailable`);
    }
    return handle;
  }

  private reconcileWriterSize(handle: HandleState): void {
    const file = this.requireFile(handle.scopeId, handle.fileId);
    const previousSize = file.size;
    const actualSize = handle.access.getSize();
    file.size = actualSize;
    handle.offset = actualSize;
    if (actualSize >= previousSize) {
      const growth = actualSize - previousSize;
      this.activeBytes += growth;
      this.bytesWritten += growth;
    } else {
      this.activeBytes = Math.max(0, this.activeBytes - (previousSize - actualSize));
    }
    this.peakActiveBytes = Math.max(this.peakActiveBytes, this.activeBytes);
    this.queryPeakActiveBytes = Math.max(this.queryPeakActiveBytes, this.activeBytes);
  }

  private closeFileHandles(scopeId: number, fileId: number): void {
    for (const [handleId, handle] of this.handles) {
      if (handle.scopeId !== scopeId || handle.fileId !== fileId) continue;
      try {
        handle.access.close();
      } catch {
        // Deletion below is authoritative. Continue closing sibling handles so
        // one broken handle cannot prevent query-wide cleanup.
      } finally {
        this.handles.delete(handleId);
      }
    }
  }

  private closeScopeHandles(scopeId: number): void {
    for (const [handleId, handle] of this.handles) {
      if (handle.scopeId !== scopeId) continue;
      try {
        handle.access.close();
      } catch {
        // Deletion below is authoritative. Continue closing sibling handles so
        // one broken handle cannot prevent query-wide cleanup.
      } finally {
        this.handles.delete(handleId);
      }
    }
  }
}

export function browserOpfsStorage(): OpfsStorage {
  const browserStorage = navigator.storage as StorageManager & {
    getDirectory?: () => Promise<FileSystemDirectoryHandle>;
  };
  if (typeof browserStorage.getDirectory !== 'function') {
    throw new OpfsSpillError('unavailable', 'browser OPFS is unavailable');
  }
  return {
    getDirectory: async () => wrapBrowserDirectory(await browserStorage.getDirectory!()),
    estimate: async () => browserStorage.estimate(),
  };
}

function wrapBrowserDirectory(directory: FileSystemDirectoryHandle): OpfsDirectory {
  type IterableDirectory = FileSystemDirectoryHandle & {
    entries(): AsyncIterableIterator<[string, FileSystemHandle]>;
  };
  return {
    getDirectoryHandle: async (name, options) =>
      wrapBrowserDirectory(await directory.getDirectoryHandle(name, options)),
    getFileHandle: async (name, options) =>
      wrapBrowserFile(await directory.getFileHandle(name, options)),
    removeEntry: async (name, options) => directory.removeEntry(name, options),
    async *directoryEntries() {
      for await (const [name, handle] of (directory as IterableDirectory).entries()) {
        if (handle.kind === 'directory') {
          yield [name, wrapBrowserDirectory(handle as FileSystemDirectoryHandle)];
        }
      }
    },
  };
}

function wrapBrowserFile(file: FileSystemFileHandle): OpfsFile {
  type SyncFile = FileSystemFileHandle & {
    createSyncAccessHandle(): Promise<OpfsSyncAccess>;
  };
  return {
    createSyncAccessHandle: () => (file as SyncFile).createSyncAccessHandle(),
    getLastModified: async () => (await file.getFile()).lastModified,
  };
}

function opaqueUuid(): string {
  return crypto.randomUUID();
}

function mapOpfsError(error: unknown): OpfsSpillError {
  if (error instanceof OpfsSpillError) return error;
  if (isQuotaError(error)) {
    return new OpfsSpillError('quota_exceeded', 'browser spill storage quota exceeded');
  }
  if (isUnavailableError(error)) {
    return new OpfsSpillError('unavailable', 'browser spill storage is unavailable');
  }
  return new OpfsSpillError('io_failure', 'browser spill storage I/O failed');
}

function generationClosedError(): OpfsSpillError {
  return new OpfsSpillError('unavailable', 'browser spill query generation is closed');
}

function isQuotaError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'QuotaExceededError';
}

function isLockError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'NoModificationAllowedError';
}

function isNotFoundError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'NotFoundError';
}

function isUnavailableError(error: unknown): boolean {
  return (
    error instanceof OpfsSpillError ||
    (error instanceof DOMException &&
      [
        'NotAllowedError',
        'NotFoundError',
        'NotSupportedError',
        'SecurityError',
        'UnknownError',
      ].includes(error.name))
  );
}

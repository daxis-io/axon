import {
  OpfsSpillError,
  type OpfsSpillFileRef,
  type OpfsSpillHost,
  type OpfsSpillMetrics,
  type OpfsSpillScopeRef,
} from './opfs-spill-host.ts';

const BRIDGE_GLOBAL = '__axon_opfs_spill_v1__';

type InstalledBridge = {
  createScope(): Promise<number>;
  createFile(scopeId: number): Promise<{ fileId: number; writerId: number }>;
  append(writerId: number, bytes: Uint8Array): void;
  finalizeWriter(writerId: number): void;
  openReader(scopeId: number, fileId: number): Promise<number>;
  readNext(readerId: number, target: Uint8Array): number | undefined;
  closeReader(readerId: number): void;
  deleteFile(scopeId: number, fileId: number): Promise<void>;
  deleteScope(scopeId: number): Promise<void>;
  releaseScope(scopeId: number): Promise<void>;
  recordMergePass(): void;
  metrics(): OpfsSpillMetrics;
};

type SpillBridgeGlobal = typeof globalThis & {
  [BRIDGE_GLOBAL]?: InstalledBridge;
};

/**
 * Installs one worker-generation-local bridge for the Wasm runtime.
 *
 * Only opaque numeric references cross this boundary. Browser handles, SQL,
 * resource identities, and file names remain private to `OpfsSpillHost`.
 */
export function installOpfsSpillBridge(host: OpfsSpillHost): () => void {
  const scope = globalThis as SpillBridgeGlobal;
  if (scope[BRIDGE_GLOBAL] !== undefined) {
    throw new OpfsSpillError('io_failure', 'browser spill bridge is already installed');
  }

  const bridge: InstalledBridge = {
    createScope: async () => (await host.createScope()).scopeId,
    createFile: async (scopeId) => {
      const created = await host.createFile(scopeRef(scopeId));
      return { fileId: created.file.fileId, writerId: created.writerId };
    },
    append: (writerId, bytes) => host.append(writerId, bytes),
    finalizeWriter: (writerId) => host.finalizeWriter(writerId),
    openReader: (scopeId, fileId) => host.openReader(fileRef(scopeId, fileId)),
    readNext: (readerId, target) => host.readInto(readerId, target),
    closeReader: (readerId) => host.closeReader(readerId),
    deleteFile: (scopeId, fileId) => host.deleteFile(fileRef(scopeId, fileId)),
    deleteScope: (scopeId) => host.deleteScope(scopeRef(scopeId)),
    releaseScope: (scopeId) => host.releaseScope(scopeRef(scopeId)),
    recordMergePass: () => host.recordMergePass(),
    metrics: () => host.metrics(),
  };
  Object.defineProperty(scope, BRIDGE_GLOBAL, {
    configurable: true,
    enumerable: false,
    value: bridge,
    writable: false,
  });

  return () => {
    if (scope[BRIDGE_GLOBAL] === bridge) delete scope[BRIDGE_GLOBAL];
  };
}

function scopeRef(scopeId: number): OpfsSpillScopeRef {
  requireOpaqueId(scopeId);
  return { scopeId };
}

function fileRef(scopeId: number, fileId: number): OpfsSpillFileRef {
  requireOpaqueId(scopeId);
  requireOpaqueId(fileId);
  return { scopeId, fileId };
}

function requireOpaqueId(id: number): void {
  if (!Number.isSafeInteger(id) || id <= 0) {
    throw new OpfsSpillError('unavailable', 'browser spill reference is unavailable');
  }
}

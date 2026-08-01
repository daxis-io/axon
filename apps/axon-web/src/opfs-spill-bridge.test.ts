import { afterEach, describe, expect, it, vi } from 'vitest';
import { installOpfsSpillBridge } from './opfs-spill-bridge.ts';
import type { OpfsSpillHost } from './opfs-spill-host.ts';

const BRIDGE_GLOBAL = '__axon_opfs_spill_v1__';

type TestBridgeGlobal = typeof globalThis & {
  [BRIDGE_GLOBAL]?: {
    createScope(): Promise<number>;
    createFile(scopeId: number): Promise<{ fileId: number; writerId: number }>;
    append(writerId: number, bytes: Uint8Array): Promise<void>;
    deleteScope(scopeId: number): Promise<void>;
    releaseScope(scopeId: number): Promise<void>;
    recordMergePass(): void;
  };
};

afterEach(() => {
  delete (globalThis as TestBridgeGlobal)[BRIDGE_GLOBAL];
});

describe('OPFS Wasm spill bridge', () => {
  it('crosses only opaque numeric references and byte buffers', async () => {
    const createScope = vi.fn(async () => ({ scopeId: 7 }));
    const createFile = vi.fn(async () => ({
      file: { scopeId: 7, fileId: 9 },
      writerId: 11,
    }));
    const append = vi.fn(async () => undefined);
    const deleteScope = vi.fn(async () => undefined);
    const releaseScope = vi.fn(async () => undefined);
    const recordMergePass = vi.fn(() => undefined);
    const host = {
      createScope,
      createFile,
      append,
      finalizeWriter: vi.fn(async () => undefined),
      openReader: vi.fn(async () => 13),
      readNext: vi.fn(() => undefined),
      closeReader: vi.fn(() => undefined),
      deleteFile: vi.fn(async () => undefined),
      deleteScope,
      releaseScope,
      recordMergePass,
      metrics: vi.fn(() => ({
        backend: 'opfs',
        bytesWritten: 0,
        bytesRead: 0,
        filesCreated: 0,
        activeBytes: 0,
        peakActiveBytes: 0,
        storageLimitBytes: 64 * 1024 * 1024,
        activeScopes: 0,
        activeFiles: 0,
        activeHandles: 0,
        mergePasses: 0,
        scopesDeleted: 0,
        abandonedScopesDeleted: 0,
      })),
    } as unknown as OpfsSpillHost;

    const uninstall = installOpfsSpillBridge(host);
    const bridge = (globalThis as TestBridgeGlobal)[BRIDGE_GLOBAL]!;
    const scopeId = await bridge.createScope();
    const created = await bridge.createFile(scopeId);
    const bytes = Uint8Array.from([1, 2, 3]);
    await bridge.append(created.writerId, bytes);
    bridge.recordMergePass();
    await bridge.deleteScope(scopeId);
    await bridge.releaseScope(scopeId);

    expect(scopeId).toBe(7);
    expect(created).toEqual({ fileId: 9, writerId: 11 });
    expect(createFile).toHaveBeenCalledWith({ scopeId: 7 });
    expect(append).toHaveBeenCalledWith(11, bytes);
    expect(recordMergePass).toHaveBeenCalledOnce();
    expect(deleteScope).toHaveBeenCalledWith({ scopeId: 7 });
    expect(releaseScope).toHaveBeenCalledWith({ scopeId: 7 });
    uninstall();
    expect((globalThis as TestBridgeGlobal)[BRIDGE_GLOBAL]).toBeUndefined();
  });

  it('rejects a second bridge in the same worker generation', () => {
    const host = {} as OpfsSpillHost;
    installOpfsSpillBridge(host);
    expect(() => installOpfsSpillBridge(host)).toThrow('already installed');
  });
});

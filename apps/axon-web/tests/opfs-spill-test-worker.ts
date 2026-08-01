/// <reference lib="webworker" />

import {
  OpfsSpillHost,
  browserOpfsStorage,
  probeBrowserExternalMemory,
} from '../src/opfs-spill-host.ts';

const worker = self as unknown as DedicatedWorkerGlobalScope;
const FOUR_MIB = 4 * 1024 * 1024;
const CHUNK_BYTES = 256 * 1024;

void run().then(
  (result) => worker.postMessage(result),
  (error: unknown) =>
    worker.postMessage({
      available: false,
      error: error instanceof Error ? error.message : String(error),
    }),
);

async function run(): Promise<unknown> {
  const storage = browserOpfsStorage();
  const probe = await probeBrowserExternalMemory(storage);
  if (!probe.available) return probe;

  const host = await OpfsSpillHost.open(storage, {
    productionCapBytes: 64 * 1024 * 1024,
  });
  const scope = await host.createScope();
  const { file, writerId } = await host.createFile(scope);
  let expectedChecksum = 0;
  for (let offset = 0; offset < FOUR_MIB; offset += CHUNK_BYTES) {
    const chunk = new Uint8Array(CHUNK_BYTES);
    for (let index = 0; index < chunk.byteLength; index += 1) {
      const value = (offset + index) % 251;
      chunk[index] = value;
      expectedChecksum = (expectedChecksum + value) >>> 0;
    }
    await host.append(writerId, chunk);
  }
  await host.finalizeWriter(writerId);

  const readerId = await host.openReader(file);
  let actualBytes = 0;
  let actualChecksum = 0;
  for (;;) {
    const chunk = host.readNext(readerId, CHUNK_BYTES);
    if (!chunk) break;
    actualBytes += chunk.byteLength;
    for (const value of chunk) actualChecksum = (actualChecksum + value) >>> 0;
  }
  host.closeReader(readerId);
  const beforeCleanup = host.metrics();
  await host.deleteScope(scope);
  const afterCleanup = host.metrics();

  return {
    available: true,
    actualBytes,
    checksumMatches: actualChecksum === expectedChecksum,
    beforeCleanup,
    afterCleanup,
  };
}

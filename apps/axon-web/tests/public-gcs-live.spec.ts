import { expect, test } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';

const liveTableUri = process.env.AXON_LIVE_PUBLIC_GCS_TABLE_URI;
const liveOrigin = process.env.AXON_LIVE_PUBLIC_GCS_ORIGIN ?? 'http://localhost:5173';

test.describe('public GCS live smoke', () => {
  test.skip(!liveTableUri, 'set AXON_LIVE_PUBLIC_GCS_TABLE_URI to run live public GCS smoke');

  test('public GCS Delta table root supports anonymous list, log read, and range read', async ({
    request,
  }) => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'gcs',
      tableUri: liveTableUri!,
    });
    const manifest = await buildPublicDeltaLogManifest(root);
    expect(manifest.objects.length).toBeGreaterThan(0);

    const firstLog = manifest.objects.find((object) => object.relative_path.endsWith('.json'));
    expect(firstLog, 'live table must expose at least one JSON Delta log object').toBeTruthy();

    const logResponse = await request.get(firstLog!.url, {
      headers: { Origin: liveOrigin },
    });
    expect(logResponse.status()).toBe(200);
    expect(logResponse.headers()['access-control-allow-origin']).toBe(liveOrigin);

    const addPath = addPathFromDeltaLog(await logResponse.text());
    const dataResponse = await request.get(publicObjectUrl(root, addPath), {
      headers: {
        Origin: liveOrigin,
        Range: 'bytes=0-15',
      },
    });
    expect(dataResponse.status()).toBe(206);
    expect(dataResponse.headers()['content-range']).toContain('bytes 0-15/');
    expect(dataResponse.headers()['access-control-allow-origin']).toBe(liveOrigin);
    expect(
      Buffer.from(await dataResponse.body())
        .subarray(0, 4)
        .toString('utf8'),
    ).toBe('PAR1');
  });
});

function addPathFromDeltaLog(log: string): string {
  for (const line of log.split('\n')) {
    if (!line.trim()) continue;
    const action = JSON.parse(line) as { add?: { path?: unknown } };
    if (typeof action.add?.path === 'string') return action.add.path;
  }
  throw new Error('Delta log did not contain an add action');
}

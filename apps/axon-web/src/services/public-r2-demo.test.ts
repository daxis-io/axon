import { describe, expect, it } from 'vitest';
import { publicR2DemoPresetFromEnv } from './public-r2-demo.ts';

const TABLE_URI = 'r2://axon-public-data/fixtures/onboarding-v1/table';
const ENDPOINT = 'https://data.axon.daxistech.io';

describe('public R2 onboarding preset', () => {
  it('is absent when both build settings are absent', () => {
    expect(publicR2DemoPresetFromEnv({})).toBeUndefined();
  });

  it.each([
    [{ VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI: TABLE_URI }],
    [{ VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT: ENDPOINT }],
  ])('rejects partial all-or-none build settings', (env) => {
    expect(() => publicR2DemoPresetFromEnv(env)).toThrow(/set together/i);
  });

  it('exposes the production custom-domain preset when both settings are valid', () => {
    expect(
      publicR2DemoPresetFromEnv({
        VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI: `${TABLE_URI}/`,
        VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT: `${ENDPOINT}/`,
      }),
    ).toEqual({
      tableUri: TABLE_URI,
      endpoint: ENDPOINT,
    });
  });

  it.each(['https://pub-1234567890abcdef.r2.dev', 'https://qualification.example.com'])(
    'keeps the preset hidden for a non-production origin %s',
    (endpoint) => {
      expect(
        publicR2DemoPresetFromEnv({
          VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI: TABLE_URI,
          VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT: endpoint,
        }),
      ).toBeUndefined();
    },
  );

  it('rejects invalid configured values without echoing credential material', () => {
    const secret = 'TOP-SECRET-R2-CREDENTIAL';
    let message = '';
    try {
      publicR2DemoPresetFromEnv({
        VITE_AXON_PUBLIC_R2_DEMO_TABLE_URI: TABLE_URI,
        VITE_AXON_PUBLIC_R2_DEMO_ENDPOINT: `${ENDPOINT}?X-Amz-Credential=${secret}`,
      });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toMatch(/invalid public R2 demo/i);
    expect(message).not.toContain(secret);
  });
});

import { describe, expect, it } from 'vitest';

import {
  BROWSER_MEMORY_PROFILE_QUERY_PARAM,
  EXTERNAL_MEMORY_WORKING_SET_MIB,
  PREVIOUS_BROWSER_MEMORY_PROFILE_MIB,
  forwardBrowserMemoryProfile,
  browserQueryWorkerName,
  parseBrowserMemoryProfileMib,
} from './browser-memory-profile.ts';

describe('browser external-memory working-set profile', () => {
  it('accepts the 128 MiB spill profile and 64 MiB conformance/kill-switch profile', () => {
    for (const profile of [PREVIOUS_BROWSER_MEMORY_PROFILE_MIB, EXTERNAL_MEMORY_WORKING_SET_MIB]) {
      expect(
        parseBrowserMemoryProfileMib(new URLSearchParams(`browser_memory_profile_mib=${profile}`)),
      ).toBe(profile);
    }

    expect(parseBrowserMemoryProfileMib(new URLSearchParams())).toBe(
      EXTERNAL_MEMORY_WORKING_SET_MIB,
    );
  });

  it.each(['0', '96', '160', '256', 'unbounded', '128.5'])(
    'rejects nonconforming profile %s',
    (profile) => {
      expect(() =>
        parseBrowserMemoryProfileMib(new URLSearchParams(`browser_memory_profile_mib=${profile}`)),
      ).toThrow('unsupported browser memory profile');
    },
  );

  it('forwards only the selected bounded profile onto the dedicated child', () => {
    const workerUrl = new URL('https://example.test/assets/sandbox-query-worker.js');
    forwardBrowserMemoryProfile(
      new URLSearchParams('browser_memory_profile_mib=64&ignored=secret'),
      workerUrl,
    );

    expect(workerUrl.searchParams.get(BROWSER_MEMORY_PROFILE_QUERY_PARAM)).toBe('64');
    expect(workerUrl.searchParams.has('ignored')).toBe(false);
  });

  it('encodes the opt-in external-memory canary without forwarding unrelated page state', () => {
    expect(
      browserQueryWorkerName(
        new URLSearchParams(
          'browser_external_memory=enabled&browser_memory_profile_mib=128&token=secret',
        ),
      ),
    ).toBe(
      'axon-editor-query-worker?browser_memory_profile_mib=128&browser_external_memory=enabled',
    );
  });
});

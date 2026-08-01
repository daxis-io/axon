import { describe, expect, it } from 'vitest';
import { defaultCapabilityMatrix, overlayCapabilityReport } from './capabilities.ts';

describe('browser external-memory capability reporting', () => {
  it('does not advertise external memory before a worker probe reports it', () => {
    const externalMemory = defaultCapabilityMatrix().find(
      (capability) => capability.key === 'browser_external_memory',
    );

    expect(externalMemory?.browser).toBe('unsupported');
  });

  it('allows a probe-derived query report to advertise support', () => {
    const externalMemory = overlayCapabilityReport(defaultCapabilityMatrix(), {
      browser_external_memory: 'supported',
    }).find((capability) => capability.key === 'browser_external_memory');

    expect(externalMemory?.browser).toBe('supported');
  });
});

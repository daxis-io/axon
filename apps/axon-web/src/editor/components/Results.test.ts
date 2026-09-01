import { describe, expect, it } from 'vitest';
import * as ResultsModule from './Results.tsx';

type ExternalMemoryPresentation = {
  label: string;
  value: string;
  sub: string;
  title: string;
};

type ResultsTelemetryModule = {
  externalMemoryReservationPresentation?: (
    peakReservationBytes: number,
    workingSetLimitBytes: number,
  ) => ExternalMemoryPresentation;
};

describe('external-memory telemetry presentation', () => {
  it('distinguishes operator reservations from total browser memory', () => {
    const presentation = (ResultsModule as ResultsTelemetryModule)
      .externalMemoryReservationPresentation;

    expect(presentation).toEqual(expect.any(Function));
    if (!presentation) return;

    expect(presentation(120 * 1024 * 1024, 128 * 1024 * 1024)).toEqual({
      label: 'Peak operator reservations',
      value: '120.0 MB',
      sub: '128.0 MB spill watermark · includes spill headroom',
      title:
        'DataFusion-accounted operator reservations. Includes aggregate spill-sort headroom and does not represent total browser memory.',
    });
  });
});

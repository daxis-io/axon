import { describe, expect, it } from 'vitest';

import {
  PRIVATE_STREAM_PROTOCOL_VERSION,
  QueryCreditGate,
  QueryStage,
  type PrivateStreamChunk,
} from './sandbox-query-stream-protocol';

function chunk(overrides: Partial<PrivateStreamChunk> = {}): PrivateStreamChunk {
  const bytes = new Uint8Array([1, 2, 3]);
  return {
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: 'query-1',
    sequence: 0n,
    phase: 'schema',
    logical_batch_sequence: null,
    fragment_index: 0n,
    end_of_logical_batch: true,
    rows_completed: 0n,
    byte_length: 3n,
    bytes,
    ...overrides,
  };
}

describe('private Arrow IPC query staging', () => {
  it('keeps chunks private until a validated success terminal commits them', () => {
    const stage = new QueryStage('query-1');
    const schema = chunk();
    const data = chunk({
      sequence: 1n,
      phase: 'data',
      logical_batch_sequence: 0n,
      rows_completed: 2n,
      bytes: new Uint8Array([4, 5]),
      byte_length: 2n,
    });
    const eos = chunk({
      sequence: 2n,
      phase: 'end_of_stream',
      rows_completed: 2n,
      bytes: new Uint8Array([6]),
      byte_length: 1n,
    });

    expect(stage.stage(schema)).toEqual({ credit_class: 'control', byte_length: 3 });
    expect(stage.stage(data)).toEqual({ credit_class: 'data', byte_length: 2 });
    expect(stage.stage(eos)).toEqual({ credit_class: 'control', byte_length: 1 });
    const committed = stage.commit({
      metadata_version: 1,
      status: 'succeeded',
      arrow_ipc_byte_length: '6',
      row_count: '2',
    });
    expect(committed.map((part) => [...part])).toEqual([[1, 2, 3], [4, 5], [6]]);
  });

  it('discards staged ownership on failure and rejects protocol violations', () => {
    const stage = new QueryStage('query-1');
    stage.stage(chunk());
    expect(() => stage.stage(chunk({ sequence: 2n }))).toThrow(/sequence/i);
    stage.discard();
    expect(stage.stagedByteLength).toBe(0);
    expect(() => stage.stage(chunk({ sequence: 1n }))).toThrow(/discarded/i);

    const failedStage = new QueryStage('query-1');
    failedStage.stage(chunk());
    failedStage.stage(
      chunk({
        sequence: 1n,
        phase: 'end_of_stream',
        bytes: new Uint8Array([4]),
        byte_length: 1n,
      }),
    );
    expect(() =>
      failedStage.commit({
        metadata_version: 1,
        status: 'failed',
        arrow_ipc_byte_length: '4',
        row_count: '0',
      }),
    ).toThrow(/success/i);
  });

  it('requires exact-sized transferable chunk ownership', () => {
    const stage = new QueryStage('query-1');
    const backing = new Uint8Array([0, 1, 2, 3]);
    expect(() => stage.stage(chunk({ bytes: backing.subarray(1), byte_length: 3n }))).toThrow(
      /exact-sized/i,
    );
  });

  it('rejects an over-limit chunk before retaining its ownership', () => {
    const stage = new QueryStage('query-1', 3);

    expect(stage.stage(chunk())).toEqual({ credit_class: 'control', byte_length: 3 });
    expect(stage.stagedByteLength).toBe(3);
    expect(stage.peakStagedByteLength).toBe(3);
    expect(stage.stagingLimitBytes).toBe(3);

    expect(() =>
      stage.stage(
        chunk({
          sequence: 1n,
          phase: 'end_of_stream',
          bytes: new Uint8Array([4]),
          byte_length: 1n,
        }),
      ),
    ).toThrow(/staging limit/i);
    expect(stage.stagedByteLength).toBe(3);
    expect(stage.peakStagedByteLength).toBe(3);
  });
});

describe('private Arrow IPC credit gate', () => {
  it('does not permit a second data pull until staged ownership is acknowledged', async () => {
    const gate = new QueryCreditGate();
    const first = await gate.reserveAll(['data']);
    expect(first).toHaveLength(1);

    let secondPullStarted = false;
    const second = gate.reserveAll(['data']).then((reservations) => {
      secondPullStarted = true;
      return reservations;
    });
    await Promise.resolve();
    expect(secondPullStarted).toBe(false);

    const firstReservation = first?.[0];
    if (!firstReservation) throw new Error('expected initial data reservation');
    gate.commitTransfer(firstReservation, 7n, 17);
    await Promise.resolve();
    expect(secondPullStarted).toBe(false);

    expect(() => gate.addCredit('data', 6n, 17)).toThrow(/sequence/i);
    expect(() => gate.addCredit('data', 7n, 16)).toThrow(/byte/i);
    gate.addCredit('data', 7n, 17);
    expect(await second).toHaveLength(1);
  });

  it('reserves both candidate windows at a logical-batch boundary and wakes on deadline', async () => {
    const gate = new QueryCreditGate();
    const candidates = await gate.reserveAll(['data', 'control']);
    expect(candidates?.map((reservation) => reservation.creditClass)).toEqual(['data', 'control']);

    const waiting = gate.reserveAll(['data']);
    gate.cancel('deadline_exceeded');
    expect(await waiting).toBeUndefined();
    expect(gate.cancelReason).toBe('deadline_exceeded');
  });
});

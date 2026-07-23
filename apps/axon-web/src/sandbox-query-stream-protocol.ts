import type {
  BrowserWorkerCommand,
  BrowserWorkerEventEnvelope,
  QueryError,
  WireBrowserWorkerResponseEnvelope,
} from './axon-browser-sdk';

export const PRIVATE_STREAM_PROTOCOL_VERSION = 1 as const;
export const PRIVATE_STREAM_DATA_CREDIT_BYTES = 1024 * 1024;
export const PRIVATE_STREAM_CONTROL_CREDIT_BYTES = 1024 * 1024;

export type PrivateStreamPhase = 'schema' | 'data' | 'end_of_stream';
export type PrivateCreditClass = 'data' | 'control';
export type PrivateTerminalStatus = 'succeeded' | 'cancelled' | 'deadline_exceeded' | 'failed';

export type PrivateDataFusionMemoryMetrics = {
  limit_bytes: string;
  reserved_bytes: string;
  peak_bytes: string;
};

export type PrivateStreamChunk = {
  version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
  query_id: string;
  sequence: bigint;
  phase: PrivateStreamPhase;
  logical_batch_sequence: bigint | null;
  fragment_index: bigint;
  end_of_logical_batch: boolean;
  rows_completed: bigint;
  byte_length: bigint;
  bytes: Uint8Array;
};

export type PrivateTerminalMetadata = {
  metadata_version: 1;
  status: PrivateTerminalStatus;
  error?: QueryError;
  response?: unknown;
  preview?: unknown;
  arrow_ipc_byte_length: string;
  row_count: string;
  cursor_metrics?: {
    peak_input_batch_bytes: string;
    peak_pending_encoded_bytes: string;
    peak_pending_encoded_capacity: string;
    peak_transport_chunk_bytes: string;
  };
  datafusion_memory?: PrivateDataFusionMemoryMetrics;
  [key: string]: unknown;
};

export type PrivateCoordinatorMessage =
  | {
      kind: 'command';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
      command: BrowserWorkerCommand;
    }
  | {
      kind: 'credit';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
      query_id: string;
      sequence: bigint;
      credit_class: PrivateCreditClass;
      bytes: number;
    }
  | {
      kind: 'cancel';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
      query_id: string;
      reason: 'cancelled' | 'deadline_exceeded';
    };

export type PrivateChildMessage =
  | {
      kind: 'ready';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
    }
  | {
      kind: 'public';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
      envelope: WireBrowserWorkerResponseEnvelope | BrowserWorkerEventEnvelope;
    }
  | ({ kind: 'stream_chunk' } & PrivateStreamChunk)
  | {
      kind: 'stream_terminal';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
      query_id: string;
      metadata: PrivateTerminalMetadata;
    }
  | {
      kind: 'stream_start_failed' | 'stream_fault';
      version: typeof PRIVATE_STREAM_PROTOCOL_VERSION;
      query_id: string;
      error: QueryError;
    };

export type StagedCredit = {
  credit_class: PrivateCreditClass;
  byte_length: number;
};

export class QueryStageLimitError extends Error {
  readonly observedBytes: number;
  readonly limitBytes: number;

  constructor(observedBytes: number, limitBytes: number) {
    super(`private stream staging limit exceeded (${observedBytes} > ${limitBytes})`);
    this.name = 'QueryStageLimitError';
    this.observedBytes = observedBytes;
    this.limitBytes = limitBytes;
  }
}

export type CreditReservation = {
  creditClass: PrivateCreditClass;
  reservedBytes: number;
};

export class QueryCreditGate {
  #dataCredit = PRIVATE_STREAM_DATA_CREDIT_BYTES;
  #controlCredit = PRIVATE_STREAM_CONTROL_CREDIT_BYTES;
  #pendingAcknowledgements = new Map<PrivateCreditClass, { sequence: bigint; bytes: number }>();
  #waiters = new Set<() => void>();
  cancelReason: 'cancelled' | 'deadline_exceeded' | undefined;

  async reserveAll(
    creditClasses: readonly PrivateCreditClass[],
  ): Promise<CreditReservation[] | undefined> {
    if (this.cancelReason) return undefined;
    if (new Set(creditClasses).size !== creditClasses.length) {
      throw new Error('private child cannot reserve the same credit class twice');
    }
    const reservations: CreditReservation[] = [];
    for (const creditClass of creditClasses) {
      const reservation = await this.#reserve(creditClass);
      if (!reservation) {
        for (const held of reservations) this.refundUnused(held, 0);
        return undefined;
      }
      reservations.push(reservation);
    }
    return reservations;
  }

  refundUnused(reservation: CreditReservation, actualBytes: number): void {
    if (!Number.isSafeInteger(actualBytes) || actualBytes < 0) {
      throw new Error('private child credit refund bytes were invalid');
    }
    const refund = reservation.reservedBytes - actualBytes;
    if (refund < 0) throw new Error('private child transferred more than its reservation');
    this.#setCredit(reservation.creditClass, this.#credit(reservation.creditClass) + refund);
    this.#wake();
  }

  commitTransfer(reservation: CreditReservation, sequence: bigint, actualBytes: number): void {
    if (sequence < 0n) throw new Error('private child transfer sequence was invalid');
    if (this.#pendingAcknowledgements.has(reservation.creditClass)) {
      throw new Error('private child already has an unacknowledged transfer for this credit class');
    }
    this.refundUnused(reservation, actualBytes);
    this.#pendingAcknowledgements.set(reservation.creditClass, {
      sequence,
      bytes: actualBytes,
    });
  }

  addCredit(creditClass: PrivateCreditClass, sequence: bigint, bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > creditWindow(creditClass)) {
      throw new Error('private child credit acknowledgement was invalid');
    }
    const expected = this.#pendingAcknowledgements.get(creditClass);
    if (!expected) {
      throw new Error('private child credit acknowledgement had no outstanding transfer');
    }
    if (sequence !== expected.sequence) {
      throw new Error('private child credit acknowledgement sequence was invalid');
    }
    if (bytes !== expected.bytes) {
      throw new Error('private child credit acknowledgement byte count was invalid');
    }
    const next = this.#credit(creditClass) + bytes;
    if (next > creditWindow(creditClass)) {
      throw new Error('private child credit acknowledgement exceeded its window');
    }
    this.#pendingAcknowledgements.delete(creditClass);
    this.#setCredit(creditClass, next);
    this.#wake();
  }

  cancel(reason: 'cancelled' | 'deadline_exceeded'): void {
    this.cancelReason ??= reason;
    this.#wake();
  }

  async #reserve(creditClass: PrivateCreditClass): Promise<CreditReservation | undefined> {
    const full = creditWindow(creditClass);
    while (this.#credit(creditClass) < full && !this.cancelReason) {
      await new Promise<void>((resolve) => this.#waiters.add(resolve));
    }
    if (this.cancelReason) return undefined;
    this.#setCredit(creditClass, this.#credit(creditClass) - full);
    return { creditClass, reservedBytes: full };
  }

  #credit(creditClass: PrivateCreditClass): number {
    return creditClass === 'data' ? this.#dataCredit : this.#controlCredit;
  }

  #setCredit(creditClass: PrivateCreditClass, value: number): void {
    if (creditClass === 'data') this.#dataCredit = value;
    else this.#controlCredit = value;
  }

  #wake(): void {
    for (const resolve of this.#waiters) resolve();
    this.#waiters.clear();
  }
}

function creditWindow(creditClass: PrivateCreditClass): number {
  return creditClass === 'data'
    ? PRIVATE_STREAM_DATA_CREDIT_BYTES
    : PRIVATE_STREAM_CONTROL_CREDIT_BYTES;
}

export class QueryStage {
  readonly queryId: string;
  #chunks: Uint8Array[] = [];
  #expectedSequence = 0n;
  #expectedPhase: 'schema' | 'data_or_eos' | 'eos' | 'terminal' = 'schema';
  #expectedControlFragment = 0n;
  #expectedLogicalBatch = 0n;
  #activeLogicalBatch: bigint | null = null;
  #expectedDataFragment = 0n;
  #lastRowsCompleted = 0n;
  #stagedByteLength = 0;
  #peakStagedByteLength = 0;
  readonly #stagingLimitBytes: number;
  #discarded = false;
  #committed = false;

  constructor(queryId: string, stagingLimitBytes = Number.MAX_SAFE_INTEGER) {
    if (!queryId) throw new Error('private stream query id must not be empty');
    if (!Number.isSafeInteger(stagingLimitBytes) || stagingLimitBytes <= 0) {
      throw new Error('private stream staging limit must be a positive safe integer');
    }
    this.queryId = queryId;
    this.#stagingLimitBytes = stagingLimitBytes;
  }

  get stagedByteLength(): number {
    return this.#stagedByteLength;
  }

  get peakStagedByteLength(): number {
    return this.#peakStagedByteLength;
  }

  get stagingLimitBytes(): number {
    return this.#stagingLimitBytes;
  }

  stage(chunk: PrivateStreamChunk): StagedCredit {
    this.#requireMutable();
    requireProtocolVersion(chunk.version);
    if (chunk.query_id !== this.queryId) {
      throw new Error('private stream chunk query id did not match its stage');
    }
    if (chunk.sequence !== this.#expectedSequence) {
      throw new Error(
        `private stream chunk sequence ${chunk.sequence} did not match ${this.#expectedSequence}`,
      );
    }
    if (!(chunk.bytes instanceof Uint8Array)) {
      throw new Error('private stream chunk bytes must be a Uint8Array');
    }
    if (chunk.bytes.byteOffset !== 0 || chunk.bytes.byteLength !== chunk.bytes.buffer.byteLength) {
      throw new Error('private stream chunk must own an exact-sized transferable buffer');
    }
    if (chunk.byte_length !== BigInt(chunk.bytes.byteLength)) {
      throw new Error('private stream chunk byte length did not match its transferred buffer');
    }
    if (chunk.byte_length > BigInt(PRIVATE_STREAM_DATA_CREDIT_BYTES)) {
      throw new Error('private stream chunk exceeded the fixed transport limit');
    }
    if (chunk.rows_completed < this.#lastRowsCompleted) {
      throw new Error('private stream rows_completed regressed');
    }

    const creditClass = chunk.phase === 'data' ? 'data' : 'control';
    const byteLength = chunk.bytes.byteLength;
    const nextLength = this.#stagedByteLength + byteLength;
    if (!Number.isSafeInteger(nextLength)) {
      throw new Error('private stream staged byte length overflowed a safe integer');
    }
    if (nextLength > this.#stagingLimitBytes) {
      throw new QueryStageLimitError(nextLength, this.#stagingLimitBytes);
    }
    this.#validatePhase(chunk);
    this.#chunks.push(chunk.bytes);
    this.#stagedByteLength = nextLength;
    this.#peakStagedByteLength = Math.max(this.#peakStagedByteLength, nextLength);
    this.#expectedSequence += 1n;
    if (chunk.end_of_logical_batch) this.#lastRowsCompleted = chunk.rows_completed;
    return { credit_class: creditClass, byte_length: byteLength };
  }

  commit(metadata: PrivateTerminalMetadata): Uint8Array[] {
    this.#requireMutable();
    if (this.#expectedPhase !== 'terminal') {
      throw new Error('private stream cannot commit before a complete end-of-stream chunk');
    }
    requireTerminalMetadata(metadata);
    if (metadata.status !== 'succeeded') {
      throw new Error('private stream commit requires a successful terminal outcome');
    }
    const terminalByteLength = decimalBigInt(
      metadata.arrow_ipc_byte_length,
      'arrow_ipc_byte_length',
    );
    if (terminalByteLength !== BigInt(this.#stagedByteLength)) {
      throw new Error('private stream terminal byte count did not match staged ownership');
    }
    const terminalRows = decimalBigInt(metadata.row_count, 'row_count');
    if (terminalRows !== this.#lastRowsCompleted) {
      throw new Error('private stream terminal row count did not match staged chunks');
    }
    this.#committed = true;
    return this.#chunks.splice(0);
  }

  discard(): void {
    this.#chunks.length = 0;
    this.#stagedByteLength = 0;
    this.#discarded = true;
  }

  #validatePhase(chunk: PrivateStreamChunk): void {
    if (chunk.phase === 'schema') {
      if (this.#expectedPhase !== 'schema') {
        throw new Error('private stream schema chunk arrived outside the schema phase');
      }
      if (chunk.logical_batch_sequence !== null || chunk.rows_completed !== 0n) {
        throw new Error('private stream schema chunk carried data-batch accounting');
      }
      if (chunk.fragment_index !== this.#expectedControlFragment) {
        throw new Error('private stream schema fragment index was not contiguous');
      }
      if (chunk.end_of_logical_batch) {
        this.#expectedPhase = 'data_or_eos';
        this.#expectedControlFragment = 0n;
      } else {
        this.#expectedControlFragment += 1n;
      }
      return;
    }

    if (chunk.phase === 'data') {
      if (this.#expectedPhase !== 'data_or_eos') {
        throw new Error('private stream data chunk arrived outside the data phase');
      }
      if (chunk.logical_batch_sequence === null) {
        throw new Error('private stream data chunk omitted its logical batch sequence');
      }
      if (this.#activeLogicalBatch === null) {
        if (
          chunk.logical_batch_sequence !== this.#expectedLogicalBatch ||
          chunk.fragment_index !== 0n
        ) {
          throw new Error('private stream data batch or fragment sequence was not contiguous');
        }
        this.#activeLogicalBatch = chunk.logical_batch_sequence;
        this.#expectedDataFragment = 0n;
      }
      if (
        chunk.logical_batch_sequence !== this.#activeLogicalBatch ||
        chunk.fragment_index !== this.#expectedDataFragment
      ) {
        throw new Error('private stream interleaved logical batch fragments');
      }
      if (!chunk.end_of_logical_batch && chunk.rows_completed !== this.#lastRowsCompleted) {
        throw new Error('private stream reported rows before a logical batch completed');
      }
      if (chunk.end_of_logical_batch) {
        this.#activeLogicalBatch = null;
        this.#expectedLogicalBatch += 1n;
        this.#expectedDataFragment = 0n;
      } else {
        this.#expectedDataFragment += 1n;
      }
      return;
    }

    if (this.#expectedPhase !== 'data_or_eos' && this.#expectedPhase !== 'eos') {
      throw new Error('private stream EOS chunk arrived outside the EOS phase');
    }
    if (this.#activeLogicalBatch !== null || chunk.logical_batch_sequence !== null) {
      throw new Error('private stream EOS interleaved a logical data batch');
    }
    if (chunk.fragment_index !== this.#expectedControlFragment) {
      throw new Error('private stream EOS fragment index was not contiguous');
    }
    if (chunk.rows_completed !== this.#lastRowsCompleted) {
      throw new Error('private stream EOS row count did not match completed data');
    }
    if (chunk.end_of_logical_batch) {
      this.#expectedPhase = 'terminal';
      this.#expectedControlFragment = 0n;
    } else {
      this.#expectedPhase = 'eos';
      this.#expectedControlFragment += 1n;
    }
  }

  #requireMutable(): void {
    if (this.#discarded) throw new Error('private stream stage was discarded');
    if (this.#committed) throw new Error('private stream stage was already committed');
  }
}

export function requireProtocolVersion(version: number): void {
  if (version !== PRIVATE_STREAM_PROTOCOL_VERSION) {
    throw new Error(`unsupported private stream protocol version '${String(version)}'`);
  }
}

export function requireTerminalMetadata(value: unknown): asserts value is PrivateTerminalMetadata {
  if (!value || typeof value !== 'object') {
    throw new Error('private stream terminal metadata must be an object');
  }
  const metadata = value as Partial<PrivateTerminalMetadata>;
  if (metadata.metadata_version !== 1) {
    throw new Error('unsupported private stream terminal metadata version');
  }
  if (
    metadata.status !== 'succeeded' &&
    metadata.status !== 'cancelled' &&
    metadata.status !== 'deadline_exceeded' &&
    metadata.status !== 'failed'
  ) {
    throw new Error('private stream terminal status was invalid');
  }
  decimalBigInt(metadata.arrow_ipc_byte_length, 'arrow_ipc_byte_length');
  decimalBigInt(metadata.row_count, 'row_count');
  if (metadata.datafusion_memory !== undefined) {
    if (!metadata.datafusion_memory || typeof metadata.datafusion_memory !== 'object') {
      throw new Error('private stream datafusion_memory must be an object');
    }
    decimalBigInt(metadata.datafusion_memory.limit_bytes, 'datafusion_memory.limit_bytes');
    decimalBigInt(metadata.datafusion_memory.reserved_bytes, 'datafusion_memory.reserved_bytes');
    decimalBigInt(metadata.datafusion_memory.peak_bytes, 'datafusion_memory.peak_bytes');
  }
}

export function decimalBigInt(value: unknown, field: string): bigint {
  if (typeof value !== 'string' || !/^(0|[1-9]\d*)$/.test(value)) {
    throw new Error(`private stream ${field} must be an unsigned decimal string`);
  }
  return BigInt(value);
}

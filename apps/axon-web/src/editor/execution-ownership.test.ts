import { create } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';
import { ExecuteRequestSchema } from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  activeEditorExecutionId,
  cancelActiveEditorExecution,
  claimActiveEditorExecution,
  displaceActiveEditorExecution,
  editorExecutionController,
  executionMayPublish,
  releaseActiveEditorExecution,
} from './execution-ownership.ts';

describe('editor execution ownership', () => {
  it('tombstones an execution held in resolution before it can open or publish', async () => {
    const prepared = editorExecutionController.prepare({ timeoutMs: 120_000 });
    if (prepared.kind === 'rejected') throw new Error('expected an execution reservation');
    const { execution } = prepared;
    const abort = vi.fn();
    const openSession = vi.fn();
    const runQuery = vi.fn();
    const publishResults = vi.fn();
    const appendHistory = vi.fn();
    let releaseResolution!: () => void;
    const resolutionGate = new Promise<void>((resolve) => {
      releaseResolution = resolve;
    });
    editorExecutionController.attachCancellation(execution.executionId, abort);
    claimActiveEditorExecution(execution.executionId);
    const pendingExecution = (async () => {
      await resolutionGate;
      const admission = editorExecutionController.admit(
        create(ExecuteRequestSchema, {
          executionId: execution.executionId,
          deadline: execution.deadline,
        }),
      );
      if (
        admission.admission.outcome.case !== 'accepted' ||
        !admission.admission.outcome.value.launch
      ) {
        return;
      }
      openSession();
      runQuery();
      publishResults();
      appendHistory();
    })();

    expect(displaceActiveEditorExecution()).toBe(true);
    expect(executionMayPublish(execution.executionId)).toBe(false);
    releaseResolution();
    await pendingExecution;

    expect(abort).toHaveBeenCalledTimes(1);
    expect(editorExecutionController.lifecycle.getSnapshot(execution.executionId)).toMatchObject({
      state: 'rejected',
      admitted: false,
    });
    expect(openSession).not.toHaveBeenCalled();
    expect(runQuery).not.toHaveBeenCalled();
    expect(publishResults).not.toHaveBeenCalled();
    expect(appendHistory).not.toHaveBeenCalled();
    releaseActiveEditorExecution(execution.executionId);
    expect(activeEditorExecutionId()).toBeUndefined();
  });

  it('keeps ordinary user cancellation distinct from connection displacement', () => {
    const prepared = editorExecutionController.prepare({ timeoutMs: 120_000 });
    if (prepared.kind === 'rejected') throw new Error('expected an execution reservation');
    const { execution } = prepared;
    editorExecutionController.attachCancellation(execution.executionId, vi.fn());
    claimActiveEditorExecution(execution.executionId);

    expect(cancelActiveEditorExecution()).toBe(true);
    expect(executionMayPublish(execution.executionId)).toBe(true);

    releaseActiveEditorExecution(execution.executionId);
  });
});

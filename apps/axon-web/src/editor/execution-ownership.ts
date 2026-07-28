import { ExecutionLifecycleState } from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  cancelExecutionRequest,
  createExecutionController,
} from '../services/execution-lifecycle.ts';

type ActiveEditorExecution = {
  executionId: string;
  displaced: boolean;
  onCancellationRequested?: (executionId: string) => void;
};

export const editorExecutionController = createExecutionController();

let activeEditorExecution: ActiveEditorExecution | undefined;

export function claimActiveEditorExecution(
  executionId: string,
  onCancellationRequested?: (executionId: string) => void,
): void {
  activeEditorExecution = { executionId, displaced: false, onCancellationRequested };
}

export function activeEditorExecutionId(): string | undefined {
  return activeEditorExecution?.executionId;
}

export function cancelActiveEditorExecution(): boolean {
  return cancelActiveEditorExecutionOwner(false);
}

export function displaceActiveEditorExecution(): boolean {
  return cancelActiveEditorExecutionOwner(true);
}

export function executionMayPublish(executionId: string): boolean {
  return activeEditorExecution?.executionId === executionId && !activeEditorExecution.displaced;
}

function cancelActiveEditorExecutionOwner(displaced: boolean): boolean {
  const active = activeEditorExecution;
  if (!active) return false;
  if (displaced) active.displaced = true;
  const cancellation = editorExecutionController.cancel(cancelExecutionRequest(active.executionId));
  if (cancellation.state === ExecutionLifecycleState.CANCEL_REQUESTED) {
    active.onCancellationRequested?.(active.executionId);
  }
  return true;
}

export function releaseActiveEditorExecution(executionId: string): void {
  if (activeEditorExecution?.executionId === executionId) {
    activeEditorExecution = undefined;
  }
}

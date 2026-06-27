import type { BrowserWorkerEventEnvelope } from '../axon-browser-sdk.ts';
import type { Catalog } from './types.ts';
import { sameQuerySource, type QueryTableSource } from './query-source.ts';

export type QueryRuntimeManifestObject = {
  relative_path: string;
  url_path: string;
  kind?: string;
  size_bytes?: number;
  etag?: string;
};

export type QueryRuntimeManifest = {
  objects: QueryRuntimeManifestObject[];
};

export type QueryRuntimeState = {
  source: QueryTableSource;
  catalog: Catalog;
  manifest?: QueryRuntimeManifest;
};

type RuntimeStateSubscriber = {
  source?: QueryTableSource;
  listener: (state: QueryRuntimeState) => void;
};

type WorkerEventHandler = (event: BrowserWorkerEventEnvelope) => void;

let currentState: QueryRuntimeState | undefined;
let coldStartMs: number | undefined;
const stateSubscribers = new Set<RuntimeStateSubscriber>();
const workerEventSubscribers = new Set<WorkerEventHandler>();

export function publishQueryRuntimeState(state: QueryRuntimeState, coldStart: number): void {
  currentState = state;
  coldStartMs = coldStart;
  for (const subscriber of stateSubscribers) {
    if (!subscriber.source || sameQuerySource(subscriber.source, state.source)) {
      subscriber.listener(state);
    }
  }
}

export function clearQueryRuntimeState(source?: QueryTableSource): void {
  if (!currentState || (source && !sameQuerySource(currentState.source, source))) {
    return;
  }
  currentState = undefined;
  coldStartMs = undefined;
}

export function getQueryRuntimeState(source: QueryTableSource): QueryRuntimeState | undefined {
  if (!currentState || !sameQuerySource(currentState.source, source)) return undefined;
  return currentState;
}

export function subscribeQueryRuntimeState(
  listener: (state: QueryRuntimeState) => void,
  source?: QueryTableSource,
): () => void {
  const subscriber = { source, listener };
  stateSubscribers.add(subscriber);
  if (currentState && (!source || sameQuerySource(source, currentState.source))) {
    listener(currentState);
  }
  return () => {
    stateSubscribers.delete(subscriber);
  };
}

export function getColdStartMs(): number | undefined {
  return coldStartMs;
}

export function publishWorkerEvent(event: BrowserWorkerEventEnvelope): void {
  for (const handler of workerEventSubscribers) handler(event);
}

export function subscribeWorkerEvents(handler: WorkerEventHandler): () => void {
  workerEventSubscribers.add(handler);
  return () => {
    workerEventSubscribers.delete(handler);
  };
}

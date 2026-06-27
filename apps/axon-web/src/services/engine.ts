import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  getPlatformFeatures,
  selectBundle,
  type BrowserWorkerCacheMetricsEvent,
} from '../axon-browser-sdk.ts';
import {
  getColdStartMs,
  subscribeQueryRuntimeState,
  subscribeWorkerEvents,
} from './query-runtime-state.ts';
import type { EngineStatus } from './types.ts';

// Engine status combines compile-time data (bundle selection) with runtime
// observations (cold-start timing from query.ts, cache_metrics events from the
// worker). Cache budget defaults to the worker's max_session_cached_bytes when
// it arrives in the first cache_metrics event.

let latestCache: BrowserWorkerCacheMetricsEvent | undefined;
let subscribed = false;
const subscribers = new Set<(status: EngineStatus) => void>();

function bundleSnapshot() {
  const features = getPlatformFeatures();
  const selection = selectBundle(AXON_BROWSER_BUNDLE_MANIFEST, features);
  return {
    bundle:
      typeof selection.bundle.wasmUrl === 'string'
        ? (selection.bundle.wasmUrl.split('/').pop() ?? selection.bundle.id)
        : selection.bundle.id,
    bundle_tier: selection.bundle.tier,
    available_tiers: AXON_BROWSER_BUNDLE_MANIFEST.bundles.map((b) => b.tier),
    active_tier: selection.bundle.tier,
  };
}

export function currentEngineStatus(): EngineStatus {
  const bundle = bundleSnapshot();
  const cache = latestCache;
  const cacheBudgetBytes = cache?.max_session_cached_bytes ?? 0;
  const cacheUsedBytes = cache?.session_cached_bytes ?? 0;
  return {
    ...bundle,
    wasm_size_kb: 0, // Surfaced via worker handshake — see Phase 3 TODO
    cold_start_ms: getColdStartMs() ?? 0,
    worker_mem_mb: 0, // No worker-side instrumentation yet
    cache: {
      opfs_used_mb: cacheUsedBytes / (1024 * 1024),
      opfs_budget_mb: cacheBudgetBytes / (1024 * 1024),
      memory_mb: 0,
      extents: cache?.session_table_count ?? 0,
      hit_ratio: cache?.transport
        ? cache.transport.bytes_reused /
          Math.max(1, cache.transport.bytes_reused + cache.transport.validation_misses)
        : 0,
    },
    proto: 'DataFusion · Delta Lake',
  };
}

export function subscribeEngineStatus(listener: (status: EngineStatus) => void): () => void {
  ensureSubscribed();
  subscribers.add(listener);
  listener(currentEngineStatus());
  return () => {
    subscribers.delete(listener);
  };
}

function ensureSubscribed() {
  if (subscribed) return;
  subscribed = true;

  subscribeQueryRuntimeState(() => {
    notify();
  });

  subscribeWorkerEvents((event) => {
    if ('cache_metrics' in event) {
      latestCache = event.cache_metrics;
      notify();
    }
  });
}

function notify() {
  const status = currentEngineStatus();
  subscribers.forEach((fn) => fn(status));
}

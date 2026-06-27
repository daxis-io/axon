const activeLocalDeltaRuntimeIds = new Set<string>();

export function markLocalDeltaRuntimeActive(registryId: string): void {
  activeLocalDeltaRuntimeIds.add(registryId);
}

export function markLocalDeltaRuntimeInactive(registryId: string): void {
  activeLocalDeltaRuntimeIds.delete(registryId);
}

export function hasLocalDeltaRuntime(registryId?: string): boolean {
  return registryId ? activeLocalDeltaRuntimeIds.has(registryId) : false;
}

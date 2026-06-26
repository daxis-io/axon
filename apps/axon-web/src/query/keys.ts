export const queryKeys = {
  catalog: {
    root: (connectionId: string) => ['catalog', connectionId] as const,
  },
  local: {
    history: () => ['local', 'history'] as const,
    saved: () => ['local', 'saved'] as const,
  },
} as const;

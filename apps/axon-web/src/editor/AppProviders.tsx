import type { QueryClient } from '@tanstack/react-query';
import { PersistQueryClientProvider } from '@tanstack/react-query-persist-client';
import { useEffect, useMemo, type ReactNode } from 'react';
import { createAxonQueryPersistOptions, queryClient as defaultQueryClient } from '../query';
import { installCatalogQueryBridge } from '../query/catalog.ts';

type AppProvidersProps = {
  children: ReactNode;
  queryClient?: QueryClient;
};

export function AppProviders({ children, queryClient = defaultQueryClient }: AppProvidersProps) {
  useEffect(() => installCatalogQueryBridge(queryClient), [queryClient]);
  const persistOptions = useMemo(() => createAxonQueryPersistOptions(), []);

  return (
    <PersistQueryClientProvider client={queryClient} persistOptions={persistOptions}>
      {children}
    </PersistQueryClientProvider>
  );
}

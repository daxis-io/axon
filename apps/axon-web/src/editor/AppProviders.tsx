import type { QueryClient } from '@tanstack/react-query';
import { QueryClientProvider } from '@tanstack/react-query';
import { useEffect, type ReactNode } from 'react';
import { queryClient as defaultQueryClient } from '../query';
import { installCatalogQueryBridge } from '../query/catalog.ts';

type AppProvidersProps = {
  children: ReactNode;
  queryClient?: QueryClient;
};

export function AppProviders({ children, queryClient = defaultQueryClient }: AppProvidersProps) {
  useEffect(() => installCatalogQueryBridge(queryClient), [queryClient]);

  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

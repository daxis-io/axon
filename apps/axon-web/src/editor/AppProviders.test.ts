import type { QueryClient } from '@tanstack/react-query';
import { afterEach, describe, expect, it, vi } from 'vitest';

type EffectCleanup = () => void;
type EffectCallback = () => void | EffectCleanup;

describe('AppProviders', () => {
  afterEach(() => {
    vi.doUnmock('react');
    vi.doUnmock('../query');
    vi.doUnmock('../query/catalog.ts');
    vi.resetModules();
    vi.restoreAllMocks();
  });

  it('wires persisted query options through the provider and preserves catalog bridge cleanup', async () => {
    const cleanupBridge = vi.fn();
    const installCatalogQueryBridge = vi.fn(() => cleanupBridge);
    const persistOptions = {
      buster: 'test-buster',
      maxAge: 1000,
      persister: {
        persistClient: vi.fn(),
        restoreClient: vi.fn(),
        removeClient: vi.fn(),
      },
      dehydrateOptions: {},
    };
    const createAxonQueryPersistOptions = vi.fn(() => persistOptions);
    const defaultQueryClient = { id: 'default-client' } as unknown as QueryClient;
    const injectedQueryClient = { id: 'injected-client' } as unknown as QueryClient;
    const effectCleanups: EffectCleanup[] = [];
    const useEffect = vi.fn((effect: EffectCallback) => {
      const cleanup = effect();
      if (typeof cleanup === 'function') {
        effectCleanups.push(cleanup);
      }
    });
    const useMemo = vi.fn((factory: () => unknown) => factory());

    vi.doMock('react', async (importOriginal) => ({
      ...(await importOriginal<typeof import('react')>()),
      useEffect,
      useMemo,
    }));
    vi.doMock('../query', () => ({
      createAxonQueryPersistOptions,
      queryClient: defaultQueryClient,
    }));
    vi.doMock('../query/catalog.ts', () => ({
      installCatalogQueryBridge,
    }));

    const { AppProviders } = await import('./AppProviders.tsx');
    const element = AppProviders({
      children: 'child',
      queryClient: injectedQueryClient,
    }) as {
      props: {
        children: unknown;
        client: QueryClient;
        persistOptions: typeof persistOptions;
      };
    };

    expect(useEffect).toHaveBeenCalledWith(expect.any(Function), [injectedQueryClient]);
    expect(useMemo).toHaveBeenCalledWith(expect.any(Function), []);
    expect(installCatalogQueryBridge).toHaveBeenCalledExactlyOnceWith(injectedQueryClient);
    expect(createAxonQueryPersistOptions).toHaveBeenCalledOnce();
    expect(element.props.client).toBe(injectedQueryClient);
    expect(element.props.persistOptions).toBe(persistOptions);
    expect(element.props.children).toBe('child');

    expect(effectCleanups).toHaveLength(1);
    effectCleanups[0]?.();
    expect(cleanupBridge).toHaveBeenCalledOnce();
  });
});

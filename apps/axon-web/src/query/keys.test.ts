import { describe, expect, it } from 'vitest';
import { queryKeys } from './keys';

describe('queryKeys', () => {
  it('builds catalog root keys scoped by connection id', () => {
    expect(queryKeys.catalog.root('connection-1')).toEqual(['catalog', 'root', 'connection-1']);
  });

  it('builds local workspace keys without product data fetch behavior', () => {
    expect(queryKeys.local.history()).toEqual(['local', 'history']);
    expect(queryKeys.local.saved()).toEqual(['local', 'saved']);
  });
});

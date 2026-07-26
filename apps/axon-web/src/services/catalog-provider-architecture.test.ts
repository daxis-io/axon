import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const providerSource = readFileSync(
  fileURLToPath(new URL('./catalog-provider.ts', import.meta.url)),
  'utf8',
);

describe('CatalogProvider architecture boundary', () => {
  it('depends only on generated catalog/common contracts and canonical identity', () => {
    expect(providerSource).toContain(
      '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts',
    );
    expect(providerSource).toContain('../generated/contracts/protobuf/axon/common/v1/common_pb.ts');
    expect(providerSource).toContain("./canonical-table-identity.ts'");
  });

  it.each([
    ['React', /from ['"]react/],
    ['TanStack Query', /@tanstack\/react-query/],
    ['data-access contracts', /dataaccess\/v1/],
    ['execution contracts', /exec\/v1/],
    ['browser read resolution', /browser-read-resolution/],
    ['browser execution', /browser-execution/],
    ['browser descriptors', /BrowserHttpSnapshotDescriptor|BrowserHttpFileDescriptor/],
    ['sessions', /\bsession\b/i],
    ['credentials', /\bcredentials?\b/i],
    ['grants', /\bgrants?\b/i],
    ['workers', /\bworkers?\b/i],
    ['SDK table opens', /\bopenDeltaTable\b/],
    ['generic page mirrors', /\bPage\s*</],
    ['Unity Catalog', /\bUnity\s+Catalog\b|\bunity_catalog\b/i],
    ['Delta Sharing', /\bDelta\s+Sharing\b|\bdelta_share\b/i],
    ['ABFSS', /\babfss\b/i],
    ['R2', /\br2\b/i],
    ['object-storage acquisition', /from ['"].*object-storage/],
    ['persistence', /from ['"].*persistence|localStorage|indexedDB/],
    ['logging', /\bconsole\.|\blogger\b/],
    ['codegen implementation', /contracts-codegen|generate-contracts|scripts\/generate/],
  ])('does not admit %s below discovery', (_label, forbidden) => {
    expect(providerSource).not.toMatch(forbidden);
  });
});

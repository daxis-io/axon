import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const explorerSources = ['CatalogsPage.tsx', 'catalog-navigation.ts', 'router.tsx'].map((file) => ({
  file,
  source: readFileSync(fileURLToPath(new URL(`./${file}`, import.meta.url)), 'utf8'),
}));

const forbiddenImports = [
  'generated/contracts/protobuf/axon/dataaccess',
  'generated/contracts/protobuf/axon/exec',
  'browser-read-resolution',
  'browser-execution',
  'services/query.ts',
  'worker',
  'session',
];

describe('Catalog Explorer architecture boundary', () => {
  it.each(explorerSources)('$file does not import access resolution or execution', ({ source }) => {
    const imports = [...source.matchAll(/from\s+['"]([^'"]+)['"]/g)].map((match) => match[1] ?? '');

    for (const forbidden of forbiddenImports) {
      expect(
        imports.some((value) => value.includes(forbidden)),
        `forbidden Explorer import containing ${forbidden}`,
      ).toBe(false);
    }
    expect(source).not.toMatch(/\bopenDeltaTable\s*\(/);
  });

  it('keeps the Explorer page on generated metadata and logical navigation only', () => {
    const page = explorerSources.find(({ file }) => file === 'CatalogsPage.tsx')!.source;

    expect(page).not.toMatch(
      /catalogQueryOptions|resolveQuerySourceSelection|descriptor|signedUrl/,
    );
    expect(page).toContain('catalogExplorerTableDetail');
    expect(page).toContain('Open in SQL editor');
  });
});

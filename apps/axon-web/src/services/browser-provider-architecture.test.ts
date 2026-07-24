import { readdirSync, readFileSync } from 'node:fs';
import { extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const sourceRoot = fileURLToPath(new URL('..', import.meta.url));

function sourceFiles(relativeRoot: string): string[] {
  const root = join(sourceRoot, relativeRoot);
  const files: string[] = [];
  const visit = (directory: string) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(path);
      } else if (extname(entry.name) === '.ts' || extname(entry.name) === '.tsx') {
        files.push(path);
      }
    }
  };
  visit(root);
  return files;
}

describe('browser provider architecture guard', () => {
  it('keeps SDK table opening out of query, state, and editor application code', () => {
    for (const file of [
      ...sourceFiles('query'),
      ...sourceFiles('state'),
      ...sourceFiles('editor'),
    ]) {
      const source = readFileSync(file, 'utf8');
      expect(source, file).not.toMatch(
        /\.open(?:DeltaTable|ParquetDataset|DeltaLocation|DeltaShare|UnityCatalogTable)\s*\(/,
      );
      expect(source, file).not.toMatch(/\bcreateAxonBrowserClient\s*\(/);
    }
  });

  it('allows one SDK Delta open only after executor-envelope validation', () => {
    const query = readFileSync(join(sourceRoot, 'services/query.ts'), 'utf8');
    const opens = [...query.matchAll(/\.openDeltaTable\s*\(/g)];

    expect(opens).toHaveLength(1);
    expect(query).not.toMatch(/\.openParquetDataset\s*\(/);
    expect(query.indexOf('validateBrowserExecuteInput(input')).toBeLessThan(opens[0]!.index);
    expect(query).toContain('createValidatedBrowserExecutionProvider(delegate');
    expect(query).toContain('for await (const response of provider.execute(input))');
  });

  it('routes cancellation and supported worker events through the generated provider stream', () => {
    const query = readFileSync(join(sourceRoot, 'services/query.ts'), 'utf8');

    expect(query).toContain('provider.cancel(cancelExecutionRequest(executionId))');
    expect(query).toContain("item: { case: 'event'");
    expect(query).toContain("response.item.case === 'event'");
  });

  it('prevents removed handwritten execution-contract mirrors from returning', () => {
    const prohibited = [
      'QueryExecRequest',
      'ExecutionAdmissionInput',
      'ExecutionBudgets',
      'SelectedQuerySourceIdentity',
    ];
    for (const file of [
      ...sourceFiles('query'),
      ...sourceFiles('state'),
      ...sourceFiles('editor'),
      ...sourceFiles('services'),
    ]) {
      if (file.endsWith('browser-provider-architecture.test.ts')) continue;
      const source = readFileSync(file, 'utf8');
      for (const name of prohibited) {
        expect(source, `${name} in ${file}`).not.toContain(name);
      }
    }
  });
});

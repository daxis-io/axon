import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  vercelRewriteDestination,
  vercelSourcePattern,
  type VercelRoutingConfig,
} from '../scripts/vercel-routing.ts';
import { catalogTablePath, catalogTableSqlPath } from './editor/catalog-navigation.ts';
import { editorRouteTemplates } from './editor/router.tsx';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
  createSampleFixtureCanonicalTable,
} from './services/canonical-table-identity.ts';

const vercel = JSON.parse(
  readFileSync(resolve(import.meta.dirname, '../../../vercel.json'), 'utf8'),
) as VercelRoutingConfig;

// A value that exercises the encodings real route params carry: scheme separators and slashes.
const ENCODED_PARAM = encodeURIComponent('axon-connection://fixture/sample-lake');

describe('production host routing (vercel.json)', () => {
  it('rewrites every editor route template to the SPA entry so reloads and shared links load', () => {
    const uncovered = Object.entries(editorRouteTemplates)
      // The root is served by index.html itself; every other route only reaches the app through a
      // rewrite, and Vercel answers a non-matching path with NOT_FOUND.
      .filter(([, template]) => template !== '/')
      .map(([name, template]) => ({
        name,
        path: template.replace(/\$[A-Za-z]+/g, ENCODED_PARAM),
      }))
      .filter(({ path }) => vercelRewriteDestination(vercel, path) !== '/index.html');

    expect(uncovered).toEqual([]);
  });

  it('rewrites the canonical table paths the app pushes when a table is selected', () => {
    const tables = [
      createSampleFixtureCanonicalTable('events'),
      createLocalDeltaCanonicalTable({ registryId: 'local registry/id', tableName: 'events' }),
      createPublicObjectStorageCanonicalTable({
        provider: 'gcs',
        connectionId: 'axon-connection://public-gcs/public-bucket',
        normalizedTableUri: 'gs://public-bucket/sales%20ops/events',
        tableName: 'events',
      }),
    ];

    for (const table of tables) {
      for (const path of [catalogTablePath(table), catalogTableSqlPath(table)]) {
        expect(vercelRewriteDestination(vercel, path), path).toBe('/index.html');
      }
    }
  });

  it('keeps missing build assets as 404s instead of answering them with the SPA', () => {
    for (const path of [
      '/assets/sandbox-query-child-worker-deadbeef.js',
      '/assets/axon_web_wasm_bg-deadbeef.wasm',
      '/fixtures/prod-like/missing.json',
    ]) {
      expect(vercelRewriteDestination(vercel, path), path).toBeUndefined();
    }
  });

  it('evaluates path-to-regexp sources and rejects syntax it cannot evaluate', () => {
    expect(vercelSourcePattern('/catalog/:path*').test('/catalog/a/b/c/d/e/sql')).toBe(true);
    expect(vercelSourcePattern('/catalog/:path*').test('/catalogs')).toBe(false);
    expect(vercelSourcePattern('/saved/:savedId').test('/saved/a/b')).toBe(false);
    expect(vercelSourcePattern('/(.*).wasm').test('/assets/engine-1a2b.wasm')).toBe(true);
    expect(() => vercelSourcePattern('/catalog/:id(\\d+)')).toThrow(/unsupported/);
    expect(() => vercelSourcePattern('catalog')).toThrow(/unsupported/);
  });
});

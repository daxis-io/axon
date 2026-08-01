import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const editorSmokeSource = readFileSync(
  fileURLToPath(new URL('../tests/editor-smoke.spec.ts', import.meta.url)),
  'utf8',
);
const nativeOracleSource = readFileSync(
  fileURLToPath(
    new URL(
      '../../../crates/native-query-runtime/examples/generate_stress_aggregate_oracle.rs',
      import.meta.url,
    ),
  ),
  'utf8',
);

describe('browser external-memory release contract', () => {
  it('uses the same ordered SQL source for browser execution and the native oracle', () => {
    expect(editorSmokeSource).toContain('STRESS_AGGREGATE_SQL');
    expect(nativeOracleSource).toContain('stress-aggregate.sql');

    const sql = readFileSync(
      fileURLToPath(
        new URL('../tests/fixtures/browser-external-memory/stress-aggregate.sql', import.meta.url),
      ),
      'utf8',
    );
    expect(sql).toMatch(/GROUP BY event_id\s+ORDER BY event_id\s*$/i);
  });
});

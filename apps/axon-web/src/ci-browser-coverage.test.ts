import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import productionBundleConfig from '../playwright.production-bundle.config.ts';

const app = resolve(import.meta.dirname, '..');
const ci = readFileSync(resolve(app, '../../.github/workflows/ci.yml'), 'utf8');
const scripts = (
  JSON.parse(readFileSync(resolve(app, 'package.json'), 'utf8')) as {
    scripts: Record<string, string>;
  }
).scripts;

// The `run:` commands of the job that builds the app, in order.
function buildJobCommands(): string[] {
  const job = ci.slice(ci.indexOf('\n  browser-external-memory-artifact:') + 1);
  // The job ends where the next two-space-indented job key begins.
  const end = job.slice(1).search(/\n {2}\S/);
  return [...job.slice(0, end === -1 ? undefined : end + 1).matchAll(/run: (.+)/g)].map((match) =>
    match[1].trim(),
  );
}

describe('CI coverage of the shipped browser bundle', () => {
  it('runs the SDK suite and a real query against the built bundle after the build', () => {
    const commands = buildJobCommands();
    const build = commands.indexOf('npm run build');
    expect(build).toBeGreaterThanOrEqual(0);
    expect(commands.indexOf('npm run test:sdk')).toBeGreaterThan(build);
    expect(commands.indexOf('npm run test:browser:production-bundle')).toBeGreaterThan(build);
    expect(scripts['test:sdk']).toBe('playwright test --config=playwright.sdk.config.ts');
    expect(scripts['test:browser:production-bundle']).toBe(
      'playwright test --config=playwright.production-bundle.config.ts',
    );
  });

  it('serves the built dist with vite preview rather than the dev server', () => {
    const webServer = productionBundleConfig.webServer;
    expect(Array.isArray(webServer)).toBe(false);
    if (!webServer || Array.isArray(webServer)) throw new Error('expected one web server');
    expect(webServer.command).toMatch(/^npm exec -- vite preview /);
    expect(webServer.reuseExistingServer).toBe(false);
    expect(webServer.env).toMatchObject({ AXON_BROWSER_MEMORY_EVIDENCE: '0' });
    expect(productionBundleConfig.testMatch).toEqual(/production-bundle\.spec\.ts/);
  });
});

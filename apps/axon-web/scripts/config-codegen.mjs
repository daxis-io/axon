import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export const GENERATED_CONFIG_DIR = 'src/generated/config';

export function runConfigCodegen({ output = '.' } = {}) {
  execFileSync('buf', ['generate', '--output', output], { stdio: 'inherit' });
  normalizeGeneratedConfig(join(output, GENERATED_CONFIG_DIR));
}

export function normalizeGeneratedConfig(configDir = GENERATED_CONFIG_DIR) {
  normalizeGeneratedTypescript(join(configDir, 'protobuf'));
}

function normalizeGeneratedTypescript(root) {
  if (!existsSync(root)) {
    return;
  }

  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const child = join(root, entry.name);
    if (entry.isDirectory()) {
      normalizeGeneratedTypescript(child);
      continue;
    }
    if (!entry.name.endsWith('.ts')) {
      continue;
    }

    const current = readFileSync(child, 'utf8');
    const normalized = current.replace(/\n*$/u, '\n');
    if (normalized !== current) {
      writeFileSync(child, normalized);
    }
  }
}

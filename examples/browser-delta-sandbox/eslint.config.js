import js from '@eslint/js';
import tseslint from 'typescript-eslint';

export default [
  {
    ignores: [
      'node_modules/**',
      'dist/**',
      'playwright-report/**',
      'test-results/**',
      'src/wasm/**',
      'public/fixtures/prod-like/**',
      'public/designs/**',
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['**/*.{js,ts,tsx}'],
    rules: {
      // TypeScript already checks browser and Node globals from tsconfig libs/types.
      'no-undef': 'off',
    },
  },
];

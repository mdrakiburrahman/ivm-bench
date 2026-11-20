import js from '@eslint/js';
import ts from 'typescript-eslint';

export default ts.config(
  js.configs.recommended,
  ...ts.configs.recommended,
  { files: ['**/*.mjs'], languageOptions: { globals: { URL: 'readonly', process: 'readonly', console: 'readonly' } } },
);

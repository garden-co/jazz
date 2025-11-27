# Code Snippets TypeScript Configuration

This directory contains code snippets for documentation examples. The snippets use framework-specific import aliases (like `@/app/*` for Next.js and `$lib/*` for SvelteKit) that need special TypeScript configuration to resolve correctly.

## How It Works

### Auto-Generated Path Mappings

A script (`scripts/generate-snippet-paths.mjs`) automatically scans all subdirectories and:
1. Detects import aliases used in the code (e.g., `@/app/components/*`, `$lib/*`)
2. Generates TypeScript path mappings for each alias
3. Creates two tsconfig files:
   - `tsconfig.snippets.json` (in homepage root) - for CLI typechecking
   - `tsconfig.json` (in this directory) - for IDE support

### Running the Generator

The generator runs automatically before typechecking:

```bash
pnpm generate:snippet-paths  # Run manually
pnpm check                    # Runs generator + typecheck
```

### Adding New Snippet Directories

Simply create a new directory under `code-snippets/` with your TypeScript/Svelte files. The generator will automatically detect and configure any import aliases you use.

### Framework-Specific Imports

Some framework imports (like `$env/static/public` or `$app/state` in Svelte) are intentionally skipped and should use `// @ts-expect-error` comments, as they're provided by the framework at runtime.

## Generated Files

- `tsconfig.json` - Auto-generated local config for IDE support (DO NOT EDIT MANUALLY)
- `../../../tsconfig.snippets.json` - Auto-generated root config for CLI (DO NOT EDIT MANUALLY)

Both files are regenerated each time you run `pnpm generate:snippet-paths` or `pnpm check`.


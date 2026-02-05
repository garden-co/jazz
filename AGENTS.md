# AGENTS.md

Guidelines for AI agents working on the Jazz codebase.

## Project Overview

Jazz is a distributed database framework for building local-first apps. It syncs data across frontends, serverless functions, and backend servers with real-time collaboration, offline support, and end-to-end encryption.

- **Monorepo**: pnpm workspaces with Turbo build orchestration
- **Languages**: TypeScript (primary), Rust (performance-critical CRDT code in `crates/`)
- **Node.js**: 22.18.0+ required
- **Package Manager**: pnpm 10.16.1

## Quick Reference

```bash
pnpm install              # Install dependencies
pnpm build:packages       # Build TypeScript packages
pnpm build:core           # Build native packages
pnpm build:all-packages   # Build everything
pnpm test                 # Run tests (Vitest, watch mode)
pnpm test --watch=false   # Run tests without watch
pnpm test fileName        # Run tests on files matching fileName
pnpm format-and-lint:fix  # Format and lint
```

## Repository Structure

```
packages/       # Main npm packages (jazz-tools, cojson, etc.)
crates/         # Rust code (cojson-core NAPI, WASM, React Native)
examples/       # Example applications
starters/       # Project starter templates
tests/          # Integration and e2e tests
homepage/       # Documentation site (Next.js)
```

## Code Conventions (packages)

- **Import extensions**: Always use `.js` extensions in imports (enforced by Biome)
- **Naming**: camelCase for functions, PascalCase for types/interfaces
- **Unstable APIs**: Prefix with `unstable_` or `experimental_`
- **TypeScript**: Strict mode enabled, avoid `any` types in production code
- **JSDoc**: Document public APIs with `@param`, `@returns`, `@example` tags

## Testing

- **Framework**: Vitest (unit/integration), Playwright (e2e)
- **File naming**: `*.test.ts`, `*.test.tsx` for unit tests; `*.spec.ts` for e2e
- **Location**: `src/tests/**/*.test.ts`
- **First run**: Execute `pnpm exec playwright install` for browser tests

## Commit Messages

Format: `type(scope): description`

Types: `fix`, `feat`, `chore`, `refactor`, `test`, `docs`, `perf`

Examples:
- `fix(cojson): prevent message queuing after connection closure`
- `feat(jazz-tools): add useCoState hook`
- `test(browser-integration): add sync conflict tests`

## Before Submitting Changes

1. Run `pnpm format-and-lint:fix` to format code
2. Run `pnpm test --watch=false` to verify tests pass
3. Create a changeset using the related skill if the change affects package versions
4. Update JSDoc comments for any modified public APIs

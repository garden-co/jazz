# ~~jazz-ts CLI: monorepo-aware jazz binary fallback~~ — ✅

When `jazz-ts build` can't find `jazz` on PATH, it should try `target/debug/jazz` relative to the monorepo root before giving up. This avoids every example needing `--jazz-bin ../../target/debug/jazz` in its build script.

## Behaviour

1. Try `jazzBin` as given (default: `"jazz"`, i.e. PATH lookup)
2. On ENOENT, walk up from `process.cwd()` looking for a directory that contains both `Cargo.toml` and `target/debug/jazz`
3. If found, use it and print a note: `Using monorepo jazz binary at <path>`
4. If not found, print the current warning and continue (versioned schemas skipped)

## Scope

Single change in `packages/jazz-ts/src/cli.ts` (`runJazzBuild` function). No new deps.

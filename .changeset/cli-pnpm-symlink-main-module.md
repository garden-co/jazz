---
"jazz-tools": patch
---

Fix the `jazz-tools` CLI silently exiting 0 without running any command when `dist/cli.js` is invoked through a pnpm symlink. `isMainModule()` now compares the realpaths of both `process.argv[1]` and `import.meta.url`, so the symlinked package path resolves and the CLI dispatches as expected.

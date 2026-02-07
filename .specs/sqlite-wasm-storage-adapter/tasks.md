# Implementation Tasks

## Tasks

- [ ] 1. **Scaffold the `cojson-storage-sqlite-wasm` package** — Create `packages/cojson-storage-sqlite-wasm/` with `package.json` (dependencies: `@sqlite.org/sqlite-wasm`, `cojson workspace:*`; devDependencies: `typescript`, `@vitest/browser-playwright`), `tsconfig.json`, and `vitest.config.ts` configured for Vitest Browser Mode with Playwright + headless Chromium (ref: Design § Package Structure, § Vitest Browser Mode Configuration)

- [ ] 2. **Implement `SqliteWasmDriver`** — Create `src/SqliteWasmDriver.ts` implementing `SQLiteDatabaseDriverAsync` from `cojson`. Use `sqlite3Worker1Promiser` directly (no custom worker). Methods: `initialize()`, `run()`, `query()`, `get()`, `transaction()`, `closeDb()`, `getMigrationVersion()`, `saveMigrationVersion()`. Handle OPFS fallback to in-memory (ref: Design § SqliteWasmDriver.ts)

- [ ] 3. **Create package entry point** — Create `src/index.ts` exporting `getSqliteWasmStorage()` factory (calls `getSqliteStorageAsync(driver)` from `cojson`) and re-exporting `SqliteWasmDriver`. Add JSDoc with `@param`, `@returns`, usage example, and COOP/COEP requirements note (ref: Design § index.ts)

- [ ] 4. **Update `BaseBrowserContextOptions` storage type** — In `packages/jazz-tools/src/browser/createBrowserContext.ts`, extend `storage?: "indexedDB"` to `storage?: "indexedDB" | "sqlite-wasm"` (ref: Design § Framework Provider Updates)

- [ ] 5. **Add `"sqlite-wasm"` branch to `setupPeers()`** — In the same `createBrowserContext.ts`, add a conditional branch in `setupPeers()` that does `const { getSqliteWasmStorage } = await import("cojson-storage-sqlite-wasm")` and calls `getSqliteWasmStorage()` when `options.storage === "sqlite-wasm"`. Default remains IndexedDB (ref: Design § BaseBrowserContextOptions)

- [ ] 6. **Verify framework provider type propagation** — Confirm that `JazzBrowserContextManager` (`BrowserContextManager.ts`), `JazzReactProvider` (`provider.tsx`), `Provider.svelte`, and `JazzSvelteProviderWithClerk.svelte` all derive their `storage` type from `BaseBrowserContextOptions["storage"]` and require no code changes (ref: Design § Files that reference BaseBrowserContextOptions)

- [ ] 7. **Write driver unit tests** — Create `src/tests/storage.sqlite-wasm.test.ts` with tests for: DDL + query, `get()` single row, transaction commit, transaction rollback, migration version read/write. All tests use in-memory mode (`useOPFS: false`). Must run in Vitest Browser Mode (ref: Design § Driver Unit Tests)

- [ ] 8. **Port integration test utilities** — Create `src/tests/testUtils.ts` and `src/tests/messagesTestUtils.ts` adapted from `cojson-storage-indexeddb/src/tests/`, using `getSqliteWasmStorage("test.db", false)` instead of `getIndexedDBStorage()` (ref: Design § Integration Tests)

- [ ] 9. **Write storage integration tests** — Create integration tests mirroring `storage.indexeddb.test.ts`: store & load CoValue, dependency loading (group inheritance), transaction correction recovery, multi-session content, large data streaming, account persistence, sync state tracking, sync resumption (ref: Design § Integration Tests)

- [ ] 10. **Run lint, build, and test pipeline** — Execute `pnpm format-and-lint:fix`, `pnpm build:packages`, and `pnpm test --watch=false` to verify everything compiles and passes

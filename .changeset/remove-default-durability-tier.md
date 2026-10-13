---
"jazz-tools": patch
---

**Breaking:** Removed `defaultDurabilityTier`. Reads default to `"local-first"` in browsers and React Native, and without a configured server; other environments with a server default to `"remote"`. Set `tier` on individual reads to override this behavior.

Also removed, as part of the same cleanup:

- The `tier` option on backend `createJazzContext()`. A backend that set `tier: "local"` with a `serverUrl` used to read local-first; it now reads `"remote"` by default and waits on the server. Pass `tier: "local-first"` on reads that should not wait.
- The `clientId`, `runtimeSources` and `tier` fields of `AppContext`, so backend `createJazzContext()` no longer accepts `runtimeSources` either. The client `runtimeSources` option is unchanged.
- The `sqliteStorage` option on React Native `createDb()` (`ReactNativeDbConfig`). It was always rejected; storage is owned by the native runtime.
- The React Native exports `UnimplementedSqliteStorageDriver`, `ReactNativeSqliteConnection`, `ReactNativeSqliteStorageDriver`, `ReactNativeSqliteTransaction`, `REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR` and `REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR`.
- The `--jazz-bin` flag of `jazz-tools validate`. It had no effect.

TypeScript reports the removed options. Plain JavaScript ignores them silently, so remove them from your config.

# Firefox Private Browsing: OPFS Unavailable

## What

Firefox's private browsing mode blocks `navigator.storage.getDirectory()`, so Jazz 2 fails to initialise entirely — there is no fallback to ephemeral/in-memory storage.

Error chain:

1. `OpfsFile::open()` (`crates/opfs-btree/src/file.rs`) calls `storage.get_directory()` — throws in private mode
2. Error bubbles through `WasmRuntime::openPersistent()` → `handleInit()` in the worker
3. Worker posts `{ type: "error", message: "Init failed: ..." }` and the app never starts

There is no feature-detection for OPFS availability, no `openEphemeral()` path, and no graceful degradation.

## Priority

high

## Notes

- `localStorage` (used by `BrowserAuthSecretStore`) does work in private mode, so auth secret storage is fine — the block is purely the OPFS-backed data persistence layer
- An `openEphemeral()` / in-memory fallback already exists in Rust (`OpfsBTreeStorage::memory()`); it just isn't wired up on the browser path
- Same issue will affect Chrome/Edge incognito if they ever restrict OPFS; Safari already blocks it in some private-mode configurations
- Relevant files: `crates/opfs-btree/src/file.rs`, `crates/jazz-wasm/src/runtime.rs`, `packages/jazz-tools/src/worker/jazz-worker.ts`, `packages/jazz-tools/src/runtime/db.ts`

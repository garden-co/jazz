# OPFS BTree Storage Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a browser runtime API that exports worker-owned OPFS `opfs-btree` storage as a dependency-free bundle, plus a dev viewer that opens the bundle and lists raw B-tree entries.

**Architecture:** Extend `opfs-btree` with in-memory file import/export helpers and raw entry scanning. Add an OPFS snapshot hook to Jazz storage and expose it through the existing worker bridge protocol. Add a focused Vite/React dev app that parses the bundle and renders raw entries only.

**Tech Stack:** Rust 2024, wasm-bindgen, postcard worker protocol, TypeScript, Vite, React, Vitest.

---

## File Structure

- Modify `crates/opfs-btree/src/file.rs`: add `MemoryFile::from_bytes` and byte snapshot helpers.
- Modify `crates/opfs-btree/src/db.rs`: add `OpfsBTree::file_bytes`.
- Modify `crates/opfs-btree/src/lib.rs`: expose wasm helper functions for bundle viewer entry scans.
- Modify `crates/jazz-tools/src/storage/storage_trait.rs`: add default unsupported `export_debug_bundle_file`.
- Modify `crates/jazz-tools/src/storage/opfs_btree/mod.rs`: implement the OPFS file snapshot hook.
- Modify `crates/jazz-wasm/src/runtime.rs`: add `export_storage_bundle`.
- Modify `crates/jazz-wasm/src/worker_protocol.rs`: add storage export request/response protocol variants and JS test codecs.
- Modify `crates/jazz-wasm/src/worker_host.rs`: handle worker-side storage export requests.
- Modify `crates/jazz-wasm/src/worker_bridge.rs`: expose a promise-returning `exportStorageBundle`.
- Modify `packages/jazz-tools/src/types/jazz-wasm.d.ts`: add runtime/bridge type declarations.
- Modify `packages/jazz-tools/src/runtime/worker-bridge.ts`: wrap the bridge export method.
- Modify `packages/jazz-tools/src/runtime/db.ts`: add `exportStorageBundle` and `downloadStorageBundle`.
- Create `packages/jazz-tools/src/runtime/storage-bundle.ts`: shared TypeScript bundle encode/decode helpers for tests and download metadata.
- Create `packages/jazz-tools/src/runtime/storage-bundle.test.ts`: bundle parser tests.
- Create `dev/opfs-btree-viewer/*`: Vite viewer app.
- Modify `pnpm-workspace.yaml`: include the new dev app.

## Task 1: Add raw file snapshots and scans in `opfs-btree`

- [ ] Add tests in `crates/opfs-btree/src/db.rs` proving a tree can export file bytes and reopen from `MemoryFile::from_bytes`.
- [ ] Add `MemoryFile::from_bytes(bytes: Vec<u8>) -> Self`.
- [ ] Add `OpfsBTree::file_bytes(&self) -> Result<Vec<u8>, BTreeError>` that reads the full underlying file with `SyncFile::len` and `read_exact_at`.
- [ ] Add a wasm-only helper in `crates/opfs-btree/src/lib.rs` that accepts file bytes, opens a memory-backed `OpfsBTree`, scans `range(b"", &[0xff], usize::MAX)`, and returns JS entries with `key`, `keyBytes`, and `value`.
- [ ] Run `cargo test -p opfs-btree`.

## Task 2: Add a Jazz storage export hook

- [ ] Add `Storage::export_debug_bundle_file(&self, name: &str) -> Result<Option<Vec<u8>>, StorageError>` with a default `Ok(None)`.
- [ ] Forward the method in the `impl Storage for Box<T>`.
- [ ] Implement the hook for `OpfsBTreeStorage` by checkpointing and returning `tree.file_bytes()`.
- [ ] Add an `OpfsBTreeStorage` unit test that writes a raw entry, exports bytes, reopens them with `MemoryFile`, and finds the raw storage key.
- [ ] Run `cargo test -p jazz-tools storage::opfs_btree`.

## Task 3: Add runtime and worker bridge export API

- [ ] Add `WasmRuntime::export_storage_bundle(db_name: String) -> Result<Uint8Array, JsValue>` that calls the storage hook and builds the versioned bundle.
- [ ] Add `ExportStorageBundle { request_id }` and `ExportStorageBundleOk { request_id, bundle }` / `ExportStorageBundleFailed { request_id, message }` worker protocol variants.
- [ ] Handle the request in `worker_host.rs` by calling the runtime export method.
- [ ] Add promise resolver plumbing in `worker_bridge.rs` and expose `exportStorageBundle(): Promise<Uint8Array>`.
- [ ] Update `packages/jazz-tools/src/types/jazz-wasm.d.ts` and `packages/jazz-tools/src/runtime/worker-bridge.ts`.
- [ ] Run `cargo test -p jazz-wasm worker_protocol`.

## Task 4: Add TypeScript `Db` API

- [ ] Add a shared TypeScript bundle parser in `packages/jazz-tools/src/runtime/storage-bundle.ts` with magic/version validation.
- [ ] Add `WorkerBridge.exportStorageBundle`.
- [ ] Add `Db.exportStorageBundle(): Promise<Uint8Array>` that waits for bridge readiness and rejects without a worker bridge.
- [ ] Add `Db.downloadStorageBundle(options?: { filename?: string }): Promise<void>` using `Blob`, object URLs, and a temporary anchor.
- [ ] Add TypeScript tests for parser validation and runtime error behavior.
- [ ] Run `pnpm --filter jazz-tools test -- storage-bundle`.

## Task 5: Build the raw viewer app

- [ ] Add `dev/opfs-btree-viewer/package.json`, TypeScript config, Vite config, HTML, and source files.
- [ ] Implement bundle import, file selection, raw entry scanning through the `opfs-btree` WASM helper, and preview mode controls.
- [ ] Render a dense table with key text, key byte length, value byte length, and value preview.
- [ ] Add viewer tests for invalid bundles and rendered entry rows.
- [ ] Add the app to `pnpm-workspace.yaml`.
- [ ] Run `pnpm --filter opfs-btree-viewer test`.

## Task 6: Verify integration

- [ ] Run focused Rust tests: `cargo test -p opfs-btree` and `cargo test -p jazz-tools storage::opfs_btree`.
- [ ] Run focused worker protocol tests: `cargo test -p jazz-wasm worker_protocol`.
- [ ] Run focused TypeScript tests for `jazz-tools`.
- [ ] Run viewer tests.
- [ ] Start the viewer with `pnpm --filter opfs-btree-viewer dev` and inspect it in the browser.
- [ ] Run formatting for touched files.

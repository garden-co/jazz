# OPFS BTree Storage Viewer Design

## Goal

Let browser developers export the raw persisted `opfs-btree` files from a worker-backed Jazz runtime and inspect their raw B-tree entries in a small dev application.

## Assumptions

- The first supported source is browser persistent mode, where the dedicated worker owns OPFS.
- The current persistent OPFS backend stores one durable file per namespace, named `<dbName>.opfsbtree`.
- The viewer intentionally shows raw `opfs-btree` keys and values only. It does not decode Jazz row formats, table schemas, catalogue entries, histories, or indices.
- Bundling should not add a ZIP or archive dependency.

## Runtime API

Add a browser persistent-mode API on `Db`:

- `exportStorageBundle(): Promise<Uint8Array>`
- `downloadStorageBundle(options?: { filename?: string }): Promise<void>`

`exportStorageBundle` waits for the worker bridge, asks the worker runtime to checkpoint storage, and returns a versioned dependency-free bundle containing the active OPFS storage file. `downloadStorageBundle` builds on that API with `Blob` and `URL.createObjectURL` in browser environments.

The API rejects with a clear error when called without a worker-backed persistent runtime.

## Bundle Format

Use a small binary envelope:

- magic bytes: `JAZZOPFSBUNDLE1`
- format version: `1`
- creation metadata as UTF-8 JSON
- file count
- for each file: UTF-8 path, byte length, bytes

The format supports multiple files even though the current export writes one file. This keeps the exporter stable if `opfs-btree` later stores auxiliary files.

## Worker Flow

The main-thread `WorkerBridge` sends a new protocol request to the worker. The worker:

1. Confirms the runtime is ready.
2. Flushes/checkpoints storage.
3. Reads the OPFS-backed file bytes from the active storage backend.
4. Wraps the file in the bundle format.
5. Transfers the bundle bytes back to the main thread.

The existing binary postcard worker protocol remains the transport. The response carries a request id so concurrent calls can be resolved independently.

## Viewer

Add `dev/opfs-btree-viewer`, a Vite/React app included in `pnpm-workspace.yaml`.

The first screen is the usable tool:

- drag/drop or file picker for a `.jazz-opfs-bundle`
- bundle metadata summary
- file list
- raw entry table for the selected file

The table shows:

- key text
- key byte length
- value byte length
- value preview as UTF-8, hex, or base64
- copy controls for key and value previews

The viewer parses the bundle in TypeScript, then uses a WASM helper from `opfs-btree` to open the selected file bytes with an in-memory file and scan raw entries. It does not need application schemas or a Jazz server connection.

## Testing

- Rust tests cover `opfs-btree` file byte snapshots and raw entry scanning from in-memory bytes.
- Rust/wasm protocol tests cover the new worker request/response variants.
- TypeScript tests cover bundle parsing, bridge error handling, and the `Db` download helper.
- Viewer tests cover import failure states and raw entry rendering.

## Out Of Scope

- Jazz-aware decoding of row histories, visible rows, catalogue rows, indices, permissions, or schemas.
- Export from native SQLite/RocksDB backends.
- ZIP-compatible archive output.

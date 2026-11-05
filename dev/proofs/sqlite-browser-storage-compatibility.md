# SQLite browser storage admission

The SDK SharedWorker opens the SQLite file selected by its physical browser
root. This port uses upstream's filename and IndexedDB layout, while retaining
E2EE's owner admission, durable flush and missing-file checks.

## Encoding and identity

The sidecar uses IndexedDB database version **3**, with `pages`, `metadata` and
`storage-manifest` object stores. The epoch manifest retains upstream's exact
fields and `storageEpoch: 1`. Its required codec inventory is
`["jazz.sqlite-storage.v2"]`, matching the Rust SQLite storage format.

The separate `storage-manifest` entry `sqlite-file-owner-v2` contains this
canonical UTF-8 JSON tuple:

```
["jazz.browser.sqlite-root.v2", physicalName, exactOwner, replicaHex, state]
```

`replicaHex` is the lowercase hexadecimal encoding of the 16-byte replica node.
`state` is `opening`, `ready` or `resetting`. Readers check the physical name,
owner, replica, tuple length and canonical bytes. Independently assembled fixture
`packages/jazz-tools/fixtures/browser-sqlite-root-v2.json` pins all three states
and filename examples. Historical v1 fixtures remain unchanged. Owner,
worker-epoch and foreground lease records retain their existing encodings;
SQLite lease updates request strict IndexedDB durability.

The descriptor is `{backend: "indexeddb", path, requireExisting}`. Its path is
`/jazz-<digest>.sqlite`, where `digest` is the first 16 SHA-256 bytes of the UTF-8
physical database name, rendered as lowercase hexadecimal. This matches
upstream's default `StoreSpec` mapping. `requireExisting` is true for a ready
root. Distinct physical roots for Alice receive different files. Bob cannot open
Alice's root: owner admission precedes WASM configuration. Copying the root guard
to another physical name is rejected.

## Publication, retirement and reset

The physical Web Lock covers admission, file opening, flush, retirement and
reset. The sidecar reserves one runtime before awaiting its open callback. It
publishes `ready` in a strict transaction only after the actual SQLite flush
completes. A ready root whose file is missing fails without creating a
replacement or rewriting the owner record.

Every open reloads that file's persisted IndexedDB blocks under the physical
lock, after the previous runtime has closed and flushed. A cached VFS image
cannot conceal external file deletion or corruption. Other files are unaffected.

Retirement waits for an in-flight open and closes the runtime before releasing
its reservation or physical lock. A failed close keeps both for retry; a
successor cannot acquire the file until that retry succeeds. This also applies
to failed initialization, schema replacement and reset. WASM owner close marks
shared views closed and installs a deny-all SQLite connection authorizer before
flushing. Retained views, streams, transports and staged transactions cannot use
the retired connection. Closing a schema view does not retire its owner.

Reset closes the runtime, persists `resetting` strictly, deletes the bound SQLite
file and then deletes the sidecar. Interrupted reset completes deletion and
rotates the replica node and lease pool before opening again. The filename stays
the same: it is bound to the physical name, not the replica node. Deleting only
the sidecar does not constitute a reset; callers must use the normal reset path
to delete the file as well.

## Compatibility and validation boundary

This is an unreleased format change with no migration. Historical SQLite v1 and
Groove codec inventories are refused before owner or lease writes and before a
SQLite open. The former E2EE version-4 sidecar is also incompatible: IndexedDB
refuses opening it at version 3. Old JSM/JSA admission receipts and Groove
preflight are not part of the SQLite open path. Historical fixtures remain
unchanged and explicit reset is required to replace incompatible storage.

`sqlite-v2-browser-jazz-corpus.json` is output from the public Chromium producer:
account creation, catalogue deployment, real SharedWorker writes, flush and
reopen. Its compatibility test checks rows, branches, large values, subsequent
writes and another offline reopen. Read-only reopening preserves every stored
byte except the foreground lease lifecycle. Corrupt files and historical
formats are rejected without modifying their records or SQLite blocks.

The loader validates generated WASM fingerprints and producer manifests. Missing,
mismatched or incomplete artifacts still fail. Validation uses a normal
`node dev/artifacts/build.mjs wasm fast` build. The storage tests qualify this
bridge; they do not establish complete browser E2EE readiness or qualify the
NAPI and React Native hosts.

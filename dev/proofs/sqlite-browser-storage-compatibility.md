# SQLite browser storage admission

The SDK SharedWorker now opens the SQLite file selected by its physical browser
root. Previously it passed the retired Groove page adapter to a WASM API that
expected a SQLite descriptor. This ignored `dbName`, could alias punctuation in
owner names, and left the SQLite file behind when resetting the old sidecar.

## Encoding and identity

A SQLite sidecar uses IndexedDB database version **4** and contains only the
`storage-manifest` object store. Its manifest is the canonical UTF-8 JSON tuple:

```
["jazz.browser.sqlite-root.v1", physicalName, exactOwner, replicaHex, state]
```

`replicaHex` is the lowercase hexadecimal encoding of the existing 16-byte
replica node. `state` is `opening`, `ready` or `resetting`. Readers validate the
exact physical name, owner, replica, tuple length and canonical bytes. The
fixture `packages/jazz-tools/fixtures/browser-sqlite-root-v1.json` pins all three
states. Owner, node, worker-epoch and foreground lease records retain their
existing encodings; SQLite lease updates request strict IndexedDB durability.

The descriptor is `{backend: "indexeddb", path:
"/jazz-sqlite-v1-<replicaHex>.sqlite", requireExisting: state === "ready"}`.
Two independently created roots for Alice receive different nodes and files.
Bob cannot open Alice's root: owner admission precedes WASM configuration.
Copying the manifest to a different physical name is rejected.

Low-level WASM calls without a path use the separately named mapping
`jazz.browser.default-path.v1`: SHA-256 of its UTF-8 name followed by a zero byte
and the complete UTF-8 owner, rendered as
`/jazz-owner-v1-<lowercase digest>.sqlite`. The same fixture pins punctuation-
distinct owners, and a browser test reopens the generated file explicitly.
The SDK supplies a replica-specific path instead of using this default.

## Publication, retirement and reset

The physical Web Lock covers admission, file opening, flush, retirement and
reset. A sidecar reserves one runtime before awaiting its open callback. It
publishes `ready` in a strict transaction only after the actual SQLite flush
completes. A ready root whose file is missing fails before creating a replacement;
it must be reset so its old transaction identity cannot be recycled.

Retirement waits for an in-flight open, closes the runtime and drains pending
work before releasing its reservation or physical lock. WASM owner close marks
shared views closed, installs a deny-all SQLite connection authorizer, then
flushes the file. Installing the authorizer invalidates cached prepared
statements. Retained views, streams, transports and staged transactions cannot
use that connection after retirement. Closing a schema view does not retire its
owner. This fence is runtime state, with no persisted policy or wire changes.

Reset closes the runtime, commits `resetting` strictly, deletes the bound SQLite
file and then deletes the sidecar. If either deletion is interrupted, reopening
finishes file deletion and atomically creates a fresh node, empty lease pool and
`opening` manifest. Tests cover failure both before and after file deletion.
An external deletion of the sidecar can leave an orphaned VFS file; a new root
uses a new random node and cannot accidentally reopen that orphan.

## Compatibility and validation boundary

This is an unreleased storage format change. Existing version-3 Groove roots
are rejected without upgrading or rewriting their manifest. There is no Groove
migration and no migration from the former punctuation-sanitised default SQLite
filenames. Explicit reset is required. The SQLite row/catalogue/wire encodings
are unchanged by this port.

The browser loader verifies the real generated WASM artifact without importing
the unavailable NAPI fingerprint. Verification rules remain unchanged: missing,
mismatched and incomplete artifacts fail. The dedicated browser storage suite
also checks the producer manifest before starting; it needs a current normal
`node dev/artifacts/build.mjs wasm fast` build.

Tests exercise the real SQLite WASM and IndexedDB lifecycle, the production
SharedWorker's admission/reopen/reset messages, ownership rejection, missing-file
failure, retained-handle retirement and canonical manifest bytes. Existing
worker initialization tests retain their assertions. This qualifies the storage
bridge; it does not establish foreground write relay durability, encrypted
subscriptions, the NAPI binding or a complete browser E2EE application.

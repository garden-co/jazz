---
"jazz-tools": major
"jazz-wasm": major
"jazz-napi": major
"jazz-rn": major
---

**Breaking: alpha.60 changes the wire protocol and the storage format together.** alpha.59 and alpha.60 peers can't sync with each other, and alpha.60 can't open any store written by alpha.59 or earlier. There is no migration: upgrade clients, native builds, relays and Core servers together, and start from empty storage. Read [Upgrading to alpha.60](https://jazz.tools/docs/reference/upgrading-to-alpha-60) before upgrading a deployment that holds data you need.

### Wire protocol v7

Every endpoint now speaks wire protocol v7, and only v7. A peer on any other version is refused at the `Hello` handshake with `UnsupportedProtocolVersion` (retry `Never`), before any message is decoded, in both directions. An alpha.59 client (protocol v3) talking to an alpha.60 server sees `unsupported wire protocol advertisement: remote 3..=3, expected 7..=7`.

This also covers the unreleased protocol versions that builds of `main` spoke between alpha.59 and alpha.60. Their bytes changed several times under one version number. In particular, #4001 removed obsolete wire messages and renumbered the remaining message tags within protocol v5, so `main` builds from before and after #4001 can't talk to each other although both claim v5. v7 refuses all of them at `Hello`.

What changes on the wire:

- **Linear row history.** Rows no longer carry `parents`. Each write names the base image it was made over, and Core orders accepted writes by its own sequence number instead of writer clocks.
- **Smaller row payloads.** A sync message defines each row descriptor once and later rows reference it (#3380). A 1,000-row, 10-column view update goes from 2,398,861 to 457,804 bytes.
- **Reads see your earlier writes, including after a reconnect** (#3863). A server answers a read only after it has answered every write the same connection sent before it. This replaces the earlier limitation that a one-shot `"remote"` read could miss writes made before a reconnect.
- **Writes apply in the order they were written** (#4072). Core no longer answers "retry later" when a write reaches it before its predecessor, and a write no longer fails as "predecessor lost". The transport keeps each writer's writes in order, so Core applies every write as written over the row's current state.
- **Forward compatibility** (#2468). Tagged unions stay strictly decoded. Later additive changes ride negotiated capability bits, and only the `Hello` version range and the `Error` frame are shared across versions, so a newer peer now gets a version mismatch instead of a malformed-frame error.
- **Exclusive aggregates.** An exclusive transaction's count or other aggregate read travels as its result (see below).

React Native: the native relay ABI moves from 2 to 3. A JavaScript-only (OTA) update to this release needs a new native build; a mismatched build fails at startup with "new native development/release build required".

### Storage format

Every store written by alpha.59 or earlier is refused when it is opened, before any record is read or changed: native RocksDB and SQLite data directories, browser IndexedDB databases, React Native stores, relays, Core, and the server's catalogue and account registry. The error is the typed `UnsupportedStorageCodecs` (`code: "unsupported_storage_codecs"`), whose `missing` and `unknown` lists name the storage formats the store lacks and the ones this build doesn't read. A missing or damaged manifest still fails with the generic storage error, so an old store can be told apart from a damaged one.

There is no converter. Delete the old store and let the client resync. Writes that never reached Core are lost. Core and relays start from empty data directories, so data that exists only in alpha.59 stores is not carried over.

What changes on disk:

- Row history is linear: one row-state image per accepted write, with no version graph and no merge versions. Row history also drops its `by_tx` index; each transaction keeps a short list of the rows it touched instead.
- Each store declares one Jazz format marker (`jazz.node-storage`, `jazz.catalogue-storage` or `jazz.account-storage`) instead of a list of per-encoding markers, and obsolete stores and fallbacks are gone.
- The transaction record holds only what status reads need. Provenance and pending-only read evidence move to their own tables, and authors are stored as 4-byte codes in a Groove dictionary. A row inserted and then updated takes 1,259 bytes on Core instead of 1,546, and 771 bytes on a reader instead of 985.
- Large values are more compact: no repeated chunk magic, no text-only coordinates on binary values, and no stored UTF-16 lengths that can be derived.

> **PLACEHOLDER (alpha.60 bump pass): #4088, #4103, #4109 and the storage-tightening PRs.** Add their on-disk changes and the final per-row sizes here once they merge: 4-byte node aliases and 6-byte timestamps (#4088), history that keeps only superseded images (#4103), the in-memory pending overlay (#4109), and the table-id keys, PK-copy, null-base and touched-row items.

### Behaviour changes

- **Exclusive aggregates record their result.** Exclusive transactions now record what a count or other aggregate returned instead of every row it counted. A commit that counts many rows stays small, and it is no longer rejected when another writer changes a counted row without changing the count, sum, minimum or maximum the transaction saw. It is still rejected with `exclusive_conflict` when that result changes. Reading an aggregate after writing in the same exclusive transaction now fails with an "is not supported in exclusive transactions yet" error; read the aggregate before the first write.
- **Tuple values decode exactly as they encode.** A present nullable integer inside a tuple no longer reads back byte-swapped, and `F64` tuple members encode, decode and sort numerically instead of failing. Stored tuple bytes are unchanged.
- **Subscriptions without a server open at once.** With no server configured, or when the server link has failed, a `"local-first"` subscription delivers its first result immediately, even when nothing is stored locally: an empty result that is still pending. Before, it published its first result only once local data changed.

> **PLACEHOLDER (alpha.60 bump pass): #4023.** Add the user-facing text for per-column merges across schema versions (#3899: an older-schema client's edit no longer reverts columns it doesn't know, merge-column writes across schema versions are accepted, and a `CopyColumn` target keeps its own value) and for the typed old-store refusal on every platform (#3728: NAPI, WASM, React Native and the Rust tools client, plus `isUnsupportedStorageCodecsError` from `jazz-tools` and `jazz-tools/react-native`).

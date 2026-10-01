# Linear history storage and wire compatibility (#3281)

> **Upgrading requires wiping every Core root: a full server data reset.**
> The new build refuses an alpha.59 (or earlier) Core, relay or client store
> and there is no converter, so Core and relays must start from an empty
> root and every client resyncs from it. Data that exists only in the old
> stores is not carried over. See [What users must do](#what-users-must-do).

Comparison: main `eb772f48d` (alpha.59 formats) against the #3281 branch,
checked at `8635b5ad8`. These revisions are **not storage compatible and not
wire compatible**. There is **no migration, dual read or downgrade path**.
Old stores must be deleted by the user or app; the new build refuses them
before it decodes or mutates any record, on native roots and in the browser
alike, with a typed error that names the missing codec families.

#3673 (author aliases) is stacked on this change and adds one more node codec
family, `jazz.author-alias.v1`. Where it matters, this document says what
changes with #3673.

Run the refusal proofs with:

```sh
dev/gates/storage-compat.sh
RUST_MIN_STACK=4194304 cargo test -p jazz --test integration storage_format_refusal
RUST_MIN_STACK=4194304 cargo test -p jazz --test integration wire_fixtures::pre_v4_peers_are_refused_at_hello_with_a_typed_version_mismatch
```

The browser receipts run in the TypeScript partition's "browser storage
compatibility corpus" command (`dev/gates/local-ci-equivalent.mjs`).

Citations are `path` plus symbol, because line numbers shift with every main
merge.

## What changed

- **Wire:** protocol v3 becomes v4 (`crates/jazz/layers/protocol/src/wire.rs`,
  `WIRE_PROTOCOL_VERSION`; TypeScript
  `packages/jazz-tools/src/runtime/native-runtime/websocket.ts`,
  `WIRE_PROTOCOL_VERSION`). Row payloads use `JVRR\x02` with no `parents`, a
  `_deletion` cell and trailing `col_stamps`. `SyncMessage` tags 15/16 and
  `KnownStateDeclaration` tag 2 are reserved and uninhabited. SPEC 8 records
  the boundary ("Linear-history boundary").
- **Storage:** every root that stores rows opens with
  `node_storage_codec_profile()`
  (`crates/jazz/layers/protocol/src/storage_codec_profile.rs`), which is the
  epoch-1 base plus `JAZZ_NODE_STORAGE_CODECS`:
  `groove.durable-index.v2` and `jazz.history-version-current.v4` (#3673 adds
  `jazz.author-alias.v1`). The storage epoch stays 1. Roots with no row
  history (server account registry, catalogue-entry store) keep
  `epoch_1_storage_codec_profile()` and are unaffected.

## What old peers see

| Pair                                                 | Where it fails                                                                                                                                                                               | Exact error                                                                                                                                                                                                                                                                           |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| alpha.59 (v3) client → new (v4) server               | Server, on the client's Hello (`crates/jazz-server/src/server/routes/websocket.rs`, `negotiate_wire` call in the websocket accept path). The server sends one `WireFrame::Error` and closes. | `WireErrorCode::UnsupportedProtocolVersion`, `WireRetry::Never`, message `unsupported wire protocol advertisement: remote 3..=3, expected 4..=4` (`wire.rs`, `negotiate_wire`). An old TS client names the code `unsupported_protocol_version` (`websocket.ts`, `wireErrorCodeName`). |
| new (v4) client → alpha.59 (v3) server               | Old server, same path on main.                                                                                                                                                               | Same code and retry, message `unsupported wire protocol advertisement: remote 4..=4, expected 3..=3` (main `wire.rs`, `negotiate_wire`).                                                                                                                                              |
| new TS client reading a server Hello that is not v4  | Client (`websocket.ts`, `decodeServerHello`).                                                                                                                                                | `server must advertise exactly wire protocol 4, got 3..=3`                                                                                                                                                                                                                            |
| new NAPI binding told a different negotiated version | `crates/jazz-napi/src/lib.rs`, transport constructor.                                                                                                                                        | `server negotiated wire protocol 3, but this native binding supports only 4`                                                                                                                                                                                                          |
| v3 message envelope on a v4 link                     | `wire.rs`, `validate_metadata`.                                                                                                                                                              | `UnsupportedProtocolVersion`, `wire message protocol version 3 does not match negotiated 4`                                                                                                                                                                                           |
| v3 `KnownStateDeclaration::ExactVersionSet` (tag 2)  | Payload decode (`protocol.rs`, `KnownStateDeclaration::Reserved2`).                                                                                                                          | Decode failure; tag 2 is uninhabited.                                                                                                                                                                                                                                                 |

Nothing reaches payload decoding across versions, so no old frame can be
reinterpreted. Receipt: `crates/jazz/tests/wire_fixtures.rs`,
`pre_v4_peers_are_refused_at_hello_with_a_typed_version_mismatch` replays frozen
v3 Hello and Subscribe frames and a tag-2 declaration.

## What old stores see

### Native roots (Core, relay, Node/NAPI, native tools client, React Native through `jazz-native-relay`)

Manifest admission compares the declared codec families before any ordinary
key is read (`crates/groove/src/storage/manifest.rs`,
`StorageEpochManifest::admit_existing`) and returns
`groove::storage::Error::UnsupportedStorageCodecs { epoch, missing, unknown }`
(`crates/groove/src/storage/mod.rs`). Its message is:

```text
unsupported storage format: this epoch-1 root lacks codec families [..missing..]
required by this build and declares [..unknown..] that this build does not read
```

Adapter-level receipts are in `crates/jazz/tests/storage_format_refusal.rs`
(public `open_*` entry points with the node profile). Node-level receipts are
in `crates/jazz/layers/node/src/node/tests/native_storage_corpus.rs` (the
`node::tests::harness::` tests below).

| Root written by                                                                                    | `missing`                                                    | `unknown`                                                | Receipt                                                                                                                                                                                                   |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| alpha.54 RocksDB (published)                                                                       | `groove.durable-index.v2`, `jazz.history-version-current.v4` | none                                                     | `published_alpha54_rocksdb_root_is_refused_with_a_typed_format_error` (refusal is stable on retry); `published_alpha54_native_corpus_is_refused_with_the_typed_codec_error` (no record or family changes) |
| alpha.56 RocksDB with an unsynced edge-accepted write (previously opened and failed on first read) | same                                                         | none                                                     | `published_alpha56_rocksdb_root_is_refused_with_a_typed_format_error`; `published_alpha56_legacy_edge_receipt_is_refused_without_rewriting_its_records`                                                   |
| pre-linear current SQLite and RocksDB corpora (`crates/jazz/fixtures/pre-linear-native-jazz*`)     | same                                                         | none                                                     | `pre_linear_native_corpora_are_refused_before_any_mutation` (SQLite file byte-identical afterwards)                                                                                                       |
| older epoch-1 settlement SQLite corpus                                                             | same                                                         | `jazz.result-member-key.v1`, `jazz.result-row-source.v1` | same test; `retired_result_codec_profiles_reject_historical_native_roots`                                                                                                                                 |
| linear history before the compact index (alpha.59 index layout)                                    | `groove.durable-index.v2`                                    | none                                                     | `linear_history_root_without_the_durable_index_family_is_refused`                                                                                                                                         |
| unreleased history v2 / v3 roots                                                                   | the current families                                         | `jazz.history-version-current.v2` or `.v3`               | `storage_codec_profile.rs`, `node_profile_refuses_history_v2_and_v3_roots`                                                                                                                                |

With #3673, `jazz.author-alias.v1` joins every `missing` list above, and a
linear-history root written before author aliases is refused with only that
family missing (#3673's `pre_alias_linear_history_root_is_refused_with_a_typed_format_error`).
The manifest bytes of the node profile are pinned in
`storage_codec_profile.rs`
(`node_profile_has_a_pinned_manifest_receipt_and_refuses_base_only_roots`).

The current native corpus (`crates/jazz/fixtures/current-native-jazz-*`) is
re-pinned in the v4 layout and still has to reopen and accept writes
(`committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes`).
The DAG-layout images it replaced moved to `pre-linear-native-jazz-*` and
stay as refusal evidence.

### Browser (IndexedDB)

The browser root carries its own manifest with the same family list
(`packages/jazz-tools/src/runtime/indexeddb-page-store.ts`,
`JAZZ_EPOCH_1_STORAGE_CODEC_IDS`). `IndexedDbPageStore.open` checks it
(`assertStorageManifest`). A well-formed manifest whose codec inventory
differs, which is what every alpha.54 to alpha.59 database has, throws the
typed `UnsupportedStorageCodecsError` (`name`
`"UnsupportedStorageCodecsError"`, `code` `"unsupported_storage_codecs"`,
`missing`, `unknown`). The `code` survives the browser worker relay; the class
does not. The message keeps the older generic prefix, so existing matches on
it still hold:

```text
Missing or invalid IndexedDB storage epoch manifest: unsupported storage format:
this epoch-1 root lacks codec families ["groove.durable-index.v2","jazz.history-version-current.v4"]
required by this build and declares [] that this build does not read
```

A missing, malformed or inconsistent manifest (a damaged store, not an old
one) still throws the generic `Missing or invalid IndexedDB storage epoch
manifest`, so an app can tell "old format" apart from "damaged store".

Receipts: `indexeddb-page-store.test.ts`, "reports a codec-family mismatch as
a typed storage-format refusal". In
`packages/jazz-tools/tests/browser/indexeddb-jazz-compat.test.ts`, the
published alpha.54 browser corpus and the pre-linear browser corpus
(`packages/jazz-tools/fixtures/pre-linear-browser-jazz-corpus.json`, real
producer output from before this change) are both opened through the public
WasmDb path. Both opens are refused with the typed error above, and no raw
record changes. The same file pins a corpus in the new layout
(`packages/jazz-tools/fixtures/current-browser-jazz-corpus.json`, producer
output from this build). It opens through public WasmDb, reads back its
branches and large values offline, leaves every raw record unchanged across
read-only opens, keeps the foreground-node lease lifecycle intact, and accepts
an append from the current writer.

## What users must do

- **Upgrade clients and servers together.** A v3 and a v4 peer never sync.
- **Delete old stores.** Native data directories (RocksDB, SQLite) and browser
  IndexedDB databases written by alpha.59 or earlier must be removed, then the
  client resyncs from Core. Any write that had not reached Core is lost.
- **Core and relays** start from an empty root. There is no converter from DAG
  history to row-state history.

## Storage-compatibility checks

The refusals above are the expected result, and CI asserts them:
`test-storage-compat` runs `dev/gates/storage-compat.sh` (the native corpus
receipts and the `storage_format_refusal` receipts), and the TypeScript
partition runs the browser corpus receipts. The published alpha.54 and
alpha.56 archives are append-only evidence and stay as refusal receipts. New
current corpora must be written in the v4 layout.

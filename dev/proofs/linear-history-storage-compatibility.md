# Linear history storage and wire compatibility (#3281)

> **Upgrading requires wiping every Core root: a full server data reset.**
> The new build refuses an alpha.59 (or earlier) Core, relay or client store,
> and a store written by main after #3659 (read tiers, durability encoding v2),
> and there is no converter, so Core and relays must start from an empty
> root and every client resyncs from it. Data that exists only in the old
> stores is not carried over. See [What users must do](#what-users-must-do).

Comparison: main `eb772f48d` (alpha.59 formats) against the #3281 branch,
which has since merged main at `63adeca0c` (#3659: read tiers, wire v5 and
durability encoding v2).
This document names no verified branch revision: the refusal proofs below
are its receipts, and they check whichever revision they run on. The two
lines are **not storage compatible and not wire compatible**. There is **no migration, dual read or downgrade path**.
Old stores must be deleted by the user or app; the new build refuses them
before it decodes or mutates any record, on native roots and in the browser
alike, with a typed error that names the missing codec families.

Codec ids in this document are those of the current profile on main
(`crates/jazz/layers/protocol/src/storage_codec_profile.rs`). Since this
change, #4071 replaced the per-encoding Jazz ids it introduced with one Jazz
marker per root, and #4021 added `groove.dictionary.v1` for interned author
columns (the direction #3673, closed unmerged, had proposed as
`jazz.author-alias.v1`). The consolidated alpha.60 picture is
`dev/proofs/alpha60-compatibility.md`.

Run the refusal proofs with:

```sh
dev/gates/storage-compat.sh
RUST_MIN_STACK=4194304 cargo test -p jazz --test integration storage_format_refusal
RUST_MIN_STACK=4194304 cargo test -p jazz --test integration wire_fixtures::pre_v6_peers_are_refused_at_hello_with_a_typed_version_mismatch
```

The browser receipts run in the TypeScript partition's "browser storage
compatibility corpus" command (`dev/gates/local-ci-equivalent.mjs`).

Citations are `path` plus symbol, because line numbers shift with every main
merge.

## What changed

- **Wire:** protocol v3 becomes v6 (`crates/jazz/layers/protocol/src/wire.rs`,
  `WIRE_PROTOCOL_VERSION`; TypeScript
  `packages/jazz-tools/src/runtime/native-runtime/websocket.ts`,
  `WIRE_PROTOCOL_VERSION`). Row payloads used `JVRR\x02` (`JVRR\x03` since #4019) with no `parents`, a
  `_deletion` cell and trailing `base` and `counter_signs`. `SyncMessage` tags 14/15 (the
  compact numbering after #4001; 15/16 before it) and
  `KnownStateDeclaration` tag 2 are reserved and uninhabited. SPEC 8 records
  the boundary ("Linear-history boundary"). Wire v4 (multiple schema
  predecessors, #3651) and v5 (#3659: compact durability tags, no Edge role,
  `Relay=2`, no registration propagation flag) never shipped in a release;
  v6 carries both unchanged and refuses v5 and v4 peers the same way it
  refuses v3 ones. This branch used v5 for linear history before #3659 took
  that number; it was renumbered to v6 when main was merged, and its
  `SyncMessage` tags (then 34 `ViewUpdatePart` and 35 `RetryLater`) did not
  collide with #3659, which added no `SyncMessage` variant. `RetryLater` has
  since been retired (#4072) without ever shipping in a release: Core no
  longer answers "retry later" and no peer sends or decodes it.
- **Storage:** every root that stores rows opens with
  `node_storage_codec_profile()`
  (`crates/jazz/layers/protocol/src/storage_codec_profile.rs`): Groove's
  epoch-1 base (`groove.large-value.v2`, `groove.ordered-chunk-storage.v2`,
  `groove.ordered-kv.v1`) plus `JAZZ_NODE_STORAGE_CODECS`
  (`groove.dictionary.v1`, `groove.durable-index.v2`,
  `jazz.node-storage.v1`), six families in all. When this change landed the
  row history had its own id, `jazz.history-version-current.v4`, next to the
  per-encoding base ids (`jazz.transaction-durability.v2` and others); #4071
  retired all of them into `jazz.node-storage.v1`. The storage epoch stays 1.
  The server catalogue and account registry have their own profiles
  (`jazz.catalogue-storage.v1`, `jazz.account-storage.v1` on the same Groove
  base), so they hold no row history but are still refused when written
  before #4071.
- **Merge fields (SPEC 4 §4.6):** within the same unreleased
  node format (then `jazz.history-version-current.v4`, now part of
  `jazz.node-storage.v1`), history records are keyed
  `(branch_key, row_uuid, seq, tx_time, tx_node_id)` (`seq = 0` while a write
  is pending, moved to its seq when its accepted fate is stored) and end with
  `seq`, `base_seq` and `base_pending`; the hidden column stamps
  of earlier v4 builds are gone from history and current records. Only
  unreleased builds of this branch ever wrote stamps, so no codec id changes
  and no released root is affected. The native and browser corpora below were
  regenerated in this layout.
- **Union row cases (SPEC 10 §10.3, #4023):** a history row may be stored
  in the union case of its schema version: the schema's own cells, then the
  lineage's other columns known when that schema was published, as trailing
  nullable fields. A union case's Groove variant tag is the schema alias with
  bit 31 set (`physical/catalogue.rs`, `UNION_VARIANT_TAG_BIT`), so
  `groove_variant_tag` now refuses an alias with bit 31 set: a node can
  register at most 2^31 schema aliases. Which extras are stored (a column the
  layout dropped, a sibling's column, a `CopyColumn` target) rather than
  derived from the layout's cells is a function of the catalogue prefix up to
  the layout's own rank (`union_stored_extras`), so a later schema cannot
  change how existing union rows read. Because these rows change the node
  layout, the node marker moves from `jazz.node-storage.v1` to
  `jazz.node-storage.v2` (`storage_codec_profile.rs`,
  `JAZZ_NODE_STORAGE_CODECS`; TypeScript `JAZZ_EPOCH_1_STORAGE_CODEC_IDS`).
  A build with union cases refuses a root written by main before them
  (`missing` `jazz.node-storage.v2`, `unknown` `jazz.node-storage.v1`), and a
  pre-union main build refuses a union-case root the same way, at open
  rather than at the first union row it reads. Receipt:
  `storage_codec_profile.rs`, `node_storage_v1_and_v2_roots_refuse_each_other`.
  `jazz.node-storage.v1` never shipped in a release (it was introduced by
  #4071 during alpha.60 development), and alpha.59 and older roots are
  already refused, so no released root is affected. The current native and
  browser corpora are producer output from this build (after #4021's
  `groove.dictionary.v1`): the native SQLite and RocksDB images and the
  browser IndexedDB corpus were regenerated by their producers, and the
  native logical pack is byte-identical. On the wire, union
  cells travel in `JVRR\x03` entries whose head is `reference << 1 | union`,
  so every multi-row message spells table reference `k` as `2k`: a main build
  between #4019 and #4023 and a build with #4023 misread each other's
  multi-row payloads although both say `JVRR\x03`. This ships only together
  with the alpha.60 wire version bump (track F).
- **Node-local direct store (alpha.60 epoch):** #4072 adds
  `jazz_superseded_writes_v1` (`crates/jazz/layers/model/src/schema.rs`,
  `SUPERSEDED_WRITES_STORE`), Core's superseded-writes markers (SPEC 4 §4.6,
  `INV-HIST-23`). It is a new direct record store in the storage epoch the
  alpha.60 wipe starts, so no root written before it holds one, and it adds
  no codec family: an older linear-history root of this epoch opens with the
  store empty. It is node-local and never on the wire; only the fate
  authority writes it, and an empty store on clients and relays is harmless.
  Key: physical table id (U64), branch key (Bytes), row (Uuid), writer node
  (Uuid); value: `superseded_through` (U64 transaction time). The physical
  table id is kept across table renames. The settlement corpus leaves the
  store empty, so its native bytes (class-CF meta framing, key encoding and
  value record) are pinned by
  `native_storage_corpus.rs`, `superseded_writes_store_native_bytes_are_pinned`.
  Markers are not part of history: a Core rebuilt from history alone
  (`INV-HIST-17`) starts with none.

## What old peers see

| Pair                                                 | Where it fails                                                                                                                                                                               | Exact error                                                                                                                                                                                                                                                                           |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| alpha.59 (v3) client → new (v6) server               | Server, on the client's Hello (`crates/jazz-server/src/server/routes/websocket.rs`, `negotiate_wire` call in the websocket accept path). The server sends one `WireFrame::Error` and closes. | `WireErrorCode::UnsupportedProtocolVersion`, `WireRetry::Never`, message `unsupported wire protocol advertisement: remote 3..=3, expected 6..=6` (`wire.rs`, `negotiate_wire`). An old TS client names the code `unsupported_protocol_version` (`websocket.ts`, `wireErrorCodeName`). |
| new (v6) client → alpha.59 (v3) server               | Old server, same path on main.                                                                                                                                                               | Same code and retry, message `unsupported wire protocol advertisement: remote 6..=6, expected 3..=3` (main `wire.rs`, `negotiate_wire`).                                                                                                                                              |
| new TS client reading a server Hello that is not v6  | Client (`websocket.ts`, `decodeServerHello`).                                                                                                                                                | `server must advertise exactly wire protocol 6, got 3..=3`                                                                                                                                                                                                                            |
| new NAPI binding told a different negotiated version | `crates/jazz-napi/src/lib.rs`, transport constructor.                                                                                                                                        | `server negotiated wire protocol 3, but this native binding supports only 6`                                                                                                                                                                                                          |
| v3 whole-message frame on a v6 link                  | `wire.rs`, `decode_frame` / channel admission (v6 has no `Message` frame).                                                                                                                   | Rejected before payload decoding; outer tag 1 is `Error` since #4001.                                                                                                                                                                                                                 |
| v3 `KnownStateDeclaration::ExactVersionSet` (tag 2)  | Payload decode (`protocol.rs`, `KnownStateDeclaration::Reserved2`).                                                                                                                          | Decode failure; tag 2 is uninhabited.                                                                                                                                                                                                                                                 |

Nothing reaches payload decoding across versions, so no old frame can be
reinterpreted. Receipt: `crates/jazz/tests/wire_fixtures.rs`,
`pre_v6_peers_are_refused_at_hello_with_a_typed_version_mismatch` replays frozen
v3 and v4 Hello frames, every frame of the frozen v5 Hello corpus
(`crates/jazz/fixtures/wire_hello_frames_v5.json`), a v3 Subscribe frame and a
tag-2 declaration. A v3 or v4 relay Hello carries the retired role tag 3 and
already fails frame decoding. A main (v5) peer pairs with a v6 peer the same
way as the alpha.59 rows above, with `remote 5..=5, expected 6..=6`.

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

Every root written before #4071 (all released roots, alpha.54 to alpha.59,
and every main build before #4071) declares the retired per-encoding ids, so
`missing` holds the current families it lacks: always
`groove.dictionary.v1` and `jazz.node-storage.v1`, `groove.durable-index.v2`
where it predates the compact index, and `groove.large-value.v2` and
`groove.ordered-chunk-storage.v2` for a root a real pre-#4071 build wrote
(the published and pre-linear corpora). The constructed roots of the
`linear_history_*` tests are built on today's Groove base, so they lack only
the Jazz-owned families. `unknown`
holds the retired ids it declares: `groove.large-value.v1`,
`groove.ordered-chunk-storage.v1` and its per-encoding `jazz.*` ids
(`jazz.branch-key.v1`, the `jazz.catalogue.*.v1` ids,
`jazz.subscription-program-fact-key.v1`, and, where present,
`jazz.history-version-current.*`, `jazz.transaction-durability.v2` and the
retired result ids).

| Root written by                                                                                                 | Receipt                                                                                                                                                                                                   |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| alpha.54 RocksDB (published)                                                                                    | `published_alpha54_rocksdb_root_is_refused_with_a_typed_format_error` (refusal is stable on retry); `published_alpha54_native_corpus_is_refused_with_the_typed_codec_error` (no record or family changes) |
| alpha.56 RocksDB with an unsynced edge-accepted write (previously opened and failed on first read)              | `published_alpha56_rocksdb_root_is_refused_with_a_typed_format_error`; `published_alpha56_legacy_edge_receipt_is_refused_without_rewriting_its_records`                                                   |
| pre-linear current SQLite and RocksDB corpora (`crates/jazz/fixtures/pre-linear-native-jazz*`)                  | `pre_linear_native_corpora_are_refused_before_any_mutation` (SQLite file byte-identical afterwards)                                                                                                       |
| older epoch-1 settlement SQLite corpus (also declares `jazz.result-member-key.v1`, `jazz.result-row-source.v1`) | same test; `retired_result_codec_profiles_reject_historical_native_roots`                                                                                                                                 |
| linear history before the compact index (alpha.59 index layout)                                                 | `linear_history_root_without_the_durable_index_family_is_refused`                                                                                                                                         |
| unreleased linear history before durability v2                                                                  | `linear_history_root_without_the_durability_v2_family_is_refused`                                                                                                                                         |
| unreleased linear history with the string partial-transaction marker                                            | `transaction_string_marker_root_is_refused_before_mutation`                                                                                                                                               |
| a #4071-format node root without interned authors (`missing` is only `groove.dictionary.v1`)                    | `node_root_without_the_dictionary_family_is_refused`                                                                                                                                                      |
| any other root kind, or a changed inventory                                                                     | `storage_codec_profile.rs`, `root_profiles_reject_other_roots_and_changed_inventories`                                                                                                                    |

The receipts are in `crates/jazz/tests/storage_format_refusal.rs`, except the
`published_*_native_corpus_*`, `published_alpha56_legacy_edge_*` and
`retired_result_*` tests (`native_storage_corpus.rs`). The manifest bytes of
every root profile are pinned in `storage_codec_profile.rs`
(`root_profiles_have_pinned_manifest_bytes`).

The current native corpus (`crates/jazz/fixtures/current-native-jazz-*`) is
re-pinned in the current node format and still has to reopen and accept writes
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
does not. The browser manifest lists the same six families as a native node
root, so an alpha.59 database gets the same `missing` and `unknown` lists.
The message keeps the older generic prefix, so existing matches on it still
hold:

```text
Missing or invalid IndexedDB storage epoch manifest: unsupported storage format:
this epoch-1 root lacks codec families [..missing..]
required by this build and declares [..unknown..] that this build does not read
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

- **Upgrade clients and servers together.** A v3, v4 or v5 peer and a v6 peer never sync.
- **Delete old stores.** Native data directories (RocksDB, SQLite) and browser
  IndexedDB databases written by alpha.59 or earlier (or by main after #3659)
  must be removed, then the
  client resyncs from Core. Any write that had not reached Core is lost.
- **Core and relays** start from an empty root. There is no converter from DAG
  history to row-state history.

## Storage-compatibility checks

The refusals above are the expected result, and CI asserts them:
`test-storage-compat` runs `dev/gates/storage-compat.sh` (the native corpus
receipts and the `storage_format_refusal` receipts), and the TypeScript
partition runs the browser corpus receipts. The published alpha.54 and
alpha.56 archives are append-only evidence and stay as refusal receipts. New
current corpora must be written in the current node format.

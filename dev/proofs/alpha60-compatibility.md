# alpha.60 compatibility: one wire break, one storage break

> **Not wire compatible and not storage compatible with alpha.59, by design.**
> alpha.60 refuses alpha.59 peers at the `Hello` handshake and refuses every
> store written by alpha.59 or earlier when it opens it. There is no converter,
> dual reader or downgrade path. Users wipe local stores and resync; Core and
> relays start from empty roots.

This page puts together the alpha.60 tracks, so a reviewer can check that
every byte change sits behind one of the two refusals. Each track has its own
detailed write-up; this page links them and does not repeat their receipts.
Citations are `path` plus symbol, because line numbers shift with every merge.

State: this is the docs pass of track F. Tracks A, B, D, E and G are on main.
C (#4023) and the storage drafts #4088, #4103 and #4109 are not, and the
version bump itself (wire v7, native relay ABI 3, fixture regeneration) lands
in a later pass. The rows below say which.

## Tracks

| Track                      | PRs                                                                       | Wire                                                                                                                                                                                                                                              | Disk                                                                                                                                                                                                                   | Write-up                                                                                                               |
| -------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Baseline (before the plan) | #3651, #3659, #4001, #3281 (all merged)                                   | Schema-predecessor vectors (v4), compact durability tags and no Edge role (v5), obsolete messages removed and `SyncMessage` tags renumbered within v5 (#4001), linear history: no `parents`, `base` and `counter_signs` on every row version (v6) | Linear row-state history, durability v2, compact secondary indexes, touched-row lists instead of a `by_tx` index                                                                                                       | [linear-history-storage-compatibility.md](linear-history-storage-compatibility.md)                                     |
| A. Refs and shadow         | #4007, #4009, #4022, #4067, #4072 (merged)                                | `RetryLater` (tag 29) becomes the uninhabited `Reserved29`, never reused (#4072)                                                                                                                                                                  | `ahead_shadow` tables removed (#4022); new node-local `jazz_superseded_writes_v1` direct store (#4072), never on the wire                                                                                              | [linear-history-storage-compatibility.md](linear-history-storage-compatibility.md), "Node-local direct store"          |
| G. Storage format cleanup  | #4071 (merged)                                                            | `JVRR` row bytes change under one marker (large-value scalar tags 2/3 → 0/1, descriptor and record layouts); `BranchKey` value bytes; catalogue schema payloads always v2; lineage publication ids change                                         | One Jazz marker per root (`jazz.node-storage.v1`, `jazz.catalogue-storage.v1`, `jazz.account-storage.v1`); `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`; obsolete stores and fallbacks removed           | [storage-format-cleanup-compatibility.md](storage-format-cleanup-compatibility.md)                                     |
| D. Protocol                | #4016 (merged)                                                            | `Hello` version prefix and `Error` frame pinned across all versions (#2468); new `SyncMessage::ReadBarrier { writes_through }`, tag 30 (#3863). No existing message changes bytes                                                                 | None                                                                                                                                                                                                                   | #4016 PR body, "Compatibility write-up"; SPEC 8 §8.1.1, §8.2.1                                                         |
| E. Value codec             | #4019 (merged)                                                            | `JVRR\x03`: row descriptors defined once per sync message, then referenced (#3380). Every message carrying a `VersionRecord` changes bytes                                                                                                        | None (`JVRR` is wire-only). Tuple member decoding fixed (#2759); stored tuple bytes unchanged                                                                                                                          | #4019 PR body, "Compatibility write-up"; SPEC 16                                                                       |
| B. Transaction record      | #4021 (merged)                                                            | `PredicateRead` gains a trailing `aggregate_result` (#3768). Author codes never leave the node                                                                                                                                                    | `jazz.tx-record.v2`, `jazz_tx_provenance`, `jazz_pending_transactions`, reserved `jazz_history_horizon`, `groove.dictionary.v1` (interned authors), `jazz.exclusive-read-evidence.v2`, `jazz.aggregate-read-result.v1` | [tx-record-v2-storage-compatibility.md](tx-record-v2-storage-compatibility.md)                                         |
| C. Storage layout          | #4023 (approved, not merged)                                              | Union cells in the same `JVRR\x03` envelope; a descriptor-table reference `k` is now written `2k`, so multi-row messages change bytes even without union cells                                                                                    | Union row cases (variant tag bit 31) on history, current and ahead tables; node marker `jazz.node-storage.v1` → `.v2`                                                                                                  | [linear-history-storage-compatibility.md](linear-history-storage-compatibility.md), "Union row cases" (added by #4023) |
| Storage tightening         | #4088, #4103, #4109 (drafts); table-id keys and other items (in progress) | None: aliases and timestamps are widened back at the storage boundary, and the wire keeps `u64` and full values                                                                                                                                   | 4-byte node aliases, 6-byte timestamps (#4088); history holds only superseded images (#4103); `ahead_current` derived in memory, its column family removed (#4109)                                                     | Each PR body; #4109 also edits [linear-history-storage-compatibility.md](linear-history-storage-compatibility.md)      |
| F. The bump                | this PR (docs), then the bump pass                                        | `WIRE_PROTOCOL_VERSION` 6 → 7; native relay ABI 2 → 3                                                                                                                                                                                             | No new marker: the node marker stays at C's `jazz.node-storage.v2`; fixtures and corpora regenerated once                                                                                                              | This page                                                                                                              |

## Why mixed deployments fail closed

### On the wire: the `Hello` version check

Every connection starts with `WireFrame::Hello` (tag 0), whose first fields
are `min_protocol_version` and `max_protocol_version`. Since D (#4016) every
receiver reads that range before the version-scoped body
(`crates/jazz/layers/protocol/src/wire.rs`, `decode_hello_frame`), so a
changed `Hello` body is reported as a version mismatch, not as a malformed
frame. `negotiate_wire` (`wire.rs`) accepts only `min == max ==
WIRE_PROTOCOL_VERSION` and otherwise sends one `WireFrame::Error` (tag 1,
`UnsupportedProtocolVersion`, retry `Never`) and closes. No sync message is
decoded before this check, so no old frame is ever reinterpreted.

| Pair                                      | Result                                                                                                |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| alpha.59 (v3) client → alpha.60 (v7) Core | Refused by the server: `unsupported wire protocol advertisement: remote 3..=3, expected 7..=7`        |
| alpha.60 client → alpha.59 Core           | Refused by the old server the same way; a TypeScript client reports `unsupported_protocol_version`    |
| alpha.60 TypeScript client, old server    | `server must advertise exactly wire protocol 7, got 3..=3` (`websocket.ts`, `decodeServerHello`)      |
| Unreleased `main` builds (v4, v5, v6)     | Refused by v7 at `Hello`                                                                              |
| React Native JS bundle vs native build    | Native relay ABI 3 against 2: refused at startup with "new native development/release build required" |

The last two rows are why the bump is required and not optional. Between
alpha.59 and alpha.60, `main` changed bytes under one version number several
times:

- **v5:** #4001 renumbered `SyncMessage` tags in place. `main` builds from
  before and after it both advertise v5, pass `Hello` and then misread each
  other's messages.
- **v6:** #4071 (scalar tags, lineage ids), #4019 (`JVRR\x03`), #4021
  (`aggregate_result`), #4016 (tag 30) and #4023 (`2k` references) all
  changed v6 bytes without a version change. Two `main` builds from either
  side of any of them pass `Hello` and then fail with decode or content-id
  errors, or reject tag 30 as malformed.
- **Relay ABI 2:** #4071 changed React Native foreground row payloads while
  the ABI stayed 2.

None of these builds was released. The v7 and ABI 3 bump makes every one of
them a typed refusal against alpha.60, which is the only guarantee alpha.60
gives: a released build never syncs with a build of another format.

Receipt (after the bump): `crates/jazz/tests/wire_fixtures.rs`,
`pre_v6_peers_are_refused_at_hello_with_a_typed_version_mismatch`, renamed
and extended in the bump pass to replay the frozen v3, v4, v5 and v6 `Hello`
frames against v7.

### On disk: manifest admission at open

Every root carries a `JSM1` storage epoch manifest that lists its codec
families. Opening a root compares that list with the build's profile
(`crates/groove/src/storage/manifest.rs`,
`StorageEpochManifest::admit_existing`) before any ordinary key is read, and
requires an exact match. A mismatch is the typed
`groove::storage::Error::UnsupportedStorageCodecs { epoch, missing, unknown }`;
in the browser it is `UnsupportedStorageCodecsError` with `code`
`"unsupported_storage_codecs"` (`indexeddb-page-store.ts`,
`assertStorageManifest`). A missing or damaged manifest stays a generic error,
so an old store can be told from a broken one.

The alpha.60 profiles (`crates/jazz/layers/protocol/src/storage_codec_profile.rs`):

| Root                | Families                                                                                                                                                                                |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Client, relay, Core | `groove.dictionary.v1`, `groove.durable-index.v2`, `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, `groove.ordered-kv.v1`, `jazz.node-storage.v1` (`.v2` once #4023 merges) |
| Server catalogue    | `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, `groove.ordered-kv.v1`, `jazz.catalogue-storage.v1`                                                                         |
| Account registry    | `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, `groove.ordered-kv.v1`, `jazz.account-storage.v1`                                                                           |

The browser IndexedDB manifest lists the same families as a node root
(`indexeddb-page-store.ts`, `JAZZ_EPOCH_1_STORAGE_CODEC_IDS`). The physical
storage epoch stays 1: the break is carried by the family names.

| Store                                           | Result                                                                                                                                                                                                                           |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| alpha.59 or earlier, opened by alpha.60         | Refused with the typed error before any record is decoded or written. No alpha.59 root declares a `jazz.*-storage` marker, so every one lacks it                                                                                 |
| alpha.60, opened by alpha.59 (downgrade)        | Refused before any mutation, but untyped: alpha.59 requires an exact manifest match and reports `storage manifest is inconsistent with this adapter` (native) or `Missing or invalid IndexedDB storage epoch manifest` (browser) |
| catalogue or account root opened as a node root | Refused with the typed error (`root_profiles_reject_other_roots_and_changed_inventories`)                                                                                                                                        |

Receipts: `crates/jazz/tests/storage_format_refusal.rs` (published alpha.54
and alpha.56 roots, pre-linear corpora, constructed pre-cleanup roots),
`crates/jazz/layers/node/src/node/tests/native_storage_corpus.rs`,
`storage_codec_profile.rs` (`root_profiles_have_pinned_manifest_bytes`), and
the browser corpus tests in
`packages/jazz-tools/tests/browser/indexeddb-jazz-compat.test.ts`. The
downgrade row is argued from alpha.59 source in
[tx-record-v2-storage-compatibility.md](tx-record-v2-storage-compatibility.md),
"Downgrade to alpha.59"; no test runs an alpha.59 reader.

### Known gap between unreleased builds

The storage tightening drafts (#4088, #4103, #4109) change the node layout but
keep the node marker, because `jazz.node-storage.v2` has never shipped and the
plan is one marker for the whole alpha.60 layout. So two unreleased `main`
builds on either side of one of them both admit the other's root and fail
later, on the first record they misread. #4109 is the exception that is safe
by construction: an earlier alpha.60-epoch root opens normally and its old
`__groove_class_ahead_current` family is left unread. Released builds are not
affected: alpha.59 roots are refused by the marker, and alpha.60 is the only
release with `jazz.node-storage.v2`. Anyone running a `main` build against a
persistent store before alpha.60 ships must wipe it when updating.

## What users must do

Covered for users in `docs/content/docs/reference/upgrading-to-alpha-60.mdx`:

- Upgrade clients, native builds, relays and Core together. React Native needs
  a new native build.
- Delete every local store written by alpha.59 or earlier and resync. Writes
  that never reached Core are lost.
- Start Core, relays, the catalogue and the account registry from empty
  roots. Data that exists only in alpha.59 stores is not carried over.

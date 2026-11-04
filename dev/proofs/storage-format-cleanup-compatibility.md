# Storage format cleanup compatibility (#4071)

> **Not storage compatible and not wire compatible, and this change alone does
> not bump the wire version.** It must not ship in a release without the
> alpha.60 wire version bump (and native relay ABI bump) that lands with it.
> Upgrading also requires wiping every root: there is no converter.

Comparison: main before #4071 (merge base `57ec639969`: linear history, wire
v6, unreleased) against the #4071 branch, which has since merged main at
`f3111247c9` (#4072, #4053). The last release, alpha.59, speaks wire v3 and
writes the DAG layout; it is already refused at the Hello and at manifest
admission (`linear-history-storage-compatibility.md`), and nothing here
changes that. Citations are `path` plus symbol.

This change assigns no version numbers. It keeps wire v6, native relay ABI 2,
storage epoch 1 and has no changeset. The alpha.60 release work bumps the wire
version and the native relay ABI and writes the single changeset. It keeps the
per-root storage markers introduced here as the epoch identity.

## What changed

- **Storage markers.** Each independently persisted root requires exactly one
  Jazz marker on top of Groove's base (`groove.large-value.v2`,
  `groove.ordered-chunk-storage.v2`, `groove.ordered-kv.v1`): node roots
  `jazz.node-storage.v1` plus `groove.durable-index.v2`, the server catalogue
  `jazz.catalogue-storage.v1`, the account registry
  `jazz.account-storage.v1`
  (`crates/jazz/layers/protocol/src/storage_codec_profile.rs`). The previous
  per-family Jazz IDs (`jazz.branch-key.v1`, `jazz.catalogue.*.v1`,
  `jazz.history-version-current.v4`, `jazz.subscription-program-fact-key.v1`,
  `jazz.transaction-durability.v2`) are retired. The browser manifest carries
  the same list (`packages/jazz-tools/src/runtime/indexeddb-page-store.ts`).
- **Groove large values** (`groove.large-value.v2`, Groove SPEC 9): scalar
  tags `Primitive`/`Chunked` move from 2/3 to 0/1 (the reserved tags 0/1 are
  reused, see SPEC 9 "Scalar tag history"). Descriptors and branch children
  store UTF-16 lengths only for string/JSON. Byte edits store no UTF-16
  coordinates. The inserted UTF-16 length of an edit is derived from its
  inserted bytes instead of stored. Branch bytes and object hashes change; leaf
  bytes do not. Completed uploads get a dedicated record.
- **Ordered chunk storage** (`groove.ordered-chunk-storage.v2`): the
  per-chunk magic prefix is gone.
- **Node records:** retired subscription-cache stores and the retired
  scope-relay repair ledger are dropped; the partial-transaction marker is a
  boolean; rejection reasons are kind-specific payloads; the active schema is
  the only persisted schema selection; history rows require the merge columns.
- **Catalogue payloads:** catalogue schema bytes always use version 2 (they
  used 1 unless a table declared a composite index). The lineage-publication
  body and the jazz-server catalogue payload codec are gone: the catalogue is
  typed rows with canonical JSON definitions (SPEC ch. 10 §10.2).

### Wire bytes that change with wire v6 unchanged

| What                       | Change                                                                                         | Receipt                                                                                                                                                                                                        |
| -------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `JVRR\x02` row payloads    | Same marker, different bytes: scalar tags, large-value descriptor/edit layouts, record layouts | `protocol.rs`, `immutable_version_row_codec_preserves_schema_and_excludes_execution_bindings` (pinned hash); `crates/jazz/fixtures/wire_message_frames_v6.json` (ViewUpdate frames); `large_json_wire_v1.json` |
| `BranchKey` column values  | A string value encodes as `[1, 6, 0, ...]` instead of `[1, 6, 2, ...]`                         | `protocol.rs`, `branch_coordinate_codec_has_frozen_declared_type_bytes`                                                                                                                                        |
| Catalogue schema payloads  | Version byte 2 for every schema                                                                | `node/codec.rs`, `catalogue_schema_payload_rejects_retired_and_unknown_versions`                                                                                                                               |
| Lineage publication IDs    | Single-predecessor publications now hash in the v2 domain with a count, so their IDs change    | `node/codec.rs`, `predecessor_publications_use_one_counted_encoding`; `crates/jazz/fixtures/catalogue-predecessors.hex`; catalogue frames in `wire_message_frames_v6.json`                                     |
| RN foreground row payloads | Carry the new scalar tags; `NATIVE_RELAY_ABI_V1` stays 2                                       | `dev/rn-device-acceptance/src/foreground-byte-abi.ts`                                                                                                                                                          |

## What old peers see

| Pair                                                        | Result                                                                                                                                                                                                                                                          |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| alpha.59 (v3) and #4071 (v6)                                | Refused at the Hello with `UnsupportedProtocolVersion`, as before (`wire_fixtures.rs`, `pre_v6_peers_are_refused_at_hello_with_a_typed_version_mismatch`).                                                                                                      |
| main before #4071 (v6) and #4071 (v6), until the bump lands | Both advertise v6, so the Hello passes. Rows then fail with decode errors (a v2 scalar tag is unknown to the other side), branch keys of the same logical branch differ, and catalogue publications fail content-ID checks. No typed version refusal is raised. |
| JS bundle and native build from either side of #4071        | Both report native relay ABI 2, so the ABI check passes; foreground row payloads then fail to decode.                                                                                                                                                           |

Only unreleased builds (main after #3281, before #4071) speak v6, so no
released client is in the second row. Preview or staging deployments built
from main are. The alpha.60 bump moves #4071 builds to a new wire version and
native relay ABI, which turns the second and third rows into typed refusals.

## What old stores see

Manifest admission (`crates/groove/src/storage/manifest.rs`,
`StorageEpochManifest::admit_existing`) decodes the manifest structurally and
compares the complete codec inventory before any ordinary key is read. Every
earlier root is refused with the typed
`groove::storage::Error::UnsupportedStorageCodecs { epoch: 1, missing, unknown }`,
including roots that still declare the retired `groove.large-value.v1` and
`groove.ordered-chunk-storage.v1`. (Earlier commits of this branch reported
those roots with a generic `InvalidStorageLayout`; this was fixed before
merge.)

| Root written by                     | `missing`                                                                               | `unknown`                                                                                                                                   |
| ----------------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| main before #4071, node root        | `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, `jazz.node-storage.v1`      | `groove.large-value.v1`, `groove.ordered-chunk-storage.v1`, the ten base `jazz.*` IDs, `jazz.history-version-current.v4`                    |
| main before #4071, catalogue root   | `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, `jazz.catalogue-storage.v1` | `groove.large-value.v1`, `groove.ordered-chunk-storage.v1`, the ten base `jazz.*` IDs                                                       |
| main before #4071, account root     | `groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, `jazz.account-storage.v1`   | `groove.large-value.v1`, `groove.ordered-chunk-storage.v1`, the ten base `jazz.*` IDs                                                       |
| alpha.54 to alpha.59 and pre-linear | as the node row, plus `groove.durable-index.v2`                                         | the Groove v1 IDs and the base `jazz.*` IDs that root declared (no `jazz.transaction-durability.v2`), plus retired result IDs where present |

The ten base `jazz.*` IDs are `jazz.branch-key.v1`, the seven
`jazz.catalogue.*.v1` IDs, `jazz.subscription-program-fact-key.v1` and
`jazz.transaction-durability.v2`.

The browser refuses the same roots with `UnsupportedStorageCodecsError`
(`code` `"unsupported_storage_codecs"`).

The current native and browser corpora were regenerated in the new layout, so
there is no producer-made corpus of a pre-#4071 linear root. Its refusal is
proved with constructed manifests instead.

Receipts:

- `crates/groove/src/storage/manifest.rs`:
  `scalar_v1_root_is_refused_before_scalar_decoding`,
  `ordered_chunk_v1_root_is_refused_before_chunk_decoding`.
- `crates/jazz/layers/protocol/src/storage_codec_profile.rs`:
  `root_profiles_have_pinned_manifest_bytes`,
  `root_profiles_reject_other_roots_and_changed_inventories`.
- `crates/jazz/tests/storage_format_refusal.rs`: published alpha.54/alpha.56
  roots, pre-linear corpora, `linear_history_root_without_*`,
  `transaction_string_marker_root_is_refused_before_mutation`.
- `crates/jazz/layers/node/src/node/tests/native_storage_corpus.rs`
  (`node::tests::harness::`): historical corpora refusals and
  `committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes`.

```sh
cargo test -p groove --lib storage::manifest
cargo test -p jazz-protocol --lib storage_codec_profile
RUST_MIN_STACK=4194304 cargo test -p jazz --features testing --test integration storage_format_refusal wire_fixtures persistent_codec_family_registry
```

## UTF-16 range reads

The cleanup removes stored UTF-16 data only where it is redundant. Text
descriptors still store the total UTF-16 length, text branch children still
store each subtree's UTF-16 length, and text edits still store `utf16_offset`
and `delete_utf16_length`. A UTF-16 range read therefore still skips whole
subtrees by their stored lengths and reads only boundary leaves
(`large_values.rs`, `utf16_range_attempt`, `base_utf16_length_for_byte_range_attempt`;
receipt `crates/groove/tests/large_value_query.rs`,
`utf16_ranges_use_tree_metrics_and_tail_coordinates_without_prefix_hydration`).
The derived `insert_utf16_length` is computed from the edit's own inserted
bytes when the descriptor is decoded, which reads no base text.

## What users must do

- Upgrade clients and servers together, on the release that carries the wire
  version bump.
- Delete old native data directories and browser IndexedDB databases; clients
  resync from Core. Writes that had not reached Core are lost.
- Core, relays, the server catalogue and the account registry start from empty
  roots.

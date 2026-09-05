# Typed field identity compatibility evidence (#2558)

Historical experiment record: the governing decision is now the all-in typed
format. See Jazz SPEC 16 and [durable role receipts](typed-identity-durable-roles.md)
for the current boundary; the comparisons below do not impose a compatibility goal.

Comparison: contained `4c6eafaef5ad038a71b4583f650b22ccfd7b71db` and typed
`24d6c5b2d1dfcadcda9e150d8896199d33955354`. These revisions are **not storage
compatible**. This document and the test-only codec snapshot change no
production format and establish no cross-version database reopen guarantee.

Run the component proof with:

```sh
ulimit -n 65536
RUST_MIN_STACK=4194304 cargo test -p groove --lib typed_identity_compatibility_proof --no-fail-fast -- --nocapture
```

The contained codec fixture is copied from that exact revision's
`crates/groove/src/records/values.rs`. Its sole source adaptation supplies
`identity: None` to the newer Rust `DescriptorField` initializer. This permits
both independent codec algorithms to run against the same ordinary record
encoder; it does not test the old database executable or old storage engine.

## Findings

- Even a plain `value: U64` descriptor changes from 83 to 130 bytes. Neither
  descriptor decoder accepts the other's output. Typed `identity_name` and
  `identity_slot` extend every encoded descriptor node without a discriminator;
  exact canonical re-encoding rejects the other layout.
- Current-row and nested-array descriptors likewise differ. Raw scalar row
  bytes can remain equal while their descriptors differ.
- Recursive logical-output normalization can preserve separate public names
  `score` and literal `user_7`, while removing compiler carriers `user_7` and
  `user_user_7`. The _contained_ encoder then gives the same bytes as a directly
  constructed public descriptor. The current typed encoder still differs.
- Normalizing a parent's descriptor alone is insufficient: record-valued child
  `OwnedRecord`s must also be rebound recursively, because nested descriptor
  equality is enforced by record construction.
- Arbitrary `NamedSlot { name: "score", slot: 7 }` and slot 8 cannot be
  reconstructed from a normalized standalone public descriptor. They intentionally
  collapse to the same application field. Retaining executable binding meaning
  needs external query/catalogue context or an explicit durable representation.
- Descriptor bytes participate in synthetic group/replacement keys, flat-join
  revision preimages, and root-layout hashes. A codec-only reader fix would not
  preserve logical result identities.
- Physical index names differ (`by_physical_app_v1_7` versus
  `by_physical_user_v1_7`); the production Groove index-key function consequently
  returns different durable prefixes. Physical column carrier spellings also
  differ (`_app_7` versus `user_7`). Sharing a descriptor codec alone is insufficient.

## Governing semantics and recommendation

Jazz SPEC 18, “What is public, wire, and internal,” classifies lowered graphs,
terminal schemas, routing fields and physical carrier choices as internal.
Its non-negotiable rule 2 says provenance slots are compiler bookkeeping, not
application cells. Groove SPEC 2 §2.2 defines logical declaration-order fields;
`INV-STORAGE-27` requires inline nested descriptors and canonical child bytes.
Neither specifies the experimental Rust `FieldIdentity` enum as durable data.
Physical column identity already has a durable owner: Jazz's physical catalogue.

Prefer a shared, canonical persisted representation that records application
names/types/values and necessary explicit provenance, with physical identity
owned by the catalogue. Keep execution bindings as reconstructed compiler state.
This permits principled typed internals without automatically persisting every
`Name`/`Slot`/`NamedSlot` allocation.

However, the proof does **not** establish that current contained payloads already
meet this representation for every nested, aggregate, and joined result. Route A
(preserve contained bytes) needs complete producer normalization and recovery
receipts before freeze. Route B (uplift contained codec) also needs both writers
to produce the same semantic metadata and both readers to preserve it; defaulting
contained fields to no identity while typed fields default to `Name` is not
compatible. No definite shared-format freeze is justified by these proofs alone.

## Exact descriptor-boundary inventory

Production `encode_record_descriptor` callers in Jazz:

| File and function                                                     | Encoded role                                                                |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `node/codec.rs::settled_result_value_storage_bytes`                   | Authoritative synthetic group/replacement value identity, recursively typed |
| `node/maintained_subscription_view.rs::terminal_root_layout`          | Root-layout hash input under the unchanged v1 domain                        |
| `node/maintained_subscription_view.rs::decode_typed_terminal_record`  | `ResultCurrent` and `AggregateResult` descriptor/record payloads            |
| `node/maintained_subscription_view.rs::decode_aggregate_app_row`      | Aggregate application terminal payload                                      |
| `node/maintained_subscription_view.rs::flat_join_row_digest_preimage` | Public tuple descriptor in durable v1 revision preimage                     |

Recovery consumers are `node/query_eval/materialization.rs` functions
`current_row_from_aggregate_result_payload` and `current_row_from_result_payload`.
The latter currently rebuilds top-level fields from carrier names. Validation in
`node/codec.rs` rejects noncanonical descriptor/value re-encodings. The native
binding boundary is separate: `binding_codec.rs::row_batches` encodes tagged
field provenance and recursive `ValueType`, which also needs a shared ABI receipt.

A complete optionality receipt needs both directions: contained write → typed
open/write → contained reopen, and the reverse; schemas/lenses, enum payloads,
nested includes, aggregate identity, flat-join revision identity, indexes,
reconnect/no-op sync, and recovery must agree. Running both codec implementations
inside one process is deliberately narrower than that receipt.

## First Route A implementation boundary

The typed physical catalogue now uses the contained `_app_{PhysicalColumnId}`
field spelling and `by_physical_app_v1_{PhysicalColumnId}` index spelling. The
numeric ID remains the persisted semantic identity. Construction of execution
source descriptors binds that exact ID as `Slot`/`NamedSlot` using the catalogue
and the selected schema; this metadata does not rename the durable field.
The physical descriptor enum-registry resolver and index-write classifier use
the same restored spellings. Query/application carriers remain unchanged in this
first isolated step.

Existing indexed backfill across schema variants, catalogue restart, and
rename/physical-ID reuse tests pass in the typed implementation. These are
same-version behavioral checks, **not** cross-version database receipts. The
descriptor-codec and payload/digest gaps documented above remain open in #2558.

## Canonical semantic-value and revision boundary

The explicit persisted descriptor helpers now emit/read the existing v1 grammar
and preserve exact stored names, field order, nested types and enum registry IDs.
They omit execution bindings without attempting public-name inference. The
independent contained encoder and decoder agree with these helpers, including
nested records whose execution bindings differ from their durable names.

Only `settled_result_value_storage_bytes` (and its canonical validator) and
`flat_join_row_digest_preimage` adopt this boundary in this step. Their outer
field names (`value` and `flat_join_payload_{ordinal}`) and authoritative value
types already determine the persisted descriptor; runtime aliases and source
column IDs do not determine the synthetic key. A U64 group value 4 therefore has
the original descriptor+value identity bytes. Joined public tuples retain their
original `JFRD`, version 1, count/length framing and hash, including the existing
nullable-value normalization.

The original `result_member_storage_codec_has_permanent_tags_and_golden_bytes`
and `flat_join_row_digest_uses_the_v1_groove_record_envelope` tests now pass
without changing their golden bytes. The new canonical descriptor proof pins
the original nested descriptor hash `e7fcf66bb23dd514678c3b3960b69f020935d01a366c83d7b6fda963d2346e0a`.
The existing broader nested-settled test still calls the experimental execution
descriptor codec directly when constructing a result payload; that known failing
boundary remains visible. `ResultCurrent`, aggregate payload recovery, root-layout
hashes, the binding ABI, and both cross-version database directions remain
unproven. There is no shared-format-complete claim.

## Source-owned carrier parity trial

Schema constructors now produce the same durable carrier names as the contained
writer. `schema::app_storage_column_name` prefixes an application name exactly
once with `_app_`: `score` becomes `_app_score`, literal `_app_score` becomes
`_app__app_score`, and literal `user_score` becomes `_app_user_score`. Qualified
execution carriers use `_app__{table}__{column}`. Physical catalogue fields remain
`_app_{PhysicalColumnId}`; the catalogue, not a string parser, supplies physical
identity. These namespaces can have identical strings in different roles, so
runtime `FieldIdentity` and explicit publication bindings remain authoritative.

The producer/read-side changes cover history and current content schemas,
branch/current indexes (`by_app_{column}`), immutable receipt descriptors and
validation, query source field constructors and source type lookup, and the
native row shape/name helpers. Physical indexes remain
`by_physical_app_v1_{PhysicalColumnId}`. Prefixing is performed at the owning
constructor, never by rewriting an arbitrary terminal descriptor before encoding.
No descriptor or hash golden bytes are updated in this trial.

Public conversion resolves a requested application column through the CurrentRow
application binding. Looking up a generated carrier with the general logical
field resolver is incorrect: with application columns `score` and `_app_score`,
that lookup can return the second column's value for the first. A public client
regression orders each column separately and verifies every returned value,
including literal `_app_1`, `user_score`, and aggregate-shaped `sum_score`.
Names in the reserved `__jazz_` namespace remain rejected by the public schema.

This establishes constructor spelling and collision handling within the typed
implementation, not complete byte compatibility. In particular the native ABI
still differs: the shared target is contained `StoredColumn { id: u64,
outputName: string }`, with its ID supplied explicitly by the catalogue/source
producer. The current typed `PhysicalColumn(string)` cannot satisfy that contract.
ResultCurrent payloads, root-layout hashes, descriptor serde in peer VersionRecord
messages, and full database write/open/write/reopen in both directions remain
unproven. Execution Slot/NamedSlot recovery must be demonstrated at those owners;
this trial must not be described as a completed format freeze.

### Native publication proof before migration

`publication_compatibility_proof_pins_stored_id_and_nested_descriptor_gaps`
independently declares the contained publication enum and pins its postcard
bytes. Stored column ID7 exposed as `score` is `00 07 05 73 63 6f 72 65`;
a derived result named `_app_score` is `01 0a 5f 61 70 70 5f 73 63 6f 72 65`.
The typed physical-name-only variant fails decoding under that frozen contract.

The recursive type envelope is a second independent gap. Contained
`Record([score: U64])` is `10 01 01 05 73 63 6f 72 65 03`; typed `ValueType`
serde additionally serializes `DescriptorField.identity` and rejects the
contained bytes. Restoring only the publication enum therefore cannot establish
nested includes or enum payload compatibility. A role-specific recursive native
wire type must omit execution bindings while preserving exact durable names,
ordered field types, and enum metadata. Generic execution-descriptor serde must
retain its compiler bindings; changing it globally is not this migration.

## Unified native publication trial

A CurrentRow now has one per-slot publication metadata owner:
`StoredColumn { PhysicalColumnId, output_name }`, `ResultField { name, visibility }`,
or construction-only `UnresolvedSourceCell { output_name }`. Constructor roles
and compatibility name accessors do not store additional copies. Query
`FieldIdentity` remains independent: a compiler Slot is not assumed to be a
catalogue column ID. Schema-aware materialization resolves source cells using
the selected read schema's physical catalogue. Resolved query sources supply
that same catalogue mapping to collector and aggregate lowering; projection,
root layouts and cached record replacement carry the resulting bindings.
The root layout asserts that its public slot name agrees with its binding.

The native codec serializes only finalized bindings. Variant0 is exactly
`StoredColumn { id: u64, output_name: string }`; variant1 is exactly
`ResultField { name: string }`; variant2 is exactly
`HiddenMetadata { name: string }`. These are ordered postcard enum tags, followed
by the named payload fields in the order shown. Unresolved cells fail
serialization rather than acquiring a fabricated ID.

Result visibility has three producer-owned roles: ApplicationCell,
PublicProvenance and HiddenMetadata. Subscription application-cell comparison
includes ApplicationCell and stored cells, while public magic provenance is
excluded from that comparison. Native publication emits ApplicationCell and
PublicProvenance as tag1, and only HiddenMetadata as tag2. Consumers suppress
only tag2, never a spelling. `$createdAt` and `$updatedBy` remain public even
when explicitly projected. A visible `COUNT(*) AS schema_version` can coexist
with hidden `schema_version` metadata without collision. Catalogue identity and
output names belong to StoredColumn; compiler Name/Slot identities are not
serialized in this native publication envelope.

The native type envelope recursively writes the contained serde shape: a record
is an ordered vector of `{ name: Option<string>, value_type }`, without runtime
identity; arrays, nullable values, tuples and enum case payloads recurse through
that representation. Scalar type variant numbers and enum registry metadata
remain unchanged. This is a native publication DTO, not a blanket change to
execution descriptor serde or VersionRecord peer-wire serialization.

The unified-binding checkpoint first restored the original contained relation
snapshot golden verbatim, including adjacent/nonadjacent batches and a deleted
row. The approved tag2 uplift below changes only explicit hidden metadata tags;
an independently declared shared publication reader decodes and re-encodes the
resulting bytes. The nested scalar-record type proof pins the old
bytes as well. These are component receipts, not a cross-database guarantee.
Grouped one-shot publication retains the source column's catalogue ID. The
subsequent grouped reset repair retains the exact DescriptorFields emitted by
aggregate lowering, including their FieldIdentity, instead of reducing them to
carrier names. Group and aggregate-value lookup use that identity. Cached
aggregate payload materialization finalizes source cells against the selected
read schema's physical catalogue before publication, just like the noncached
path. The public Db proof checks grouped reset count/value and native encoding.

### Approved pre-freeze native visibility uplift

The original two-tag ABI cannot distinguish a valid result alias from same-named
hidden metadata. The executable counterexample uses `schema_version: U64`; query
validation permits `COUNT(*) AS schema_version`, but the contained adapter hid
that name. The approved shared target adds HiddenMetadata as tag2 while retaining
exact tag0/tag1 encodings. It changes native host publication only, not durable
Groove descriptors, catalogue/index names, row keys or hashes.

`explicit_hidden_metadata_tag_preserves_same_named_public_alias` now places
hidden metadata, a visible alias and public magic provenance in one row. It pins
`02 0e schema_version` versus `01 0e schema_version`. The TypeScript consumer
fixture verifies the visible value survives and the hidden value does not leak;
the real Db producer fixture exercises public magic projection and aggregate
aliases as well as current, nested, join and root-reset publication.

The native golden update is intentional: only row_uuid metadata tags change
from1 to2 in populated relation snapshots and subscription deltas. Existing
stored IDs, visible names, recursive types, row bytes, edges, keys and ordering
must remain identical. The original two-tag codec fixture still pins tag0/tag1.

Both implementation alternatives must adopt the same third tag before freeze.
The contained implementation must classify engine metadata at source/projection
owners, carry it through caches and root reducers, and preserve explicit public
magic provenance as ResultField1. Its adapter must remove name-based hiding and
skip only tag2. Merely updating its enum/decoder is insufficient: a logical
alias with no separate output-name override must stay visible, so absence of an
override cannot classify hidden metadata. No physical ID may be inferred from
carrier spelling or compiler slots.

This typed trial does not yet prove the contained producer port or two-way host
artifact compatibility. VersionRecord peer-wire descriptors, durable
ResultCurrent payloads, root-layout hashes, and both directions
of database reopen/recovery still prevent a shared-format-complete claim.

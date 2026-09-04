# Typed field identity compatibility evidence (#2558)

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

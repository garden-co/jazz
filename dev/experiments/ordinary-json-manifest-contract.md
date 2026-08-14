# Ordinary JSON embedded-manifest adapter contract

This is the JSON-specific follow-on to the shared ordinary-content manifest
foundation. It deliberately does not introduce a JSON-specific mutable head:
the application row version is the head and carries one atomic JSON content
value.

## Public value and atomicity

```text
JsonContent = {
  root: JsonRootId,
  editTail: Bounded<JsonOperation>,
}
```

`JsonContent` is one candidate in the containing row's version/merge model.
`root` and `editTail` MUST never be independently selected, merged, indexed,
or delivered: a root from one candidate plus a tail from another can name a
snapshot that no writer authored. A historical version of the application row
therefore directly names an exact JSON snapshot without a content-head lookup.

The schema declaration identifies the subtype (`content.json()`); ordinary
values do not redundantly carry a runtime type tag. The shared foundation owns
the carrier encoding, candidate identity, and registry hooks. This adapter owns
the JSON codec, operation validation, materialization, and JSON-specific merge
semantics.

## Immutable JSON structure

`root` names a content-addressed immutable `JsonRoot` in the same authorization
and encryption domain as the owning content column. It names:

- a stable logical JSON node graph: scalar, object, array, and tombstone nodes;
- object-member maps keyed by member name;
- fanout-32 immutable order/rank trees for arrays; and
- an optional immutable coherent projection bundle.

All structural row identities derive from a canonical, domain-separated binary
encoding that includes the row kind, format version, authorization/encryption
domain, and every semantically relevant child identity and metadata. An
`insert-if-absent` collision is valid only when canonical bytes are identical.
Root/node IDs are integrity and deduplication identities, never authority.

Unchanged structural rows are reused. Applying a tail during consolidation
creates only replacement logical nodes and copied object/order-tree paths.
Numeric array positions are never durable targets: authoring resolves them
against the authoring snapshot to stable node IDs and an explicit before/after
anchor intent before the operation enters `editTail`.

## Operations and materialization

The bounded tail carries canonical typed logical operations, initially:

```text
SetScalar { target, value }
SetMember { object, key, value_root }
RemoveMember { object, key }
InsertArray { array, new_element, anchor?, side, value_root }
Delete { target }
MoveArray { target, array, anchor?, side }
```

Every operation has a deterministic operation ID and validates its node kinds,
ownership/domain, target reachability, and anchor relation against the snapshot
it declares. A read, merge strategy, interior query, or index projection MUST
materialize the root plus enough of the bounded tail to answer its request. It
must not inspect a root alone as though it were the current JSON value.

Point reads and interior predicates hydrate the requested root paths and the
tail dependencies needed to decide the result. A full JSON value materializes
the full root then applies the tail in canonical order. This adapter will expose
one shared materializer so merge and query/index lowering cannot drift.

When the tail reaches its size/operation bound, consolidation materializes it
into a new immutable root and replaces the application-row value with
`{ root: newRoot, editTail: [] }`. The prior row version and root remain an
exact historical snapshot.

## Merge boundary

The first implementation supports a strict, explicit boundary:

- identical manifests merge trivially;
- same-root manifests may merge only when their operation IDs are disjoint and
  their typed operations commute under the JSON operation algebra;
- a manifest based on an ancestor root may be rebased only through an explicit
  descendant proof and operation revalidation;
- unrelated roots, overlapping scalar/member writes, incompatible delete/move
  operations, and absent descendant proof remain a conflict rather than an
  implicit last-writer synthetic snapshot.

Consolidation is semantically a root-changing rewrite of one manifest. It must
either prove/rebase descendants or lose to/return a conflict with concurrent
tail edits; it may not silently discard them. The shared foundation's candidate
merge entry point owns how this result is represented to Jazz.

## Query projections and indices

Small strongly-consistent query projections are part of the same atomic content
candidate as `JsonContent`, either embedded in the queryable carrier if the
foundation supports interior fields or emitted by one candidate-level
projection hook. They are recomputed from `root + editTail`; Jazz must never
independently combine a manifest candidate with columns/projections from another
candidate. A declared projection's index consumes that materialized projection,
not a stale root-only row.

Large/broad JSON projection bundles are immutable rows keyed by the exact root
(and tail identity where an un-consolidated manifest must be covered). They are
optional derived/eventual indices. Queries using one state their eventual
contract and validate its manifest identity before returning a result. They are
not a substitute for synchronous content candidates.

## Adapter proof plan

The implementation PR must add black-box/public-surface receipts covering:

1. A row version directly captures `{ root, editTail }`; a later edit leaves
   the earlier version materializing to its exact former JSON.
2. A scalar/member/array-tail edit is visible to full reads, interior reads,
   declared merge strategies, and declared synchronous query/index projections
   before consolidation.
3. Array numeric authoring positions become stable target/anchor intent; a
   concurrent insertion does not retarget an already-authored operation.
4. Consolidation preserves the materialized value, reuses unchanged structural
   node IDs, copies only the changed fanout-32 paths, and preserves history.
5. Same-root disjoint tails merge; every non-commuting or root-descendant case
   is either explicitly proved/rebased or explicitly conflicts. Planted broken
   commutativity/descendant checks must make these tests fail.
6. A synchronous projection cannot be paired with a manifest from another
   candidate; its index/query equals a full materialization oracle.
7. An eventual broad projection is rejected/withheld when its manifest identity
   does not match the queried content.

The old `ordinary_json_experiment.rs` and JSON profiling receipts are evidence
for the chosen persistent-tree direction, not production behavior or tests to
copy. In particular, their mutable `tree_documents.root_id` and separately
updated `json_path_values` model is superseded by the atomic embedded manifest.

Tooling friction: the existing receipt uses ordinary `Uuid` IDs and independent
projection rows, so it is useful for rough cost evidence but cannot be promoted
piecemeal; the new public content carrier needs a small purpose-built black-box
fixture.

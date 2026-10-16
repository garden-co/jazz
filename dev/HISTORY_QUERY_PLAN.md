# Querying history tables

Status: proposed implementation plan. This document describes intended behavior,
not an API that has shipped. Rust core comes first; TypeScript exposes the same
contract afterward. Examples use illustrative SQL to describe query semantics.

## What we want to support

Every application table `tasks` has a read-only virtual relation `history.tasks`.
It is addressable by the ordinary query API wherever a supported table source
is accepted. `tasks` continues to mean current rows. No public current/history
source-selector enum or separate history query API is required.

The initial implementation supports filtering, projection, ordering, and live
subscriptions for one row's history. User queries joining history to current
tables or other history relations are deferred and receive explicit
unsupported-pattern errors. History relations expose retained content versions,
not deletion/restore events or a transaction audit log. They enumerate versions
rather than reconstructing a point-in-time snapshot.

```sql
SELECT h.id, h.$transactionId, h.status
FROM history.tasks AS h
WHERE h.id = :task_id
ORDER BY h.$seq
```

This returns each eligible version of one task. Two versions with identical
application values remain distinct results. A newly settled content version
adds a result; losing current read permission retracts the row's history.

### Initial Rust boundary

- Ordinary prepared queries over locally retained history on any node;
  subscriptions follow in a later implementation step.
- Exact branch selection using existing branch bindings; no implicit union of
  all branches or synthesized history of a composed branch view.
- Accepted, settled content images. Pending authored patches are not exposed as
  complete historical images, and rejected transactions are excluded. Requests
  for pending/local-history semantics fail explicitly in this initial scope.
- No user-authored joins involving history, including relationship traversals
  or includes that would introduce another source. Internal policy joins remain
  necessary and use the ordinary authorization machinery.
- Existing history access paths only; no new persistent history indexes.
- History is read-only: insert, update, delete, schema declaration, and policy
  declaration against a history relation are rejected.

History contains only settled versions: an offline edit does not
appear in this relation until accepted and settled, even if it is already
visible in the ordinary current-row view.

History completeness is not required. Queries return the locally retained,
accepted content versions that pass current-row authorization. A partial node
may return fewer versions, and an empty result does not prove the row has no
history upstream. This increment does not fetch missing history automatically.
Existing authorization requirements still apply: this does not enable raw
history access through client-local reads without authorized history coverage.

## Names and virtual catalogue entries

Resolve `history.<table>` through a virtual query catalogue entry derived from
the application's table descriptor. Keep the table name string in the public
query AST. Internally, resolution still needs to distinguish the virtual
relation's storage, identity, and policy behavior from its underlying table.

Reserve the `history.` prefix for virtual relations. Do not assume dots are
already forbidden in application table names. Check all schema admission paths
and existing catalogue compatibility before enforcing the reservation. A real
table with a conflicting name must produce a clear admission/compatibility
error, never silently resolve to a different relation. `history.history.tasks`
does not recursively create another history relation.

Audit qualified-column parsing, aliases, relationship validation, schema
lookup, query normalization, and source fingerprints. `history.tasks AS h`
must allow unambiguous `h.id` references. Current and history occurrences must
not share a compiled source/cache identity merely because they use the same
underlying application table. Both normalize into the existing row-set algebra.

Virtual descriptors are derived, not persisted as new application tables or
included as extra user schema declarations. Physical storage names remain
private implementation details.

## Rows, metadata, and identity

Keep `id` as the underlying logical row ID. History adds two non-null,
read-only metadata columns, following the existing `$createdAt` / `$updatedBy`
convention:

| Column           | Rust type       | TypeScript type                | Meaning                                           |
| ---------------- | --------------- | ------------------------------ | ------------------------------------------------- |
| `$transactionId` | Existing `TxId` | Existing branded `TxId` string | Transaction that produced this version.           |
| `$seq`           | `u64`           | `bigint`                       | Accepted sequence used to order settled versions. |

Within one table and exact branch, `(id, $transactionId)` identifies a content
version; do not mint another version UUID. Internal keys retain table identity
and branch scope.

`$transactionId` supports equality filters using the existing portable
transaction encoding: full time plus originating node UUID, never a node-local
transaction-node alias. It identifies the version's transaction, independently
of field-level `$updatedBy` provenance, which can name an earlier writer.

`$seq` supports numeric filters and ordering. It denotes acceptance order, not
wall-clock time; gaps are allowed. It does not replace transaction identity.
Preserve the full `u64` range in TypeScript using `bigint`, without conversion
through a JavaScript `number`.

Reserve these metadata names consistently with existing magic columns. Reject
conflicting application declarations rather than shadowing them. Both fields
are non-null because this relation contains only accepted, settled versions.

Generalize result identity plumbing where it currently assumes one result per
row UUID. Even without joins, two versions of the same row need distinct result
keys. Carry the composite version key through projection,
deduplication, materialization, ordering tie-breakers, pagination, and
subscription deltas, even when users do not select metadata. Aggregation and
explicit distinct operations retain their ordinary relational semantics.

Project historical values into the selected read schema using existing schema
lenses. Do not substitute the current row's values. Missing projection support
produces a typed error. Raw original-schema inspection is not part of this API.

## Authorization and content eligibility

Use the underlying current table's read policies. Compute authorized current
row identities under the caller's identity and branch binding, then semijoin
history on that row coordinate. Do not evaluate today's policy directly against
an old ownership field: an old owner value must not confer access today.

This is the agreed authorization contract: evaluate current policies against
current row values, not separately against each historical image. For example,
with an owner-only read policy, transferring a task from Alice to Bob gives Bob
access to its retained eligible history, including versions owned by Alice.
Alice loses access unless another current policy grants it. A live history
subscription must reflect that transfer by retracting Alice's results and
making the eligible versions available to Bob.

Apply authorization before user filtering and output. Deferring user joins does
not remove the internal semijoin with authorized current row identities. Track policy dependencies in the normal maintained
query graph so permission changes retract results. Policy proof sources remain
the normal current sources; resolving a history relation must not recursively
reinterpret its policy sources as history.

The current checkout has deletion state within the stored image and retains
some older separate-layer documentation. Implement content eligibility from
the active write representation, including mixed content/deletion writes;
do not assume every physical history record is a content edit, or classify an
edit solely by the image's current deletion value. Pin these cases before
enabling the public source. Any missing persisted discriminator requires an
explicit storage compatibility design, not a heuristic.

### Deleted-row history: deferred scope

Access to history after deleting a row is outside the initial implementation.
A row hidden by current deletion therefore has no visible history through this
relation. Restoration can make its eligible content history visible again.
An explicit deleted-row-history mode is rejected as unsupported.

This scope limit is distinct from excluding deletion events: ordinary history
queries omit deletion/restore-only entries regardless. It does not establish
the eventual authorization contract for deleted-row history. That contract
would need to define an authorized deleted-row preimage or equivalent policy
evaluation before this scope can expand.

## Source preparation and access planning

Integrate a history source into the existing `SourceGraphPreparer` path and
lower it through ordinary Groove relational operators. Reuse physical history
projection descriptors, but do not simply expose `subscribe_history` as a
user-facing query: that raw storage subscription does not supply this policy,
eligibility, completeness, or result-identity contract.

As inspected on 2026-10-15, history's primary key is:

```text
(branch_key, row_uuid, seq, tx_time, tx_node_id)
```

There is no history secondary index in this checkout. Transaction-to-row access
uses `jazz_tx_touched_rows`. This supersedes the earlier layout with a `by_tx`
secondary index. Do not add a new index as part of this implementation.

Push exact branch and row constraints into primary-key prefix/range reads before
hydrating history. Apply remaining predicates to that bounded row history.
Sequence bounds should narrow the scan when their key position permits it.
The initial admission rules are:

| Shape                                                          | Initial admission rule                                                         |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| One row's history, optionally filtered and ordered             | Admit using an exact branch/row prefix, with sequence bounds where applicable. |
| Any user join involving history, even through indexed keys     | Explicitly unsupported in the first slice.                                     |
| Current-table selection expanded into multiple rows' histories | Explicitly unsupported in the first slice.                                     |
| History filtered only by an unindexed application field        | Reject if it requires scanning unrelated history.                              |
| Small limit over otherwise unbounded history                   | Reject; a limit alone does not establish a bounded access path.                |

These rules apply to both reads and live subscriptions. A `$transactionId`
equality filter can narrow an admitted row-history query; it does not by itself
promise a table-wide transaction-history access path.

History access cost may grow with the selected row's retained history, not
unrelated table history. The same rule applies to initial subscription
hydration and subsequent maintenance. Do not create a whole-table resident
history arrangement as a fallback. Unsupported patterns receive typed errors
naming the specific missing access pattern.

## Subscription behavior and completeness

Use the same prepared relational program for reads and subscriptions. Maintain
dependencies on the selected row's history and current policy proofs. Publish
changes at normal transaction boundaries without exposing a partially updated
authorization state.

Handle both additions and removals: retention/truncation, permission changes,
and current deletion can retract results. New eligible versions must be
observable even when they were not present at subscription attachment. Track
current-row and policy dependencies through the ordinary authorization path;
do not hydrate unrelated history.

Local availability is the initial history-query contract; do not reject a
query solely because the node is not history-complete. Keep the source identity
and output contract usable by normal sync, including future subscriptions that
receive additional versions. Do not bypass normal authorization or sync.

The eventual remote contract must prove coverage for the requested history
range, deliver version-addressed results, track policy revocation, and allow
reconnect/replacement without collapsing versions by row ID. Initial Rust
support does not claim to implement this coverage protocol.

## Implementation sequence

1. **Virtual catalogue and validation.** Reserve/check naming, derive history
   descriptors, validate aliases/metadata, and preserve history names in
   normalized source identities. Reject writes and unsupported modes.
2. **Identity and source preparation.** Carry row-plus-transaction identity
   through result materialization; prepare accepted content images with schema
   projection and current-row authorization. Implement bounded history access
   in the regular planner.
3. **Rust reads.** Expose history names through the ordinary prepared
   query APIs. Prepare a live physical-history source bounded by the selected
   branch and row primary-key prefix, semijoined with the ordinary current-row
   authorization graph. Filter settled content images in the graph, and resolve
   transaction authors and portable node UUIDs through keyed metadata lookups.
   Missing authored-column metadata fails evaluation rather than guessing content
   eligibility. The shared pipeline owns filtering, ordering, slicing, projection,
   and materialization; no eager history load or separate permission query is
   executed. Programs can be cached because their sources remain live.
   Source resolution declares one `SourceOccurrenceKey`: a current row's UUID
   or a history row's UUID plus transaction time and portable node UUID. Result
   schemas carry these complete keys in source order, and maintained decoding
   produces `SourceOccurrenceId` values before composing the output identity.
   Logical row IDs for policies and storage lookups are obtained from the key.
   Graph lowering expands the key into scalar fields for grouping, projection
   and pagination tie-breakers, even when metadata is not selected; Groove's
   ordering operators cannot sort tuple-valued fields. Its ordered key is
   `UUID(row), U64(transaction time), UUID(originating node)` using Groove's
   existing typed key encoding. Maintained decoding reconstructs the complete
   result identity from that key, independently of the public payload.
   Equal projected values therefore retain separate version keys. Ordinary
   one-shot order-key retention still applies, but history no longer forces
   all metadata into the payload to preserve multiplicity.
4. **Rust subscriptions.** Version-aware identity, binding-codec support,
   collector lowering, and maintained result decoding are implemented. Next
   enable the public subscription entry point and its history source facts.
   Reuse the same program, with incremental history, current-row authorization
   and policy dependencies over locally available history. Subscriptions remain
   disabled until that path is complete.
5. **TypeScript surface.** Carry the same virtual names through query builders,
   validation, adapters, WASM/NAPI/native codecs, and bindings. Infer history
   application columns plus `$transactionId: TxId` and `$seq: bigint`, preserve
   composite result identities in client collections, and reject mutations at type and runtime boundaries.
   Describe results as locally available history until normal sync can prove
   complete coverage.

### Follow-up: fetch complete history when needed

When a query or subscription needs history beyond the locally retained subset,
fetch the requested row/range from the upstream database through normal sync.
Track coverage so callers can distinguish locally available results from a
complete response, preserve version identities, and propagate current-policy
revocations. Do not interpret an empty local result as proof of complete history.

### Primary code boundaries

- `crates/jazz/layers/model/src/query/`: ordinary source names, validation,
  canonical query representation, and relation syntax.
- `crates/jazz/layers/engine/src/query_engine/`: normalized source identities,
  source requirements, lowering, access planning, and output identity.
- `crates/jazz/layers/node/src/node/query_eval/read_sources.rs`: source
  preparation and policy integration.
- `crates/jazz/layers/node/src/node/physical/`: history schemas/projections and
  physical access metadata.
- `crates/jazz/layers/node/src/node/currency.rs`: existing row-history and
  transaction-to-row access primitives.
- `crates/jazz/layers/db/src/db/`: public prepared reads and subscriptions.
- `packages/jazz-tools/src/runtime/` and runtime bindings: TypeScript query
  adaptation, codecs, result decoding, and subscription identity.

## Compatibility and validation

All results use one ResultKey encoding, with an optional root transaction:
`2 | root:16 | joined_count:u32be | joined:16* | arm_count:u32be |
(position:u32be | label_length:u32be | label:utf8)* | has_transaction:u8 |
[tx_time:u64be | tx_node:16]`.
Current rows, joins, and unions set `has_transaction` to 0. History roots set it
to 1 and carry the full packed HLC and originating-node UUID; `$seq` and
projected values do not participate. History joins and unions remain unsupported.
The shared `result-key.json` fixture pins a history key including the full
`u64` range; the binding golden fixture covers current rows and union results.

Result keys are ephemeral binding values, not persisted data or active peer
messages. Rust and TypeScript migrate together to this one format, rejecting
previous encodings. No compatibility decoder or database migration is needed.
Stored row/history bytes and the active peer source-closure protocol are
unchanged. Direct `OutputOccurrenceId` serialization delegates to `ResultKey`,
including inside removed rows and internal result members. The old lossy
`(root, joined)` encoding and redundant `TypedRow` member variant are removed;
there is no compatibility reader for those retired representations. Binding-codec
support alone does not enable history queries in the TypeScript API or history
subscriptions.

No new persistent history index or storage rewrite is intended. If content
eligibility requires a storage change, resolve that explicitly before release.
Check reserved-name compatibility against existing catalogues as well.

Read `crates/jazz/TESTING_GUIDELINES.md` before implementing Rust tests. Cover:

- Two versions of one row survive projection and subscription updates, including
  equal values;
  identity remains stable across restart and different node-local aliases.
- Metadata filters and ordering preserve transaction identity and sequence values
  above JavaScript’s safe-integer range through codecs and TypeScript decoding.
- Branch scope prevents cross-branch identity and authorization collisions.
- Old ownership values do not grant access; current ownership and inherited
  policy changes retract or restore all eligible historical matches.
- Content edits, deletion-only writes, restoration, mixed writes, rejected
  writes, and pending patches obey the defined visibility contract.
- Current permission changes and deletion retract the affected history; history
  additions/removals agree between fresh reads and subscriptions.
- User joins involving history fail explicitly, including history/current,
  history/history, relationship traversals, and includes.
- Schema-renamed fields use the selected read schema or fail explicitly.
- Indexed row/sequence restrictions avoid decoding unrelated history, for
  both cold reads and subscription maintenance; unsupported scans fail early.
- Partial nodes return locally retained settled versions, including an empty
  subset, while preserving current-row authorization; current queries are unchanged.
- Naming collisions, aliases, writes, metadata collisions, ordering ties,
  pagination, and any changed encoded identities have regression coverage.

Use targeted correctness and bounded-work tests, not an end-to-end performance
claim. Existing tests that encode conflicting decisions require discussion
before their expected behavior is changed.

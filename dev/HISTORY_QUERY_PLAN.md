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
ORDER BY h.$acceptedAt
```

This returns each eligible version of one task. Two versions with identical
application values remain distinct results. A newly settled content version
adds a result; losing current read permission retracts the row's history.

### Initial Rust boundary

- Ordinary prepared queries over locally retained history on any node.
  Trusted local subscriptions use `Db::subscribe_for_identity` with
  `ReadTier::LocalOnly`. Client-local reads expose locally retained accepted history without evaluating
  policies. Peer history delivery remains deferred.
- An exact `id` equality constraint, additional application-field predicates, and
  ordering by application fields or `$acceptedAt`. Branched tables remain rejected.
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
accepted content versions. Trusted serving reads apply current-row authorization. A partial node
may return fewer versions, and an empty result does not prove the row has no
history upstream. This increment does not fetch missing history automatically.
Client-local reads use the same policy-free local visibility as ordinary tables;
they may query retained history without requesting upstream history coverage.

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

| Column           | Rust type       | TypeScript type | Meaning                                                        |
| ---------------- | --------------- | --------------- | -------------------------------------------------------------- |
| `$transactionId` | Existing `TxId` | `TxId` string   | Transaction that produced this version.                        |
| `$acceptedAt`    | `u64`           | `Date`          | Authority acceptance time; core ordering retains the full HLC. |

Within one table and exact branch, `(id, $transactionId)` identifies a content
version; do not mint another version UUID. Internal keys retain table identity
and branch scope.

`$transactionId` retains the portable transaction identity: full time plus
originating node UUID, never a node-local transaction-node alias. TypeScript
exposes the same branded `TxId` string returned by write handles. The shared
NAPI/WASM row publication boundary derives it with
`TransactionId::from_committed_tx`; core values and occurrence keys retain the
full `(time, node)` identity. This changes only the host-facing value and its
existing descriptor type, not persisted records or peer protocol encodings.
User-provided `$transactionId` predicate literals and parameters are rejected. It identifies the version's transaction, independently
of field-level `$updatedBy` provenance, which can name an earlier writer.

History exposes all ordinary provenance magic columns: `$createdAt`, `$updatedAt`,
`$createdBy`, `$updatedBy`, and their account/identity subfields. They describe each
historical version, using the same projection, predicate, ordering, and TypeScript
value conventions as ordinary rows. Default and wildcard projections omit provenance
unless explicitly selected alongside the wildcard. Updating the current row does
not overwrite older versions' provenance.

`$acceptedAt` is the authority's acceptance HLC, not the authored transaction
time. The core retains its full `u64` physical/logical value for query processing,
including ordering and pagination. TypeScript Date predicates use whole-millisecond
semantics: equality covers every logical counter in that millisecond; `gt` and
`lt` exclude that millisecond, while `gte` and `lte` include it. The adapter
translates these predicates into exact HLC bounds before execution.

At the shared NAPI/WASM output boundary, convert the HLC to physical Unix
milliseconds. TypeScript exposes a `Date`, deliberately discarding the logical
counter. Distinct versions can display the same date while retaining an exact
query order and distinct occurrence keys. This is an output conversion only;
physical storage fields and peer encodings keep their existing `seq` names and
full HLC values.

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

For trusted serving reads, use the underlying current table's read policies. Compute authorized current
row identities under the caller's identity and branch binding, then semijoin
history on that row coordinate. Do not evaluate today's policy directly against
an old ownership field: an old owner value must not confer access today.

For trusted serving reads, the authorization contract is: evaluate current policies against
current row values, not separately against each historical image. For example,
with an owner-only read policy, transferring a task from Alice to Bob gives Bob
access to its retained eligible history, including versions owned by Alice.
Alice loses access unless another current policy grants it. A live history
subscription must reflect that transfer by retracting Alice's results and
making the eligible versions available to Bob.

Client-local reads do not evaluate policies. They retain the current-row visibility
gate (including deletion) without a policy proof.

For trusted serving reads, apply authorization before user filtering and output. Deferring user joins does
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

These rules apply to both reads and live subscriptions. `$transactionId` remains
selectable, but predicates with user-provided transaction IDs are rejected.

History access cost may grow with the selected row's retained history, not
unrelated table history. The same rule applies to initial subscription
hydration and subsequent maintenance. Do not create a whole-table resident
history arrangement as a fallback. Unsupported patterns receive typed errors
naming the specific missing access pattern.

## Subscription behavior and completeness

Trusted local subscriptions are supported through `Db::subscribe_for_identity`
with `ReadTier::LocalOnly`. The Rust `JazzClient` exposes this through a
session-scoped backend client (`for_session`) and its existing local query and
subscription options. Both query results and deltas preserve version-aware
`ResultKey`s, including when metadata is omitted from the projection. They use
the same prepared relational program as reads, including on non-history-complete
nodes. `Db::subscribe` supports client-local retained history with
`ReadTier::LocalOnly`, without evaluating read policies. Peer history subscriptions
remain unsupported. Local reads do not claim complete history coverage.

Maintain dependencies on the selected row's history and current policy proofs. Publish
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
4. **Rust subscriptions.** Trusted local subscriptions are implemented using
   the ordinary maintained program. History sources supply version and settlement
   metadata for its fact terminals; virtual table names resolve to their base
   physical identities. Embedded authorization programs contribute their refresh
   dependencies, including permission tables. Typed occurrence keys survive
   facade validation. The normal write-ingress path drains local subscriptions
   even when the write was applied eagerly without a deferred publication receipt.
   The Rust client decodes virtual history columns using the shared history
   descriptor. Its existing value carriers represent `$acceptedAt` as a `u64` in
   `Value::Timestamp` and `$transactionId` as a `Value::Row` containing the
   packed time (`Value::Timestamp`) and node UUID. These existing carriers preserve
   the exact unsigned values; the packed transaction time is not converted to
   Unix milliseconds. `JazzClient::restore` supports preserving or replacing
   content, session authorization and transaction staging. Jazz-testkit uses the
   existing `JazzServer`/`JazzClient` harness to exercise writes, settlement,
   queries and subscriptions for equal projections, pagination, partial replicas,
   deletion/restoration and permission revocation/restoration.
   Authority-covered peer delivery remains deferred.
5. **TypeScript surface.** `app.tasks.history` produces an ordinary query
   against `history.tasks`, retaining the original application schema for
   admission. Query-only descriptor resolution adds the two metadata columns;
   no virtual table is deployed or persisted. The read-only handle supports one
   exact `id` constraint, application-field and `$acceptedAt` predicates, projection,
   application-field or `$acceptedAt` ordering, offset and limit. Application
   columns retain their types/transforms; `$transactionId` is
   `TxId` and `$acceptedAt` is `Date`.
   Existing occurrence keys preserve multiple versions in subscription collections.
   Both `db.all` and `db.subscribe` read locally retained history using the
   normal local read path. Clients do not evaluate policies; trusted backend/session
   reads continue to apply current-row permissions.
   Remote history reads are rejected until authority-covered history delivery
   exists. Mutation targets are rejected at type and runtime boundaries.

   ```ts
   const history = app.tasks.history.where({ id: taskId }).orderBy("$acceptedAt", "desc").limit(20);
   const versions = await db.all(history);
   const stop = db.subscribe(history.select("title"), (rows) => {
     // Multiple versions may have the same id and title.
   });
   ```

   Results contain only locally retained accepted content versions, and may be
   incomplete. An empty result is not proof of empty upstream history.

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
to 1 and carry the full packed HLC and originating-node UUID; `$acceptedAt` and
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
support alone does not enable the TypeScript history-query API or peer history
delivery. Trusted local Rust history subscriptions use the same encoded keys.

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

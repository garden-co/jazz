# Querying history tables

Status: bounded history queries and subscriptions are implemented in Rust
and TypeScript, including upstream fetching for remote and local-first reads.
Rust also supports flat inner joins and application-column aggregates.
Further TypeScript query-operation parity is deferred.

## Goal and current coverage

`HistoryQuery` should support the same read operations as `Query`, including
combinations supported for regular tables. History is a read-only virtual table,
not a separate query execution pipeline. Each history source must still be
bounded by row IDs, supplied explicitly or by another query input.

Every application table `tasks` has a virtual `history.tasks` relation, exposed
in TypeScript as `app.tasks.history`. It enumerates accepted content versions;
it does not reconstruct point-in-time snapshots or expose a transaction audit log.

| Operation                                          | Regular tables, typed TS API     | History tables, typed TS API             |
| -------------------------------------------------- | -------------------------------- | ---------------------------------------- |
| Filtering, column projection, ordering, pagination | Supported                        | Supported with bounded IDs               |
| Local queries and subscriptions                    | Supported                        | Supported over locally retained versions |
| Includes / nested relationships                    | `.include()`                     | Deferred pending semantics               |
| Required includes                                  | `.requireIncludes()`             | Deferred with includes                   |
| Deleted-row visibility                             | `.includeDeleted()` (reads only) | Supported (reads only)                   |
| Relationship traversal                             | `.hopTo()`                       | Missing                                  |
| Recursive traversal                                | `.gather()`                      | Missing                                  |
| Unions                                             | `app.union()`                    | Missing                                  |
| Large-value page/range selection                   | Object-form `.select(...)`       | Missing                                  |
| Arbitrary flat joins                               | No dedicated typed builder       | No dedicated typed builder               |
| Aggregates / grouping                              | No typed builder                 | No typed builder                         |
| Remote reads / subscriptions                       | Supported                        | Supported with bounded IDs               |
| Branched tables                                    | Supported                        | Deferred                                 |

Parity means matching actual regular-query capabilities, not promising every
possible composition. For example, the current regular-table union builder only
supports use with `gather`, and `gather` has restrictions on includes.

Rust currently supports count, sum, average, minimum, maximum, and grouping over
application columns, plus flat inner joins with history on either or both sides.
Aggregates over history metadata remain unsupported. These Rust capabilities do
not imply that corresponding typed TS builders exist.

```ts
const history = app.tasks.history.where({ id: taskId }).orderBy("$acceptedAt", "desc").limit(20);
const versions = await db.all(history);
const stop = db.subscribe(history.select("title"), (rows) => {
  // Different versions may have identical IDs and projected values.
});
```

## Remaining operation-parity work

Implement each increment through the ordinary planner, result materialization,
and subscription machinery. Share builder typing and behavior with regular
queries wherever possible, retaining history's read-only mutation boundary.
Adding methods to `HistoryQuery` alone is not sufficient: each operation needs
correct runtime behavior, inferred result types, and live maintenance wherever
regular queries support subscriptions.

1. **`include` and `requireIncludes` — deferred pending semantics.** Do not
   implement these yet. Resolve current versus historical targets, cardinality,
   and reference-version semantics first. The eventual goal is to enable nested
   relationships involving history, preserving each parent and child
   occurrence's version identity.
   Reuse regular null/missing-reference behavior, nested filters, projections,
   ordering, limits, and required-include semantics. Define how callers choose
   current versus historical related rows and which reference values drive the
   relationship before implementing the surface.
2. **`hopTo`.** Traverse relationships from history through the ordinary hop
   pipeline, preserving multiplicity and version identities. Extend bounded
   lookup planning to accept keys from intermediate joined/traversed sources;
   it currently requires the driving input's root source.
3. **`gather`.** Support history in recursive seeds and steps, keeping every
   history expansion bounded by input IDs. Define version-aware visited/deduplication
   semantics and preserve regular depth and composition rules.
4. **`union`.** Accept history query arms wherever regular query arms are
   supported. Preserve arm identity and source-version identity together, with
   correct result typing for supported current/history combinations. Reuse the
   regular union contract rather than introducing history-specific set semantics.
5. **Large-value selection.** Support the same object-form page/range projections
   and result types as regular queries. Read the selected historical version's
   value rather than the current value, using the existing large-value machinery.

For relationships and recursion, an explicit root ID is not sufficient by itself:
**every historical source** must have a bounded access path. Reject unsupported
shapes precisely instead of loading unrelated history.

## Data and identity contract

The `history.` prefix is reserved. Virtual descriptors derive from application
tables and are never deployed or persisted as additional user tables. History
relations cannot be mutation targets or receive separate policy declarations.

`id` remains the logical row UUID. History adds two non-null metadata columns:

| Column           | Core representation                        | TypeScript value               | Meaning                            |
| ---------------- | ------------------------------------------ | ------------------------------ | ---------------------------------- |
| `$transactionId` | Transaction time and originating node UUID | Existing branded `TxId` string | Transaction producing this version |
| `$acceptedAt`    | Full `u64` HLC                             | `Date`                         | Authority acceptance time          |

Within a table and branch, `(id, $transactionId)` identifies a content version.
Use portable node UUIDs, not node-local aliases. Public transaction strings use
the same derivation as write handles. User-provided `$transactionId` predicates
and ordering by that column remain unsupported.

History exposes all `ProvenanceMagicColumns`, including author subfields, with
ordinary projection/filter/order conventions. These values describe each
historical version. Default and wildcard projections omit provenance unless
explicitly selected.

Core processing retains the full `$acceptedAt` HLC for ordering and pagination.
TS output converts it to Unix milliseconds and a `Date`. Date-filter equality
covers every logical counter in that millisecond; `gt` and `lt` exclude that
millisecond, while `gte` and `lte` include it. Distinct versions may display the
same date without losing their distinct identity or exact core ordering.

Complete source occurrence identities survive projection, joins, pagination,
and subscription deltas even when metadata is omitted. Hidden historical join
identities carry `(row UUID, transaction time, node UUID)` together; current
identities carry a UUID. `OutputOccurrenceId` / `ResultKey` compose these source
identities. Equal projected values must not collapse different versions.

## Visibility, permissions, and completeness

Only accepted, settled content versions are exposed. Pending and rejected writes
are excluded, as are deletion/restore-only events. A restore that supplies new
content can produce a content version.

By default, current deletion hides a row's history; restoration reveals eligible
retained versions again. `includeDeleted()` exposes retained content history even when the row is
currently deleted. It reuses ordinary deleted-row authorization against the
retained current-row image; it does not change content eligibility. Live
`includeDeleted` subscriptions remain unsupported for both regular and history
tables.

Trusted serving reads evaluate the current table's policies against current row
values, then restrict history to authorized row IDs. Old ownership values do not
grant access. Current-row and permission dependencies remain in the maintained
query graph so revocation retracts results. Client-local reads do not evaluate
policies and use ordinary local current-row visibility.

Local-first queries return retained history immediately (or wait according to
`firstLoadRemoteWaitMs`) and request missing history through normal upstream
query coverage. Subscriptions incorporate fetched and newly accepted versions.
An immediate local result can be incomplete, including empty.

Remote reads wait for an authority-confirmed result. Their local execution uses
only the exact versions admitted into that query's upstream scope, even when the
client retains additional history. The authority applies current-row permissions;
revocation retracts remote and local-first subscription results once coverage is
updated. A new immediate local-first read can still return retained history;
revocation does not erase previously downloaded versions from the client.

The existing supporting-row protocol transfers immutable version bodies and
settles empty results. Reconnection uses ordinary supporting-set snapshots; the
current-row watermark shortcut does not apply to historical membership.
Supporting membership includes the transaction identity so several versions of
one row can coexist. Wire v6 adds `SupportingRow.is_current`: current sources
consume only admitted current winners, while history consumes all admitted
content versions. A scope may mix current and historical occurrences of the same
physical table. Shared versions are sent once; a current-winner declaration
subsumes historical support until that current contribution is withdrawn.

Schema lenses project historical versions into the requested deployed schema.
Branched-table history and snapshot-cut reads remain unsupported. These are execution gaps separate from the
operation-parity work above.

## Access planning and compatibility

History sources use the ordinary `SourceGraphPreparer` and relational pipeline.
`SourceAccessPath::Lookup` carries lookup fields and a live key relation; source
adapters determine which lookups their storage supports. The history adapter
currently accepts row IDs, using the existing primary key:

```text
(branch_key, row_uuid, seq, tx_time, tx_node_id)
```

Explicit `id` equality / `IN` filters and live join inputs supply bounded keys.
Empty ID lists require no history reads; duplicate IDs do not duplicate versions.
Other predicates apply within the selected histories. Cost may grow with selected
rows' retained history, never unrelated history. A small limit does not justify
an unbounded scan. No new persistent history index is introduced.

Stored row/history bytes are unchanged. The peer source-closure protocol is
the planned wire v6 break; its explicit current-winner flag updates that
unreleased encoding in place, without a further version bump.
See [the compatibility contract](proofs/history-current-support-wire-compatibility.md). Internal result payloads and descriptors are transient local data;
peer publication excludes these outputs. Result keys are ephemeral host-binding
values, not persisted identities or active peer messages. Their byte contract is
covered by the shared result-key and binding fixtures. Future durable encoding
changes require explicit versioning, byte fixtures, and a compatibility design.

## Validation

Prefer small public API integration tests that each prove one behavior:
`packages/jazz-tools/tests/ts-dsl/query-api.history.test.ts` for typed TS queries,
and jazz-testkit scenarios for Rust query/subscription behavior. Use normal
hosted clients and servers rather than manually ticking the database.

For each newly supported operation, verify result types and runtime behavior,
then exercise mutations through a live subscription and compare with fresh
reads. Cover equal projected versions, reference/membership changes, ordering
and pagination, and current visibility/permission changes as relevant. Verify
that nested historical values and large values come from the selected version.
Test supported compositions against ordinary-query semantics and assert exact
errors for unsupported patterns. Preserve bounded access in both initial reads
and subscription maintenance.

Read `crates/jazz/TESTING_GUIDELINES.md` before adding Rust tests. Existing tests
encode decisions: changes to those expectations require discussion.

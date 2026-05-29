# Queries And Observed Facts

## 15. Queries And Observed Facts

Queries are relational plans that produce semantic rows and observed facts.

A query plan contains:

- SQL or relational IR
- bindings
- row decoder
- include decoder
- visibility/branch plan
- policy plan
- observed-fact collector
- expected index information when relevant

Includes follow ordinary relational semantics:

- required includes lower to inner joins
- optional includes lower to left joins

If a required include is missing or unauthorized, the parent row is filtered out.
If an optional include is missing or unauthorized, the parent row remains and
the include is null.

Required-include edge cases follow current product intent:

- a nullable scalar ref that is explicitly null does not filter the parent
- a non-null scalar ref whose target row is missing or unauthorized filters the
  parent
- a forward array include with missing required members filters the parent
- a reverse relation include with no matching children does not filter the
  parent merely because the result array is empty
- nested required includes apply within their nested scope
- pagination is applied after required-include filtering

Hop/gather queries are first-class relational IR. The engine must be able to
express and lower traversals across scalar refs, UUID-array refs, and multiple
relation hops, and subscriptions must react when any FK path changes. Gather
queries are relation traversal roots rather than simple includes; current
product constraints such as gather not combining with include should be
preserved or intentionally replaced by a more general relational query model.

Optional missing includes must produce absence facts. A receiver cannot
reproduce an optional-null result from row locators alone. Absence facts are
standing query descriptors while the corresponding subscription/sync session is
active: if the absent row later materializes in the same branch/view context,
refresh should deliver it; if a previously delivered optional include is later
deleted or hidden, refresh should repair the semantic include back to null
without removing the parent row.

Observed fact kinds include:

- result row
- dependency/include row
- absence
- predicate
- range
- policy dependency
- page boundary
- branch/source
- catalogue/schema/lens

Each observed fact records:

- kind
- table/schema identity
- branch view or source context
- row locator or normalized predicate/range
- observed visible transaction/version when applicable
- reason

Observed facts may repeat with different reasons. Sync bundles dedupe concrete
rows/transactions later.

Predicate/range/absence facts must compare by normalized expression, normalized
bound values, table/schema identity, and branch/source context. The exact normal
form is open; until then only planner-supported predicate forms are stable.
Planner-supported predicate forms currently include equality, text contains,
`IN`, `!=`, `!= null` as present optional value, selected semantic system-field
predicates, ordered page descriptors, absence descriptors, hop/gather relation
descriptors, and recursive ref descriptors.

Supported indexable query forms should lower to the embedded database on current
projections. Falling back to full visible-row scans is optimization debt and
should be named as such. The current correctness baseline allows slower
fallback paths for historical pinned-base branch snapshots, arbitrary historical
snapshots, and other query-time visibility cases where no derived projection has
been promoted yet.

For the same logical dataset, branch scope, visibility policy, and query,
result membership and ordering must not depend on storage-local identifiers such
as row numbers, transaction row numbers, insertion order, or SQLite allocation
order. Query ordering uses semantic values and public ids. For example, ordering
by a ref field sorts by the public referenced row id, not by the local numeric
row id used to store the ref. Pagination uses the same logical ordering, with a
stable public tie-breaker.

Query-scoped sync must include enough repair information for a receiver that
previously synced the same scope to remove stale rows. Exporting only the
current result rows is insufficient. If a row previously matched `done = false`
and now has `done = true`, the refresh must send the row's new visible version.
If the row was deleted, the refresh must send the tombstone. This is ordinary
history, not an authoritative result snapshot.
If a row leaves a predicate and later re-enters with newer content, stale
leave-scope refreshes must not regress a receiver that already applied the
newer re-entry refresh. Refresh application is history/fate ordered; the latest
visible semantic row wins after rerun.
Refreshing one active descriptor repairs that descriptor's semantic result; it
does not eagerly evict unrelated rows learned through other active or historical
descriptors. Cache eviction is a separate asynchronous policy.

The v0 prototype repair strategy for equality predicates is:

1. collect current result rows
2. also collect rows whose local history ever matched the equality predicate
3. export current/history versions for those repair rows
4. attach a predicate observed fact carrying table, field, value, and branch id
5. dedupe concrete history records before encoding the bundle

This strategy is correct enough for the prototype, but may over-export. A
production implementation should use active downstream query descriptors,
predicate indexes, or both so repair candidates can be bounded by actual active
interest, not only by local "ever matched" history.

Query descriptors are the sync/resubscribe unit. They are active session state
owned by the downstream runtime and replayed to upstream peers after reconnect
or upstream restart. Queries should not be persisted to disk as durable user
data; ordinary app clients resubscribe after app restart, and durable cache
tiers/edges learn active interest by downstream replay. Data received for a
query may remain cached after it leaves that query's active result set. Evicting
uninteresting cached data is an asynchronous cache-management concern, not
eager query-scope contraction.
For paginated and windowed query descriptors, the observed fact must also carry
the row ids that were previously published for that descriptor. Refresh uses
those ids as repair candidates so rows that left the page can be removed, while
the current support query supplies replacement boundary rows. This is semantic
repair metadata, not an embedded durable result snapshot.
Prototype note: removing durable query descriptors by changing storage alone is
not correct. If retained local facts remain after restart but the active query
descriptor/result scope is forgotten, a later resubscribe with an empty current
result cannot distinguish stale cached facts from the authoritative current
result. The replacement needs an explicit resubscribe/query-settlement protocol
that separates retained cache state from active query truth.

When an upstream peer refreshes active query descriptors for one downstream
peer, it should plan compatible descriptors together before assembling bundles.
Compatibility is descriptor-family specific, but generally means same branch
view, table, field/path, operator, ordering, limit, include shape, and policy
context, with only bound values/root ids differing. The implementation may still
evaluate each descriptor separately internally, but bundle assembly should dedupe
shared history, read-set facts, transaction metadata, branch/source records, and
policy dependencies before encoding. Descriptor families proven batchable in the
prototype include ordinary predicates, ordered pages, and recursive ref roots.
Duplicate overlapping refreshes for different descriptors that include the same
row must dedupe concrete history and transaction records while preserving one
logical descriptor per active query.

Example: querying open todos includes:

- todo rows that matched `done = false`
- project rows included in the semantic result
- project/member rows needed by policy
- a predicate fact for `done = false`
- ordering/page-boundary facts for `$createdAt`
- the catalogue revision used to decode rows

Open issues:

- relation inference from schema metadata
- compact predicate/range closure
- page-boundary fact shape
- active query-descriptor replay protocol across reconnects and upstream
  restarts
- cache eviction policy for data no longer covered by active query descriptors
- efficient repair candidate discovery for rows that leave predicate/range
  scopes

### 15.1 Aggregates

Aggregates are queries, not a separate data model. They must lower through the
same semantic query pipeline as row queries:

- schema-version and lens compatibility
- branch/snapshot visibility
- read policy filtering
- observed facts and sync scope
- subscription rerun/diff behavior

The first aggregate primitive to prove should be `COUNT`. The important
question is not whether SQLite can execute `COUNT`, but whether Jazz can use
SQLite's native aggregation while preserving versioned table layouts, lenses,
policy filters, and query-scoped sync.

The conservative correctness baseline is local recomputation from synced
contributing facts. An aggregate scope may therefore need to sync the
contributing row facts needed to recompute the aggregate under the receiver's
policy and branch context. Aggregate result previews in sync metadata may be
explored later as an optimization, but they are not a correctness substitute
until their staleness, policy, and reconciliation semantics are specified.

A good proof example is an account-aggregator-shaped query over several tables,
policies, and schema versions. It should validate:

- native SQLite aggregate lowering
- policy-filtered contributing rows
- schema-version/lens unions
- aggregate query descriptors
- aggregate subscription refresh and diffs
- sync scope size and repair behavior when contributors enter or leave scope

Open issues:

- compact aggregate observed-fact representation
- whether aggregate-only queries can ever avoid syncing all contributing facts
- aggregate result preview semantics
- efficient subscription diffs for large aggregate scopes
- performance of aggregates over multi-version/lensed table unions

# Sync And Subscriptions

## 16. Sync Bundles

Sync is query-scoped. It is not table replication.

Given query scope, a sender exports enough data for a receiver with compatible
catalogue and policy context to reproduce the query locally.

Bundles contain:

- transaction records
- transaction outcomes and durability receipts
- branch view/source metadata
- history rows
- observed facts needed for reproduction/invalidation
- catalogue entries when needed
- file/blob metadata and bytes when in scope and authorized

The current prototype bundle shape is:

```text
branches: branch id, base global epoch, source branch ids
txs: tx id, node id, local epoch, global epoch, conflict mode, outcome,
     rejection code, receipt tiers, creation time, optional forwarded
     authenticated user for pending exclusive validation
reads: transaction row-read facts, currently scoped to exported transaction ids
query_reads: active query descriptors with branch/table/operator/field/value
             plus ordering/window/absence/recursive-ref metadata when needed
history: row versions with branch id, tx id, op, values, and system metadata
```

This is a prototype wire shape, not the final encoding. It captures the product
boundary that matters: public ids and semantic facts cross the wire; physical
ids do not.

Bundles use public ids on the wire. Incoming sync hydrates public ids into local
physical ids before touching hot tables.

Bundles are not authoritative result snapshots. Receivers apply history,
outcome, receipts, branch metadata, and catalogue data, then run queries
locally.

Scope contraction is part of query-scoped sync. When a refreshed query scope no
longer contains a row that the receiver may currently show for that scope, the
bundle must carry enough facts/history to make a local rerun remove it. This can
happen because of updates, deletes, transaction outcome changes, branch source
changes, policy dependency changes, or catalogue/lens changes.

Scope contraction removes the row from that query's semantic result. It does not
require eager deletion of the row from the receiver's local store if another
future local query may use it. Local devices and edges are local-first caches:
they may retain previously learned rows outside active scopes until an
asynchronous eviction policy decides the data is no longer useful or permitted
to keep.

Bundle assembly must dedupe concrete history rows and transaction records even
when the same row is included for multiple reasons: result, dependency, policy,
repair, snapshot base, and branch provenance.

Transport compression should operate over the connection or stream, not over
individual bundles, rows, or payload cells. The sync layer should preserve
self-describing bundle frames while allowing the transport to compress across a
larger redundancy window.

Table-scope and query-scope exports have different obligations. Table-scope
exports include table tombstones needed to converge table replicas. Query-scope
exports include only rows/facts needed by the query, its policy dependencies,
and its repair obligations; they should avoid unrelated tombstone leakage.

Branch-scoped sync carries several provenance classes:

- active branch metadata
- source branch metadata and history needed for source candidates
- pinned main-base snapshot history
- branch-local overlay history and tombstones

If a receiver lacks required catalogue state, it should wait or fail closed. The
query-scoped bundle is not the primary discovery mechanism for an app's
catalogue graph.

Open issues:

- compact reconnect summaries
- exact bundle encoding
- whether future policy dependencies can use opaque proofs
- how much negative/repair information should be represented explicitly versus
  as ordinary history for repair rows
- read-set sync for predicate/range/absence facts; current row read-set sync is
  scoped to transactions whose history is exported
- cache eviction policy and authorization revalidation for retained
  out-of-scope data

## 17. Subscriptions

One-shot queries and live subscriptions share query semantics.

A subscription is a long-lived query interest that keeps previous semantic rows
and observed facts so later changes can be delivered as semantic diffs.

The baseline implementation reruns the query and diffs full semantic rows.
Projection-diff effects may be used as an internal scheduling/invalidation
artifact, but subscription callbacks expose semantic row diffs.

Subscription state includes:

- query plan or query AST
- previous ordered semantic rows
- dependency payloads for included rows
- previous observed facts/scope
- invalidation metadata

Diff categories:

- all
- added
- updated
- moved
- removed

`moved` is a semantic diff for order-only changes: the row remains visible with
the same semantic value but changes position in the ordered result. This matters
for ordered pages and subscriptions whose user-visible state is the sequence,
not only the set of rows.

Row diffs identify the semantic row by public id, describe the change kind, and
carry the row's deterministic position in the newly delivered result for added,
updated, and moved rows, or in the previous delivered result for removed rows.
Added and updated diffs carry the new semantic row. Moved diffs do not carry a
row payload because the semantic row is unchanged. At the JavaScript wire
boundary, moved diffs use the existing `updated` row-change kind without a row
payload (`kind: 2`, `id`, `index`) so existing subscription managers can reorder
without learning a fourth wire kind. Removed diffs do not need to carry the old
row unless a higher-level binding chooses to retain it for ergonomics.

Diff ordering is deterministic and follows the corresponding query's effective
semantic order. The product contract should promise deterministic semantic
diffs, but should avoid freezing incidental internal variant choices beyond the
observable categories. For example, an order-only change should be representable
as `moved`, while the exact internal scheduling path that discovered it remains
an implementation detail.

Tiered delivery:

- `tier: "local"` may publish local durable state plus local optimistic
  mergeable transactions
- `tier: "edge"` waits until the connected edge has settled contributing state
- `tier: "global"` waits until contributing state is globally settled

One-shot queries with a requested tier wait for the same settled condition as
the first subscription delivery at that tier.

Every subscription update is tier-gated, not only the first result.

Prototype API note: the current Rust spike exposes explicit local/edge/global
read tiers for main-branch current table reads, built queries, table
subscriptions, and built-query subscriptions. This is the first reusable
lowering for #947-style semantics: local reads include accepted local
optimistic state, edge reads require an edge receipt or global epoch, and
global reads require a global epoch. Branch snapshot reads should reuse the same
tier predicate rather than adding bespoke SQL.

Subscription latency measurements should include local rerun/diff or
poll/diff work. The product-observed update path ends when the subscription can
publish semantic diffs, not when incoming history has merely been applied.

A query settled signal means: for this query, branch view, catalogue revision,
policy context, and durability tier, the runtime has applied the row history,
transaction outcomes, durability receipts, branch metadata, catalogue metadata,
and policy facts required to publish the current semantic result.

Tiered query settlement is a delivery barrier separate from row delivery. Rows,
outcomes, or metadata may arrive before the barrier is satisfied; they must not
be published as the settled result for that query/tier until the barrier is
met. First subscription delivery waits for the requested tier's settled result,
and later updates are also gated by that tier.

Rows may arrive before a query is settled. Missing catalogue or sync state that
may still arrive should keep the query unsettled rather than immediately error.
It becomes an error after timeout, cancellation, or irrecoverable failure.

Invalidation may start coarse but must be correct. Useful invalidation facts:

- result/dependency row overlap
- predicate/range overlap
- branch/source changes
- transaction outcome/receipt changes
- catalogue/lens activation changes
- policy dependency changes
- old/new order keys for ordered pages
- column masks for projection/predicate precision

Row-id cursors alone are insufficient for ordered-page invalidation because a
row outside the page may move inside the page when its order key changes.

## 18. Incoming Sync Application

Incoming sync application is semantic, not insert-only.

It should:

1. hydrate public ids to physical ids
2. upsert transaction records
3. upsert outcomes and durability receipts
4. upsert branch/source metadata
5. insert missing history rows
6. insert or update catalogue state when present
7. repair or invalidate affected projections
8. produce projection-diff effects
9. rerun/diff affected subscriptions

Raw history insertion and application-visible effects are different facts. A
received history row may be old, rejected, hidden by branch visibility, or
non-changing for the current projection.

Duplicate incoming sync application must be idempotent.

Incoming transaction fate is merged monotonically. A stale pending or accepted
bundle must not downgrade a rejected transaction; a stale pending bundle must
not downgrade an accepted/global transaction; late global metadata enriches the
same transaction rather than replacing it.
This monotonic merge applies to global epochs, edge/global receipts, and
rejection detail. A repeated or stale bundle may add information, but must not
lower a transaction's global epoch, remove receipt tiers, erase rejection
detail, resurrect rejected rows, or publish duplicate rejection events to a
subscription baseline.

The prototype authority path currently applies an untrusted bundle, validates
pending transactions, rejects invalid ones, and repairs projection. Tests cover
important pollution cases, but the desired production shape is staging
validation before publishing proposal rows into application-visible current
projection.

Receivers are not allowed to trust the sender's query result as final. They
apply transaction/history/fate/fact data, repair or rebuild projections, and
rerun the query locally. Predicate observed facts may be used to repair stale
scope-local projection rows, but correctness still comes from local query
execution.

Downstream runtimes replay active query descriptors to upstream peers after
disconnects and upstream restarts. This replay should trickle upward through
workers, edges, and global services. Queries are not durable disk state; app
restart normally recreates them by resubscribing from application code.
Until the resubscribe/query-settlement protocol is explicit, durable
intermediaries may keep implementation-local descriptor state as a correctness
scaffold. That state must not become product data: active interest is derived
from downstream replay, and retained rows outside active query results are cache
state.

Descriptor replay must distinguish retained local facts from the authoritative
current result of a reissued query. A durable cache may retain rows learned from
old scopes, but after reconnect or resubscribe the newly settled query result is
defined by the current descriptor replay and repair bundle, not by every cached
row that happens to still exist locally. This is why removing persisted
descriptor state is a protocol change, not a storage-layout-only cleanup.

Open issues:

- affected-row discovery should become narrower than broad projection repair,
  but broad repair is acceptable as a correctness baseline
- in-memory receiver-side storage for active query descriptors and scope
  contraction
- whether incoming predicate facts should directly mutate current projection or
  only schedule rerun/repair work
- staged apply/validate/publish pipeline for untrusted authority intake

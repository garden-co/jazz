# Decision Log

## 2026-05-25 23:56 PDT

Started the continuous overnight implementation/discovery pass.

Goal: make executable progress rather than abstract answers. Priorities:

- add whole-system tests that expose missing Jazz parity and distributed invariants;
- implement enough generic runtime behavior to make the tests meaningful;
- improve architecture where the current mini crate is getting in its own way;
- keep timestamped notes from `date` before new entries;
- use subagents for parallel test/architecture scouting while local work stays on the critical path.

## 2026-05-25 23:58 PDT

Target stop time is 2026-05-26 06:00 PDT. Do not stop before then, including at
good stopping points. Treat green tests/commits as checkpoints; then pick the
next highest-leverage topic and continue.

First executable discovery target: generic update semantics and rejection repair.
Red tests show `update_row` currently requires full-row payloads, which is not
the desired Jazz-like patch behavior and also prevents rejection repair from
restoring a previous visible version cleanly.

## 2026-05-25 23:59 PDT

First slice green: generic updates now merge omitted fields from the current
visible row, and rejecting an update rebuilds projection so the previous visible
version reappears instead of disappearing. This makes transaction/read-set
tests closer to TS API semantics (`undefined`/omitted fields are no-ops).

Next target: query-scope resync when a row still matches the direct predicate
but becomes hidden through a policy dependency. Both scouts independently
flagged this as a likely gap in `export_query_where_eq` / query repair.

## 2026-05-26 00:05 PDT

Checkpoint: `cargo test -p mini-jazz-sqlite` is green with 123 tests.

Implemented:

- generic updates are patch-style and preserve omitted fields;
- patch updates choose their base from the effective visible row, including
  pinned branch base snapshots;
- rejecting an update rebuilds projection, restoring the previous visible
  version or pinned base instead of leaving a hole;
- added query-scope tests for policy-dependency changes and branch-local repair
  isolation.

Design lesson: write lowering needs an explicit "effective base row" concept.
That base is not always the row in the checked-out branch current projection:
it may be the sparse-overlay inherited main row, or a pinned historical snapshot.
This is a good candidate for an architecture cleanup boundary rather than more
ad hoc helpers inside `runtime.rs`.

## 2026-05-26 00:07 PDT

Second slice green: added recursive query-scope repair for reparenting. Prior
coverage handled descendant deletion tombstones, but not rows that left the
recursive scope because an edge changed. The red test showed peers retained
`child` and `grandchild` after `child.parent` moved to another root.

Implementation is intentionally conservative: export history for rows that
historically participated in the recursive scope so the receiver can learn the
edge moved. This is likely over-broad for large trees, but it gives a correct
baseline and identifies a future optimization target: durable recursive
predicate/read-set state rather than ad hoc historical-tree export.

Architecture cleanup started: extracted effective row value lookup into
`effective.rs`. This gives a name and boundary to the logic that chooses between
checked-out branch current rows, sparse-overlay inherited current rows, and
pinned historical snapshots. It is still small, but it is the right direction:
write lowering can ask for an effective base row instead of rediscovering branch
snapshot semantics inside the mutation path.

## 2026-05-26 00:11 PDT

Durable-worker/browser-tab topology slice is green as part of the full mini
crate suite: 125 tests pass. Added a test where an in-memory tab writes data,
syncs a query scope into a durable file-backed worker, the worker process is
reopened, and a fresh empty in-memory tab rehydrates from that worker.

Design lesson: the worker/tab topology does not require a special in-memory
runtime path. A memory node can start empty, and a durable SQLite node can be
the trusted upstream that replays the current query scope. This directly
supports the spec direction that all nodes use SQLite, with durability/topology
as configuration rather than a different semantic engine.

## 2026-05-26 00:13 PDT

Added two more distributed fate/order tests; full mini crate suite is green
with 127 tests.

- A stale in-memory phone can reconnect to a durable worker after the worker
  has rejected the optimistic transaction. Reapplying the phone's old pending
  bundle must not resurrect the row or erase the rejection reason.
- A globally accepted mergeable update at an older epoch cannot override a
  newer exclusive transaction. The current projection can still use global
  epoch order as the visible-state rule; exclusivity is a validation/admission
  property, not a special read-time precedence rule.

Design lesson: fate monotonicity and global ordering are doing useful work
across reconnects. The next stress area is less "which accepted tx wins" and
more "which facts must be present for an edge/global authority to safely decide
acceptance or rejection after a disconnect."

## 2026-05-26 00:15 PDT

Policy-dependency validation slice found and fixed a real gap. A red test
showed that an untrusted bundle containing a child write and its required
parent fact was still rejected by the edge. Root cause: table-history export
included dependencies needed to read the exported rows, but not dependencies
needed to validate their write policies.

Implemented write-policy dependency export for `RefReadable` policies. The
paired tests now cover both sides:

- if the bundle includes the parent policy fact, the edge can validate the
  incoming write without prior state;
- if the bundle is deliberately stripped of the parent, the edge rejects
  permanently, and later syncing the parent does not reopen the rejected tx.

Design lesson: sync scopes need two dependency lanes: result/read visibility
dependencies and authority-validation dependencies. They can share mechanics,
but the caller's intent matters.

## 2026-05-26 00:19 PDT

Extended the write-policy dependency finding to recursive policy chains. A
trusted edge can now receive one untrusted bundle containing a todo write, its
project parent, and the org required by the project's read policy, then validate
the write successfully. Full mini crate suite is green with 130 tests.

Explorer follow-up suggested high-value remaining parity tests: query-scope
refresh after rejection, fate-before-history message ordering, subscription
diffs on rejection, optional include absence/null semantics, same-epoch
same-row tie-break determinism, branch global acceptance visibility, and a
small edge/core/edge topology. Next focus: message-order and query-scope repair
tests because they are likely to expose sync-contract holes quickly.

## 2026-05-26 00:20 PDT

Message-order/query-scope cluster is green; full mini crate suite now has 133
tests.

- Query-scope refresh after rejection removes a row that was previously
  delivered by the same query scope, while preserving the rejection reason.
- Rejected fate can arrive before history: later history append remains
  invisible.
- Accepted/global fate can arrive before history: later history append
  materializes the row with the same public tx id and global receipt metadata.

Design lesson: tx/fate rows are a good durable landing zone independent of
history delivery order. This supports treating sync as idempotent fact
application rather than ordered messages, at least for the basic
accepted/rejected cases.

## 2026-05-26 00:22 PDT

Added three more product/distributed invariant tests; full mini crate suite is
green with 136 tests.

- Subscriptions emit `Removed` when the visible transaction for a row is
  rejected, and the subscription snapshot converges to one-shot reads.
- Same global epoch, same row, opposite apply order converges across peers and
  survives projection rebuild. Current tie-break is stable physical tx ordering
  after public tx import, which is acceptable for now but should eventually be
  specified in public terms.
- Global acceptance of a branch-local transaction does not make it visible on
  main after sync/rebuild. Acceptance means admitted/durable, not visible in
  every branch.

Design lesson: current projection rebuild is proving a good oracle. When a
test asserts both online apply and clear/rebuild, it quickly catches whether
we encoded the semantics in durable history/metadata or only in incidental
current-table mutation order.

## 2026-05-26 00:23 PDT

Added two more status-quo/product-shape tests; full mini crate suite is green
with 138 tests.

- Optional scalar include absence in the fixture query round-trips as `None`
  through query-scope sync, then becomes `Some(title)` after the referenced
  project arrives.
- Edge/core/edge topology works in miniature: client -> trusted edge with edge
  receipt -> trusted core global acceptance -> second edge -> authorized and
  unauthorized clients. The public tx id is preserved, both edge/global
  receipts survive, and policy still hides the row from Bob.

Design lesson: the current fact model already supports a credible browser edge
plus cloud-core topology. The missing pieces are transport/protocol shape and
catalogue negotiation, not a different storage/runtime semantic path.

## 2026-05-26 00:24 PDT

Added query-scope tombstone precision coverage; full mini crate suite is green
with 139 tests. The test syncs an equality query, deletes both a matching row
and an unrelated nonmatching row, then refreshes the query. The bundle includes
the matching tombstone needed to repair the peer but excludes the unrelated
tombstone.

Design lesson: query-scope repair can stay narrower than table replication even
when handling deletions. We still need richer observed facts for optional
absence/range/page scopes, but simple equality/deletion repair has a workable
shape.

## 2026-05-26 00:25 PDT

Small architecture cleanup: extracted storage statistics collection from
`runtime.rs` into `stats.rs`. Full mini crate suite remains green with 139
tests.

Design lesson: the useful module boundaries are verb/process boundaries, not
entity nouns. `stats::collect` is a tiny example; larger candidates remain
bundle application/export and write lowering. The low-risk path is to keep
Runtime as the facade and move process-shaped implementations behind it.

## 2026-05-26 00:27 PDT

Added first status-quo query-language parity slice: text `contains` lowers to
SQLite `instr`, works for generic schemas, and preserves the current Jazz
behavior that an empty substring matches all strings. Non-text `contains` fails
explicitly for now. Full mini crate suite is green with 140 tests.

Design lesson: small pieces of the high-level query DSL can lower cleanly
without building a query graph. The next question is not whether this works for
simple predicates, but how much observed-fact/scope machinery each predicate
form needs for correct sync and subscriptions.

## 2026-05-26 00:29 PDT

Extended `contains` from local query lowering into query-scope sync. Query
observed facts now carry a small predicate op (`eq` or `contains`), and refresh
repair uses the op when deleting stale current rows. Added a test where a row
previously matching a text `contains` query is updated to stop matching; the
peer's refreshed query scope removes it. Full mini crate suite is green with
141 tests.

Design lesson: observed facts should be a tiny relational predicate IR, not
just `(field, value)`. This does not need to become a query graph; it can remain
the replay/repair contract for the lowered SQL query shape.

## 2026-05-26 00:30 PDT

Found and fixed a real patch-update/write-policy bug. Red test:
`patch_update_uses_preserved_ref_for_write_policy_validation`. A todo update
that only changed `title` should preserve the existing `project` ref and pass
`write_if_ref_readable("project")`, but the outer write path rechecked policy
against the raw patch after the inner lowerer had already merged effective
values. The second check failed with `expected ref id for project`.

Fix: the outer post-insert rejection check now also computes the effective row
values for updates. Full mini crate suite is green with 142 tests.

Design lesson: patch/effective-row semantics must be centralized. The current
double-check shape is a smell; the future write-lowering module should return
or own the effective row and validation result so callers cannot accidentally
validate a different value set.

## 2026-05-26 00:31 PDT

Follow-up architecture cleanup after the patch-update bug: extracted
`effective_write_values` and used it in both the outer write path and the inner
history/current lowerer. Full mini crate suite remains green with 142 tests.

Design lesson: this is the write-path equivalent of `effective.rs`. The
eventual module should probably be organized around lowering a write into
`effective_values`, `policy_reads`, `history_row`, `current_projection_effect`,
and `fate`, rather than around table/transaction nouns.

## 2026-05-26 00:32 PDT

Added fail-closed catalogue/scope coverage; full mini crate suite is green with
144 tests.

- A bundle whose history references an unknown table errors and leaves no
  history/current projection behind.
- A bundle whose query-scope metadata references an unknown table also errors
  before partially applying the history.

Design lesson: applying a bundle inside one SQLite transaction is paying off.
Catalogue/schema gaps can be treated as unsettled/fail-closed without leaving
half-imported txs or rows that need a later scrub.

## 2026-05-26 00:33 PDT

Added `id` as the first generic magic-field query. `read_rows_where_eq(table,
"id", "...")` now filters by public row id and rejects non-string id values.
Full mini crate suite is green with 145 tests.

Design lesson: product magic fields can start as semantic query lowering, even
if the first implementation is an in-memory filter over visible rows. The spec
should distinguish product contract from eventual SQL/index lowering.

## 2026-05-26 00:34 PDT

Added `$createdBy` as a second semantic magic-field equality query. Full mini
crate suite is green with 146 tests.

Design lesson: magic fields need a small registry/planner rather than being
special-cased forever. `id` and `$createdBy` can filter over decoded visible
rows for now, but `$createdAt` and pagination/order will need SQL lowering and
observed range/page facts.

## 2026-05-26 00:35 PDT

Closed the sync half of `id` magic-field queries. `export_query_where_eq(...,
"id", ...)` can now sync the matching row and repair deletion through query
scope refresh. Full mini crate suite is green with 147 tests.

Design lesson: every product-visible predicate needs both local evaluation and
observed-fact repair semantics. Even a simple `id` predicate needed special
repair handling because it is not a user schema column.

## 2026-05-26 00:36 PDT

Closed the sync half of `$createdBy` magic-field queries as well. Query-scope
export/repair now handles `$createdBy = principal` and repairs deletes on the
peer. Full mini crate suite is green with 148 tests.

Design lesson: `id` and `$createdBy` are enough to prove the shape, but the
ad-hoc branches in query repair are accumulating. Next architecture pass should
extract a query predicate planner/evaluator that can produce local SQL,
repair SQL, and history-row expansion from one predicate description.

## 2026-05-26 00:37 PDT

Added a transaction identity interning invariant. Replicas may assign different
physical SQLite `tx_num`s to the same public `tx_id` after local writes happen
in different orders, and sync still converges by public identity. Full mini
crate suite is green with 149 tests.

Design lesson: physical ids are purely local cache keys. The public contract
has to stay on row ids, tx ids, branch ids, and semantic query results.

## 2026-05-26 00:39 PDT

Small architecture step: extracted generic SQL/value lowering for schema-column
query predicates into `query_predicate.rs`. Full mini crate suite remains green
with 149 tests.

This is only the first slice of the planner idea; `id` and `$createdBy` are
still special repair branches in `runtime.rs`. But the direction is clearer:
predicate descriptions should own their local SQL, history expansion, and
repair behavior.

## 2026-05-26 00:41 PDT

Hardened query predicate bundle serialization. Non-equality predicate operators
now have an explicit test proving they survive JSON roundtrip, and older bundle
shapes without an operator decode as equality. Full mini crate suite is green
with 150 tests.

Design lesson: predicate metadata is part of the sync contract, not just a local
planner concern. Backward-compatible defaults are useful, but only if tests make
the default precise.

## 2026-05-26 00:42 PDT

Added the first protocol-version tag to sync bundles. Exported bundles now carry
version `1`, older untagged bundle JSON decodes as version `1`, and future
bundle versions fail closed before any partial apply. Full mini crate suite is
green with 153 tests.

Design lesson: version tags are cheap to add at the boundary and valuable
because the bundle apply path already has strong atomicity expectations. This
only tags `Bundle`; storage/catalogue/worker protocol versioning still needs
separate treatment.

## 2026-05-26 00:45 PDT

Added structured rejection details to transaction fate. Rejected transaction
records now carry an optional JSON detail in storage, bundles, and
`transaction_info`. Authority-side policy rejection records a safe detail with
`reason`, `table`, and `row_id`, and that detail survives sync back to the
writer. Full mini crate suite is green with 153 tests.

Design lesson: the code/detail split feels right. Product code can branch on a
stable rejection code while trusted-debug surfaces can inspect structured detail.
The open product question is still what detail is safe to expose to untrusted
clients for more complex policies.

## 2026-05-26 00:47 PDT

Added a first storage format tag. New SQLite stores set `PRAGMA user_version` to
`1`, runtimes expose the current storage format version for tests/debugging, and
stores with a future version fail before schema installation. Full mini crate
suite is green with 155 tests.

Design lesson: SQLite gives us a nearly free coarse storage-version boundary.
This does not replace catalogue/schema/lens versioning, but it is a good guard
for physical format changes and migration entry points.

## 2026-05-26 00:48 PDT

Closed a generic write-shape bug: unknown user fields are now rejected instead
of being silently dropped by schema-column lowering. The check covers both
inserts and patch updates through the shared transaction insert path. Full mini
crate suite is green with 156 tests.

Design lesson: a generic SQLite lowering has to validate the semantic row shape
before it touches physical columns. Otherwise "helpful" projection code becomes
silent data loss.

## 2026-05-26 00:49 PDT

Locked in batch-wide rejection semantics for the unified transaction concept. A
single transaction that writes a project and two todos disappears entirely from
current reads when rejected, while all three history/write-set rows remain and
projection rebuild preserves invisibility. Full mini crate suite is green with
157 tests.

Design lesson: the current fate/projection model already had the right shape
for whole-transaction rejection. The new test is valuable because this is a core
status-quo parity point from batches that must remain true after refactors.

## 2026-05-26 00:52 PDT

Added the first `in` predicate slice: `id IN [...]` works for local reads,
query-scope export, sync, and deletion repair of a selected member. Full mini
crate suite is green with 158 tests.

Design lesson: array-valued predicate metadata fits the bundle shape cleanly.
The implementation is intentionally narrow (`id` only) because general
schema-column `IN` will need a more complete predicate planner that can produce
variable-arity SQL and repair clauses without ad-hoc branches.

## 2026-05-26 00:55 PDT

Added a narrow ordered/limited query-scope slice: newest open todos by
`j_created_at DESC LIMIT n`. A peer that had two rows receives a refresh with a
newer matching row and removes the displaced boundary row. Full mini crate suite
is green with 159 tests.

Important discovery: query-scope repair must run after applying incoming
history, not only before it. Delete repair can work before history, but page
boundary repair needs the newly arrived row to be visible before deciding which
old rows have left the result. The current implementation runs repair both
before and after history apply.

Design lesson: top-k subscriptions need explicit page-boundary semantics, not
just row membership. This slice is fixture-specific, but the invariant is broad:
scope repair depends on the post-apply result set.

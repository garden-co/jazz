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

## 2026-05-26 00:56 PDT

Locked in copy-on-write lens update semantics. A row created under an old schema
with `title` can be imported through a new schema lens exposing `name`, patched
as `name`, exported as semantic `name`, and applied to another new-schema peer
without leaking the old field name. Full mini crate suite is green with 160
tests.

Design lesson: the current storage-name/semantic-name split is doing useful
work. Lens reads are not just presentation: once a new-schema runtime patches a
row, the exported history payload follows the current semantic schema shape.

## 2026-05-26 00:59 PDT

Extended `IN` predicates from `id` to ordinary schema fields. Local reads filter
semantic row values, query-scope export records an array-valued `in` predicate,
and repair currently decomposes schema-field `IN` into repeated equality repair.
Full mini crate suite is green with 161 tests.

Discovery: when a row leaves a predicate by update, our refreshed bundle can
carry the newer out-of-scope row version as repair history. Query result
visibility is correct, but table-wide contraction semantics are still blurry:
should a scope-limited peer remove that row entirely, or retain it as a known
fact outside this particular query? Existing equality tests mostly assert query
results, not full table contraction. This needs a spec decision before making
repair more aggressive.

## 2026-05-26 01:01 PDT

Added predicate subscriptions for equality queries. `RowsSubscription` now
stores a query descriptor instead of only a table name, and polling reruns the
same semantic query before diffing. The new test covers a row entering and
leaving a `done = false` subscription. Full mini crate suite is green with 162
tests.

Design lesson: local listener semantics and sync query-scope semantics can share
the same simple rerun/diff posture for now. The abstraction should probably
become a common query descriptor instead of separate ad-hoc subscription and
bundle predicate shapes.

## 2026-05-26 01:02 PDT

Added semantic schema validation before SQLite DDL. Schemas now reject duplicate
semantic fields, duplicate physical storage fields introduced by lenses, and
indexes that reference unknown fields. Full mini crate suite is green with 163
tests.

Design lesson: treating SQLite errors as the schema validator leaks the lowering
layer too early. The core should reject incoherent Jazz schemas with Jazz-shaped
messages before physical DDL is even attempted.

## 2026-05-26 01:03 PDT

Strengthened the fate race test to include structured rejection detail.
Accepted/global metadata arriving after local rejection still does not resurrect
the transaction, and it also does not erase the rejection detail. Full mini
crate suite is green with 163 tests.

Design lesson: mutable transaction fate can still be replayable if precedence is
monotonic: accepted metadata may enrich a rejected tx, but rejected outcome and
detail remain authoritative.

## 2026-05-26 01:05 PDT

Made ref-readable policy declarations fail closed at schema install time. A
policy that points at a missing field or a scalar field now gets a Jazz-shaped
schema error instead of silently degrading to allow-all behavior. Full mini
crate suite is green with 164 tests.

Design lesson: policy syntax validation is part of policy-first behavior. Bad
policy declarations must fail loudly before any runtime can start accepting or
serving data under the wrong assumptions.

## 2026-05-26 01:36 PDT

Extended local predicate subscriptions beyond equality. Subscriptions now cover
`contains` and `in` predicates using the same rerun/diff model, with tests for
rows entering and leaving both query shapes. Full mini crate suite is green with
166 tests.

Design lesson: the subscription object is turning into a query descriptor. That
is good pressure toward unifying local subscriptions and sync query scopes
around one semantic query representation instead of parallel ad-hoc APIs.

## 2026-05-26 01:37 PDT

Added lens-backed query-scope coverage. A new-schema runtime can query an
old-schema row through the renamed semantic field, sync that query scope to a
peer, and later repair the peer when the row leaves scope through a semantic
field update. Full mini crate suite is green with 167 tests.

Design lesson: lens support is now covered across reads, writes, policies,
subscriptions indirectly through reads, and query-scoped sync. The storage-name
mapping is holding up better than expected.

## 2026-05-26 01:39 PDT

Added branch query-scope repair coverage for sparse overlays. A peer that first receives a pinned main-base row for a branch query now removes it when a branch-local overlay shadows the same row out of the predicate result. Full mini crate suite is green with 168 tests.

Design lesson: branch query scopes must be evaluated over the effective branch snapshot, not as independent main-base rows plus branch deltas. Overlay shadowing is part of query-scope contraction.

## 2026-05-26 01:42 PDT

Generalized the ordered page-boundary query-scope experiment beyond fixture todos. Generic schemas can now read and export `eq + top createdAt desc` scopes, and refresh removes a displaced boundary row when a newer matching row enters the page. Full mini crate suite is green with 169 tests.

Design lesson: ordered-page repair can be represented as a query descriptor over ordinary schema fields plus system ordering metadata. The implementation is still a narrow descriptor shape, but the invariant no longer depends on todo-specific code.

## 2026-05-26 01:44 PDT

Persisted observed query-read descriptors. Applying a query-scoped bundle now records its query read in SQLite, dedupes repeated application, survives durable reopen, and still rolls back the observation when malformed query metadata fails closed. Full mini crate suite is green with 170 tests.

Design lesson: query-scoped sync needs a durable desired/observed-query lane, not just query metadata inside transient bundles. This first table only records observations; using it to drive reconnect/resubscribe work remains a larger next slice.

## 2026-05-26 01:45 PDT

Represented optional include absence as a query-read fact. The open-todos export now emits an `absent` id read for a missing project include, receivers persist it, and later project arrival still updates the semantic row. Full mini crate suite is green with 170 tests.

Design lesson: `null` includes are not just missing data; they are observations. Even a fixture-shaped absence descriptor clarifies the future generic model: optional relationships need explicit absence facts so reconnect and invalidation can distinguish "not requested" from "requested and absent/unauthorized."

## 2026-05-26 01:47 PDT

Added ordered-page local subscription coverage. A `where eq + top createdAt desc` subscription now reports the newer row entering and the old page-boundary row leaving. Full mini crate suite is green with 171 tests.

Design lesson: local listeners and sync query scopes are converging on the same query descriptor problem. Keeping separate subscription enums and bundle query-read records is now clearly duplication; a shared semantic query descriptor should be an architecture cleanup soon.

## 2026-05-26 01:48 PDT

Started consolidating query descriptors. Local row subscriptions now store a `QueryPredicateRecord` instead of one bespoke enum variant per predicate operator, while bundle query reads keep their wire-compatible shape. Full mini crate suite is green with 171 tests.

Design lesson: the right abstraction is not a "subscription" object or a "sync query read" object; it is a semantic query descriptor with branch/snapshot context layered around it when needed. This cleanup is intentionally partial but points the implementation in that direction.

## 2026-05-26 01:51 PDT

Added schema compatibility fingerprints to bundles. Receivers fail closed before applying structurally incompatible bundles, older untagged bundles remain legacy-compatible, and index-only plus rename-lens differences still apply because the fingerprint is based on lowered storage shape rather than semantic names or policies. Full mini crate suite is green with 172 tests.

Design lesson: catalogue compatibility should be split by lane. Structural storage/lens compatibility can be checked at the sync boundary; policy heads should remain separate because peers may evaluate stricter or newer permissions over compatible row storage.

## 2026-05-26 01:52 PDT

Pinned an exclusive-conflict invariant: exclusive transactions conflict at whole-row granularity, even when the later write only touches a different column. Full mini crate suite is green with 173 tests.

Design lesson: per-column metadata is useful for mergeable conflict resolution, but exclusive/global consistency should reason over row items unless we deliberately design a narrower serializability model. This matches the recent spec clarification.

## 2026-05-26 01:58 PDT

Used persisted query reads to drive reconnect refresh. A durable worker can receive a query scope, restart, send its observed descriptors to an upstream, and apply returned refresh bundles that remove rows which left scope while offline. Full mini crate suite is green with 174 tests.

Design lesson: durable query-read storage is not just introspection. It can be the seed of a real resubscribe protocol: receiver remembers desired/observed scopes, upstream reruns descriptors, receiver applies repairs. The current API is manual and single-hop, but the core loop is now executable.

## 2026-05-26 02:00 PDT

Extended reconnect refresh to durable ordered-page descriptors. A restarted durable worker can send its persisted `eq + top createdAt desc` descriptor upstream and repair a page where a newer row displaced the old boundary while offline. Full mini crate suite is green with 175 tests.

Design lesson: the resubscribe loop is not limited to simple predicates. Ordered pagination can use the same persisted descriptor lane, provided the descriptor carries both predicate and ordering/window metadata. This strengthens the case for one query descriptor model across sync and listeners.

## 2026-05-26 02:01 PDT

Covered branch-scoped reconnect refresh from persisted query reads. A durable worker can persist a branch query, restart on that branch, send the descriptor upstream, and repair the branch view after an overlay shadows the base row out of scope. Full mini crate suite is green with 176 tests.

Design lesson: persisted query descriptors need branch context, but they do not need a separate branch-specific protocol. The same reconnect refresh loop works as long as branch provenance and checkout context are explicit.

## 2026-05-26 02:03 PDT

Added explicit forgetting for persisted query reads. A durable peer can remove an observed query descriptor, the removal survives restart, and reconnect refresh generation no longer produces work for that forgotten scope. Full mini crate suite is green with 177 tests.

Design lesson: durable query state needs lifecycle, not just persistence. This is the beginning of unsubscribe semantics: desired scopes should be durable, inspectable, refreshable, and intentionally removable.

## 2026-05-26 02:06 PDT

Covered reconnect refresh when query visibility changes through a policy dependency. A durable worker syncs an open task through a readable project, restarts, then refreshes its persisted query after the task is re-pointed to an unreadable project; the row is removed from the query result. Full mini crate suite is green with 178 tests.

Design lesson: persisted query descriptors do not need to explicitly encode every policy dependency if refresh reruns semantic export against current policy state and sends repair history. Predicate descriptors plus policy-aware export are enough for this class of reconnect repair.

## 2026-05-26 02:11 PDT

Added policy fingerprints as bundle metadata and clarified their semantics. Bundles now carry a separate policy fingerprint from structural schema compatibility; untrusted acceptance still uses the authority runtime policy, not the sender policy fingerprint. Full mini crate suite is green with 179 tests.

Design lesson: policy catalogue state is a separate lane, but sender policy metadata is not an authorization precondition for data bundles. The authority must evaluate its local trusted policy catalogue; remote policy fingerprints are useful for diagnostics/negotiation and future catalogue sync, not for replacing authority-side validation.

## 2026-05-26 02:13 PDT

Added first required-include semantics. Optional todo/project includes still return the parent with `project_title = None`, while `open_todos_require_project` filters the parent when the referenced project is missing or unauthorized and restores it when the project becomes visible. Full mini crate suite is green with 181 tests.

Design lesson: required includes are query semantics, not permission semantics. The parent row may be readable on its own, but the required include changes result membership based on target visibility. This should become a generic query-shape feature rather than a todo-specific helper.

## 2026-05-26 02:14 PDT

Extended required includes through query-scoped sync. A peer can receive a parent row whose required ref target is missing, keep it out of the required-include result, and then include it once the target row arrives later. Full mini crate suite is green with 182 tests.

Design lesson: query-scoped sync can safely deliver parent rows before required include dependencies arrive, as long as result materialization reruns include visibility. Required include semantics need not block storage of the parent history.

## 2026-05-26 02:16 PDT

Added a first ordinary conflict-meta read surface. Multi-base branch conflicts can now be surfaced as ordinary `RowView`s with `conflict_count`, instead of only through the side-channel candidate API. Full mini crate suite is green with 182 tests.

Design lesson: pure multi-base conflicts currently do not appear in plain `read_rows`; they only exist as branch-source candidates. A conflict-aware query surface can bridge that, but the current implementation is intentionally narrow. The real design likely needs conflict metadata in the normal semantic row shape, plus explicit resolution transactions.

## 2026-05-26 02:19 PDT

Added a first explicit conflict-resolution transaction. A merge branch can now write a chosen row value over conflicting branch-source candidates, expose the resolved row with `conflict_count = 0`, and preserve that result after rebuilding the current projection. Full mini crate suite is green with 183 tests.

Design lesson: branch conflict resolution can be modeled as an ordinary row write on the merge branch. The source candidates remain as provenance/history, while the current-branch value suppresses conflict metadata in the semantic read surface. This keeps resolution replayable, but it leaves richer conflict metadata and explicit "resolved from candidates X/Y" provenance for a later slice.

## 2026-05-26 02:21 PDT

Covered conflict resolution through sync. A receiving peer can import branch source provenance, the merge-branch resolution write, and then read the resolved row with no active conflict while still being able to inspect the original candidates. Full mini crate suite is green with 184 tests.

Design lesson: preserving branch sources and writing the resolution on the merge branch composes with the existing bundle protocol. We did not need a special conflict-resolution sync record for the first version, but we still need explicit candidate provenance if product UX wants to say exactly which alternatives a resolution settled.

## 2026-05-26 02:23 PDT

Generalized required-ref result filtering beyond the todo helper. `read_rows_require_ref(table, ref_field)` now filters parent rows whose referenced target is missing or not visible under that target table's read policy, and rejects non-ref fields. Full mini crate suite is green with 185 tests.

Design lesson: required includes can start as a generic query-materialization rule over visible target rows. This is still not the final typed query API, but it removes another todo-shaped assumption and gives the future lowering layer a simple semantic contract to target.

## 2026-05-26 02:25 PDT

Added restarted subscription recovery from persisted query reads. A durable worker can reconstruct a subscription from an observed query descriptor after restart, apply an upstream refresh bundle, and emit the semantic removal diff against the stale local snapshot. Full mini crate suite is green with 186 tests.

Design lesson: durable query descriptors can power both reconnect repair and listener recovery. We still do not persist listener identity/callback state, but the core mechanism now bridges stored query desire, refresh application, and semantic diff emission.

## 2026-05-26 02:25 PDT

Investigated exclusive stale-read validation as the next candidate slice and deliberately did not fake it. Current `jazz_tx_read` records `(tx, table, row, reason)` but not the observed row version/visible transaction, so it cannot prove that a read dependency changed after a transaction's base. The existing exclusive conflict checks are row-write based, not full read-set validation.

Design lesson: precise exclusive validation needs versioned read-set entries or a compact base/snapshot reference that can be resolved to observed row versions. Row identity alone is useful for sync scoping and policy dependencies, but not enough for serializable read validation.

## 2026-05-26 02:26 PDT

Pinned the missing-lens fail-closed behavior. A bundle produced by a schema that semantically renamed `title` to `name` is rejected by a peer whose schema has an unrelated physical `name` column and no lens, leaving no rows or transaction metadata partially applied. Full mini crate suite is green with 187 tests.

Design lesson: the structural compatibility fingerprint is already doing useful catalogue-boundary work: two schemas with the same public field names are not compatible if their storage/lens shapes differ. This is a good default, though a future explicit catalogue-sync lane may want better diagnostics than "incompatible schema fingerprint".

## 2026-05-26 02:28 PDT

Added a first branch metadata read surface. Runtimes can list branch ids, base global epochs, and source branch ids; the metadata survives table-history sync for a merge branch. Full mini crate suite is green with 188 tests.

Design lesson: branch provenance is already durable and syncable as system metadata, but it is not yet a user-visible backing row with ordinary permissions. The new read surface makes that gap explicit: product branch permissions should probably wrap or replace this system-only table with a policy-controlled branch catalogue.

## 2026-05-26 02:29 PDT

Extended conflict metadata through a filtered equality read. Multi-base branch candidates now keep `conflict_count` when queried through a field predicate, not only through the full-table conflict-meta read. Full mini crate suite is green with 189 tests.

Design lesson: conflict metadata needs to be part of semantic row materialization before query filtering, otherwise query APIs will accidentally hide conflict state. The current implementation is intentionally simple and reruns a full conflict-meta read before filtering; a real lowering should push predicates into SQL while preserving candidate expansion.

## 2026-05-26 02:41 PDT

Implemented the first versioned read-set slice. `jazz_tx_read` now records the observed visible transaction for row reads, bundles carry `observed_tx_id`, and untrusted exclusive transactions are rejected with `stale_read_set` when a policy dependency changed since the sender observed it. Full mini crate suite is green with 190 tests.

Design lesson: row identity read sets were enough for sync scoping, but exclusive validation really does need version identity. The first implementation validates pending exclusive bundles before acceptance using the authority's current view, then imports and rejects the stale transaction so replay/fate remains durable. It is intentionally narrow: absence reads, branch-specific base snapshots, and compact read-set encoding still need sharper treatment.

## 2026-05-26 02:43 PDT

Added a small observed-read introspection API. `transaction_observed_read_rows(tx)` exposes the row read set together with the observed transaction id, and the previous-row read-set test now pins that an update observed the earlier version. Full mini crate suite is green with 190 tests.

Design lesson: versioned read sets should be inspectable as semantic data, not only buried in bundles or SQLite columns. The tuple API is deliberately rough, but it gives tests and future spec work a stable way to assert causality/version observations.

## 2026-05-26 02:48 PDT

Tightened the versioned read-set slice after review. Storage format is now version 2 so old v1 files fail fast instead of missing the new `observed_tx_num` column at runtime. Branch transactions that read inherited pinned-base rows now record the observed base transaction. Stale-exclusive prevalidation also preserves branch base metadata when the incoming bundle introduces the branch. Full mini crate suite is green with 193 tests.

Design lesson: adding read-version causality touches three boundaries at once: storage compatibility, effective branch visibility, and sync prevalidation order. The fixes make the first implementation less misleading, but branch source overlays and absent/range reads still need their own version semantics.

## 2026-05-26 02:49 PDT

Added a first restore/undelete operation for generic rows. `restore_deleted_row(table, id)` reads the latest delete tombstone values and writes a new visible history version, surviving current-projection rebuild. Full mini crate suite is green with 194 tests.

Design lesson: undoing a delete can stay append-only: restore is not removal of the tombstone, it is a new transaction derived from the tombstone's stored values. The current API is minimal and needs policy semantics, sync coverage, and branch-base behavior before being considered product-shaped.

## 2026-05-26 02:50 PDT

Covered restore through sync. A peer that imports insert, delete, and restore history sees the restored row as current. Full mini crate suite is green with 195 tests.

Design lesson: restore-as-new-history composes with the existing bundle protocol without a special operation kind. This reinforces treating undo/restore as semantic writes over preserved history, while leaving product-level authorization and UX naming open.

## 2026-05-26 02:52 PDT

Covered restarted ordered-page subscriptions. A durable worker can persist an `eq + top createdAt desc` query, restart, reconstruct a subscription from the observed descriptor, apply a refresh where a newer row displaced the old boundary, and emit added/removed semantic diffs. Full mini crate suite is green with 196 tests.

Design lesson: the same durable descriptor now supports current-row repair and listener recovery for paginated views. The descriptor still encodes only the current page shape, not all possible future page boundaries, which matches the current scope but should remain explicit in the spec.

## 2026-05-26 02:56 PDT

Split versioned read-set mechanics into a dedicated `read_set` module. Runtime still orchestrates transaction acceptance, but observed-read recording, stale exclusive validation, and bundle prevalidation now live behind named helpers. Full mini crate suite is green with 196 tests.

Design lesson: read-set/version semantics are now a distinct subsystem rather than runtime glue. This should make the next slices around absent/range reads and branch source overlays easier to test without bloating `runtime.rs` further.

## 2026-05-26 02:58 PDT

Added transaction-scoped absent row reads for inserts. A create now records whether the target public row id was absent at write time, bundles preserve that read, and untrusted exclusive transactions are rejected with `stale_read_set` when the authority already has a visible version for that row. Full mini crate suite is green with 197 tests.

Design lesson: absent reads fit better as transaction read-set facts than as durable query subscriptions. Reusing `jazz_tx_read` with a distinct reason kept the protocol simple, but it also made clear that reason codes should become named, documented integer enums before the next spec sync.

## 2026-05-26 03:03 PDT

Extended ordinary branch reads over source branches. A merge branch now reads current rows from its explicit source branches, while a branch-local resolution row shadows source candidates for the same logical row. Full mini crate suite is green with 198 tests.

Design lesson: multi-source branch visibility is not just a conflict side API; it belongs in the normal query lowering. The precedence rule is now branch-local overlay first, then source branches, with pinned main-base handling still layered separately. This is close to the product branch-view model, but branch backing rows and branch-row permissions remain unimplemented.

## 2026-05-26 03:04 PDT

Added a first branch backing-row mirror. `jazz_branch_backing` stores branch id, base epoch, source branch ids, and created time as durable data mirrored from engine branch metadata; local create and synced branch records now keep the mirror aligned. Full mini crate suite is green with 198 tests.

Design lesson: branch-as-data can start as a mirror/read surface without replacing the execution tables. This avoids derailing branch visibility and sync while making the product shape concrete. The next hard part is policy: checkout/use should eventually be gated by the backing row's ordinary row permissions.

## 2026-05-26 03:06 PDT

Made branch base epochs immutable. Recreating a branch with the same base remains idempotent, but attempting to recreate it with a different base now fails instead of silently keeping the old metadata. Full mini crate suite is green with 199 tests.

Design lesson: branch provenance needs fail-loud invariants early. Silent `INSERT OR IGNORE` behavior is too dangerous for branch views because the row history can appear valid while the branch base/sources no longer describe the intended snapshot.

## 2026-05-26 03:08 PDT

Added a first nullable-field slice. Schemas can declare `optional_text`, explicit JSON null values round-trip through history/current/query materialization, and equality filters over null lower to `IS NULL` instead of SQL's `= NULL` trap. Full mini crate suite is green with 200 tests.

Design lesson: nullability needs to be a schema property, not a loose value convention. This small slice preserves required-field failures while proving the storage/query codec can represent SQL nulls semantically. Optional refs, defaults, `ne null`, and query-scope sync for null predicates remain open.

## 2026-05-26 03:10 PDT

Added optional references. Schemas can declare `optional_ref`, rows can store an explicit null ref, null ref equality uses the existing `IS NULL` lowering, and `read_rows_require_ref` skips null refs while keeping linked rows. Full mini crate suite is green with 201 tests.

Design lesson: once nullability is carried on `FieldDef`, refs compose cleanly with the existing storage and semantic row codec. The harder remaining ref work is not basic null storage; it is include/query-scope behavior for null and missing refs, plus policy decisions around nullable ref-readable policies.

## 2026-05-26 03:12 PDT

Added a first `ne` predicate API. `read_rows_where_ne` now supports local semantic filtering over ordinary fields and magic fields, and `ne null` behaves as status quo expects: it returns rows with present optional values. Full mini crate suite is green with 202 tests.

Design lesson: the product semantics are easy to state, but this slice is intentionally not yet SQL-lowered or sync-scoped. Before making `ne` part of `QueryReadRecord`, we should decide the operator contract for null, refs, and index/range read-set capture together.

## 2026-05-26 03:13 PDT

Added declared scalar defaults for inserts. Schemas can declare `text_default` and `bool_default`; omitted insert fields are filled before policy checks, history writes, current projection writes, sync export, and rebuild. Full mini crate suite is green with 203 tests.

Design lesson: defaults belong in the effective write-value phase rather than at SQLite DDL level for now. That keeps defaults semantic and replayable across history, but default metadata should probably participate in schema/catalogue compatibility once the schema version story is less skeletal.

## 2026-05-26 03:15 PDT

Added whole-system coverage for initially empty query scopes. An equality query that matches no rows still syncs its durable query-read descriptor; a later refresh sends a newly inserted matching row without sending unrelated non-matching rows. Full mini crate suite is green with 204 tests.

Design lesson: query descriptors already behave like desired-state subscriptions, not just repair hints for rows that were previously delivered. This is an important local-first invariant and should be made explicit in the spec's sync/subscription section.

## 2026-05-26 03:16 PDT

Added same-principal multi-node coverage. Two runtimes with the same principal but different node ids each start their own local epoch sequence, produce distinct public transaction ids, sync into one peer, and preserve `j_created_by = alice` for both rows. Full mini crate suite is green with 205 tests.

Design lesson: node identity and authorization principal are separate axes in the prototype already. This matters for browser/device topologies: causality and local epochs belong to nodes, while authorship and policy identity belong to principals.

## 2026-05-26 03:23 PDT

Added fail-closed write-policy compatibility checks for ordinary peer bundle
apply while preserving authority-side untrusted validation. Bundles now carry a
policy fingerprint scoped to the tables represented by their history/query-read
payload, and `apply_bundle` rejects mismatched non-legacy write-policy
fingerprints before any partial apply. `apply_untrusted_bundle` deliberately
skips this transport compatibility check and validates using the authority's
local policy instead.

Discovery: read policies are local visibility semantics, not an import
compatibility boundary. A node with a stricter read policy can safely accept
history and filter its current view locally, including across schema lenses.
Write policies are the dangerous peer-sync boundary because they describe which
writers were allowed to create the history. The fingerprint must also be scoped:
a `projects` bundle should not fail because `todos` has a newer write policy,
and fate-only bundles have no policy surface at all.

Validation: `cargo test -p mini-jazz-sqlite` passes with 206 whole-system tests.

## 2026-05-26 03:26 PDT

Promoted `ne` from local-only filtering to a distributed/query predicate.
`export_query_where_ne` now records durable query reads, refreshes can deliver
rows that later enter a not-equal scope, subscriptions diff rows that enter or
leave the scope, and SQLite lowering uses `IS NOT ?` so optional/null fields
behave like the local semantic filter.

Discovery: `ne null` is a compact way to express "present optional value" and
is worth supporting in the same machinery as equality/contains/in. The refresh
test also clarified the query-read topology again: the observing peer persists
the desired query descriptor, and the upstream peer exports refreshes against
that descriptor during reconnect.

Validation: `cargo test -p mini-jazz-sqlite` passes with 208 whole-system tests.

## 2026-05-26 03:28 PDT

Captured declared defaults as a replay/sync invariant rather than only a local
insert convenience. A durable peer now receives an insert whose omitted scalar
fields were filled by schema defaults, then reopens and still reads the same
defaulted values from stored history/current projection.

Discovery: the current placement of defaults in the effective write-value phase
is holding up under sync and durable rebuild. Defaults are semantic row content
once the transaction is sealed, not SQLite DDL-side generated values.

Validation: `cargo test -p mini-jazz-sqlite` passes with 209 whole-system tests.

## 2026-05-26 03:29 PDT

Added branch overlay coverage for the new `ne` query path. A branch-local row
that initially satisfies `tag != null` is delivered by query-scoped sync, then a
branch-local update to `tag = null` causes the same query scope to repair the
peer's branch projection back to empty.

Discovery: the generic query-scope machinery now handles `ne` across branch
overlays without special casing beyond the predicate lowering. This is a useful
confirmation that the branch/query integration is becoming operator-shaped
rather than equality-shaped.

Validation: `cargo test -p mini-jazz-sqlite` passes with 210 whole-system tests.

## 2026-05-26 03:31 PDT

Locked in stale row-version validation for untrusted exclusive updates. A
writer updates a row after observing version A, the authority advances the row
to version B before receiving the writer's exclusive transaction, and authority
acceptance rejects the writer transaction with `stale_read_set` while preserving
the authority's newer current row.

Discovery: row update read sets were already precise enough for this invariant;
the missing piece was coverage. Together with absent-row and policy-read stale
tests, exclusive validation now covers the three most important read-set shapes
for untrusted acceptance.

Validation: `cargo test -p mini-jazz-sqlite` passes with 211 whole-system tests.

## 2026-05-26 03:32 PDT

Added ordinary peer-sync coverage for a lens-compatible write policy. An old
schema writes `todos.project` under `write_if_ref_readable("project")`; a new
schema reads the same storage through `workspace` using `ref_lens` and
`write_if_ref_readable("workspace")`; ordinary `apply_bundle` accepts the
bundle and materializes only the new semantic field.

Discovery: storage-name-based write-policy fingerprints are doing the right
thing for rename lenses. This keeps catalogue compatibility strict enough to
fail closed on real write-policy drift while still allowing semantic field
renames that lower to the same physical policy dependency.

Validation: `cargo test -p mini-jazz-sqlite` passes with 212 whole-system tests.

## 2026-05-26 03:33 PDT

Added branch observed-query refresh coverage for source branch rows. A peer
observes an initially empty query on a merge branch with `left` and `right`
sources; the upstream later writes a matching row to `left`; reconnect refresh
from the merge branch sends the source row and preserves merge branch source
metadata on the peer.

Discovery: source-branch content changes already flow through the durable
query-read refresh loop as long as the upstream is checked out to the observed
branch. The remaining, harder case is mutating the source set of an existing
branch as a first-class operation; the current public API mostly creates branch
provenance at branch creation time.

Validation: `cargo test -p mini-jazz-sqlite` passes with 213 whole-system tests.

## 2026-05-26 03:34 PDT

Added a first public rejection-list API. `rejected_transactions()` returns
durable transaction id, code, and detail records from `jazz_tx_rejection`; the
existing policy rejection sync test now verifies that both the authority and a
downstream peer can enumerate the same rejection detail.

Discovery: transaction rejection data was already durable and synced as part of
transaction export, so a user-visible queue is mostly API shape rather than new
storage. The future callback/promise surface can be layered over this list,
with redaction semantics still deliberately unsettled.

Validation: `cargo test -p mini-jazz-sqlite` passes with 213 whole-system tests.

## 2026-05-26 03:37 PDT

Added durable observed-query support for recursive refs. `export_recursive_refs`
now records a `recursive_refs` query descriptor (`table`, parent field, root id);
reconnect refresh can re-export that recursive tree; and
`subscribe_observed_query` can turn the descriptor into a subscription that
diffs later descendants.

Discovery: recursive queries can fit the same query-read/refresh loop without
inventing a parallel subscription mechanism. The descriptor shape is still
prototype-simple (`field = parent ref`, `value = root id`), but it is enough to
prove the "recursive query as durable desired state" model. Recursive query
repair is currently handled by exporting the right tombstone/history rows rather
than by special deletion SQL in `apply_query_scope_repair`.

Validation: `cargo test -p mini-jazz-sqlite` passes with 214 whole-system tests.

## 2026-05-26 03:38 PDT

Added an explicit `add_branch_source` operation and covered query refresh after
source-set expansion. A peer observes an empty query on `merge`; upstream later
adds `left` as a source branch; reconnect refresh sends rows visible through
the new source and syncs the updated branch provenance.

Discovery: source-set mutation works naturally through the existing
`jazz_branch_source` plus backing-row mirror once there is an explicit API.
This is still not the final branch permissions model, but it gives us a concrete
verb for testing provenance changes instead of only branch creation-time
sources.

Validation: `cargo test -p mini-jazz-sqlite` passes with 215 whole-system tests.

## 2026-05-26 03:39 PDT

Added coverage that rejected branch conflict resolutions restore conflict
metadata. A merge branch first shows two source candidates, then a branch-local
resolution hides the conflict, then rejecting the resolution transaction makes
the two candidates and their `conflict_count = 2` surface again.

Discovery: this invariant already falls out of mutable transaction fate plus
projection rebuild. That is an encouraging sign for the "mutable fate on
`jazz_tx` is replayable enough" thesis: rejecting the resolver does not need a
special undo log; it just changes which history rows are eligible for current
projection.

Validation: `cargo test -p mini-jazz-sqlite` passes with 216 whole-system tests.

## 2026-05-26 03:40 PDT

Added durable restart coverage for recursive query descriptors. A worker syncs
a recursive root query, shuts down, upstream adds a descendant, the worker
reopens, reads the persisted `recursive_refs` descriptor, and uses reconnect
refresh to receive the new descendant.

Discovery: once recursive queries use the same `jazz_query_read` table as
ordinary predicates, durability and reconnect behavior comes mostly for free.
This strengthens the case that "query descriptors as desired state" can cover
both flat and recursive local-first subscriptions.

Validation: `cargo test -p mini-jazz-sqlite` passes with 217 whole-system tests.

## 2026-05-26 03:41 PDT

Did a small runtime architecture cleanup: all export paths now construct bundles
through one `make_bundle` helper, which centralizes protocol version, schema
fingerprint, and scoped policy fingerprint calculation.

Discovery: tonight's policy-fingerprint and recursive-descriptor work made it
too easy to accidentally hand-build subtly different bundles. Centralizing this
does not solve the larger runtime-module size problem, but it removes one source
of drift before adding more query/export shapes.

Validation: `cargo test -p mini-jazz-sqlite` passes with 217 whole-system tests.

## 2026-05-26 03:43 PDT

Added recursive observed-query deletion coverage. A peer subscribes from a
persisted `recursive_refs` descriptor; upstream deletes a descendant; reconnect
refresh carries the tombstone and the observed subscription emits a semantic
`Removed(child)` diff.

Discovery: recursive descriptor refresh handles both expansion and contraction
through the same bundle path. The contraction behavior depends on exporting
deleted descendant tombstones, not on generic `apply_query_scope_repair`
understanding recursive tree SQL.

Validation: `cargo test -p mini-jazz-sqlite` passes with 218 whole-system tests.

## 2026-05-26 03:44 PDT

Added branch source removal and sync contraction. `remove_branch_source` updates
the backing branch row and rebuilds projection; applying branch metadata from a
bundle now treats `source_branch_ids` as authoritative rather than only adding
new sources. Query refresh after detaching a source branch now removes rows that
were only visible through that source and syncs the empty source list.

Discovery: branch provenance cannot be merged as a grow-only set if branches can
remove sources. This is a real semantic choice: branch records are snapshots of
provenance, not deltas. The existing additive apply behavior was fine for early
creation-only tests but would leak detached source rows.

Validation: `cargo test -p mini-jazz-sqlite` passes with 219 whole-system tests.

## 2026-05-26 03:46 PDT

Added durable restart coverage for branch source removal. A durable worker syncs
a merge branch whose rows are visible through a source branch, shuts down, then
reopens after upstream detaches the source and receives a query refresh that
removes the source row and persists the empty source list.

Discovery: source-list contraction now works across the browser-worker-shaped
durable boundary too, not only in a single in-memory session. This matters
because branch provenance is system metadata and must survive exactly the same
disconnect/reconnect loop as row history.

Validation: `cargo test -p mini-jazz-sqlite` passes with 220 whole-system tests.

## 2026-05-26 03:48 PDT

Improved rejection detail for untrusted writes whose policy dependency is not
available at the authority. Instead of only reporting `write_policy_denied`, the
authority now reports `policy_dependency_unavailable` with the child row and the
missing dependency table/row id when a ref-readable write policy points at a row
that is absent from the authority's visible state.

Discovery: policy-denial UX can be layered on existing read-set/policy
information without changing transaction fate. This is still intentionally not a
complete redaction story, but it gives clients a much more actionable durable
error for the common "you did not send/sync the policy-influencing row" case.

Validation: `cargo test -p mini-jazz-sqlite` passes with 220 whole-system tests.

## 2026-05-26 03:49 PDT

Added fail-closed validation for recursive query descriptors. Applying a bundle
whose `recursive_refs` query-read names an unknown parent field now rejects
before partial apply, leaving history/current/query-read state empty.

Discovery: treating recursive descriptors as normal query reads means they also
need catalogue validation at apply time. The first implementation only checked
the table; the test caught that malformed recursive descriptors could otherwise
be persisted silently.

Validation: `cargo test -p mini-jazz-sqlite` passes with 221 whole-system tests.

## 2026-05-26 03:50 PDT

Pinned explicit-null/default semantics. Inserts with an omitted defaulted field
still receive the default, but explicitly supplied `null` on an optional field
is preserved as row content and is not treated as omission.

Discovery: the current effective-write-value implementation already has the
right shape: defaults apply by missing key, not by falsy/null value. This is a
small but important high-level API invariant for optional fields.

Validation: `cargo test -p mini-jazz-sqlite` passes with 222 whole-system tests.

## 2026-05-26 03:51 PDT

Added distributed query-scope coverage for `ne` over the `$createdBy` magic
field. A peer syncs "rows not created by alice", receives only Bob's row, then a
later update to Bob's row refreshes through the same observed query descriptor.

Discovery: the generic query-scope machinery already supports non-equality
operators over magic fields well enough for `$createdBy`. This gives more
confidence that query descriptors can stay operator-shaped across both user and
system columns.

Validation: `cargo test -p mini-jazz-sqlite` passes with 223 whole-system tests.

## 2026-05-26 03:56 PDT

Added distributed query-scope support for `id != ...`. The first red test
showed id predicates only supported equality and `in` during query repair.
`id != excluded` now exports broad matching row ids for repair and apply-side
repair can remove rows that no longer have non-deleted matching history in the
scope.

Discovery: broad predicates over magic fields need their own repair shape; they
cannot reuse equality's finite-row-id loop. This is a useful warning for future
range predicates and pagination cursors: the descriptor has to encode enough to
repair rows that leave a broad result set, not just rows named by the cursor.

Validation: `cargo test -p mini-jazz-sqlite` passes with 224 whole-system tests.

## 2026-05-26 04:17 PDT

Added deletion repair coverage for `$createdBy != ...` query scopes. The red
test showed `$createdBy` repair was equality-shaped even though initial export,
local reads, and update refreshes already handled `ne`.

Discovery: broad magic-field predicates need operator-aware repair everywhere,
not just for initial export. Apply-side repair and export-side row discovery now
lower `eq` and `ne` separately, so rows created by other principals can leave a
broad magic-field scope after deletion.

Validation: `cargo test -p mini-jazz-sqlite` passes with 225 whole-system tests.

## 2026-05-26 04:19 PDT

Added subscription coverage for an observed `$createdBy != ...` query scope
whose matching remote row is later deleted upstream and refreshed into a worker.

Discovery: after the repair-path fix, subscription polling needed no special
case. The listener layer compares semantic row snapshots returned by the query
facade, so broad magic-field predicates inherit correct remove diffs once scoped
sync converges the worker projection.

Validation: `cargo test -p mini-jazz-sqlite` passes with 226 whole-system tests.

## 2026-05-26 04:20 PDT

Added the symmetric observed-subscription test for `id != ...` query scopes.
A worker subscribes to an observed broad id predicate, receives an upstream
delete repair, and emits a semantic removal diff for the deleted included row.

Discovery: `id != ...` and `$createdBy != ...` now have the same listener-level
shape: no extra subscription machinery is needed once the broad query repair
paths are operator-aware and projection-convergent.

Validation: `cargo test -p mini-jazz-sqlite` passes with 227 whole-system tests.

## 2026-05-26 04:21 PDT

Added recursive observed-query subscription coverage for a descendant that
stays structurally reachable but becomes hidden by a recursive policy dependency
change. The upstream moves the child from Alice's readable org to Bob's
unreadable org; refresh removes it from the worker and the subscription emits a
semantic removal.

Discovery: recursive query refresh already handles this harder policy case. The
same descriptor shape that repairs deleted and reparented descendants also
exports enough state for policy-hidden descendants to disappear without adding a
new repair mechanism.

Validation: `cargo test -p mini-jazz-sqlite` passes with 228 whole-system tests.

## 2026-05-26 04:24 PDT

Added a trusted-transport/untrusted-visibility invariant. A trusted edge can
hold and export both Alice and Bob policy-protected rows, but an Alice peer that
applies that broad history still reads only Alice-visible rows from current
state.

Discovery: this cleanly separates sync/storage richness from session
visibility. The current projection may contain history from trusted transport,
but `read_rows` remains policy-filtered for ordinary principals.

Validation: `cargo test -p mini-jazz-sqlite` passes with 229 whole-system tests.

## 2026-05-26 04:26 PDT

Added observed-query subscription coverage for renamed-field lenses. A peer
stores a `name == "Important"` query-read against a schema where `name` is a
lens over old storage column `title`; after the writer updates the semantic
`name`, refresh removes the row and the subscription emits a semantic removal.

Discovery: query-read descriptors stay semantic across lens-compatible schema
versions. The stored descriptor names the new-schema field, while export and
repair correctly lower through the lens to the old physical storage column.

Validation: `cargo test -p mini-jazz-sqlite` passes with 230 whole-system tests.

## 2026-05-26 04:28 PDT

Found and fixed a stale branch-source replay bug. A peer could apply a stale
bundle that still listed `left` as a source for `merge` after a newer bundle had
removed that source, causing the source to reappear and branch results to widen
again.

Change: branch metadata now carries a monotone `source_version`, exported in
`BranchRecord` and checked on apply before replacing the source list. Local
source-list mutations bump the version. This bumped the prototype SQLite
storage format from 2 to 3 because `jazz_branch` gained a system column.

Discovery: branch provenance/source metadata is mutable state and therefore
needs replay ordering just like transaction fate. Treating branch records as
last-applied snapshots is not safe under out-of-order sync.

Validation: `cargo test -p mini-jazz-sqlite` passes with 231 whole-system tests.

## 2026-05-26 04:23 PDT

Narrowed one todo-specific query descriptor wart. The legacy
`export_query_scope_newest_open_todos` fixture method now records the generic
`eq_top_created_at_desc` query-read descriptor instead of the old hardcoded
`top_created_at_desc` op; the test pins the generic op at the observed-query
boundary.

Discovery: the todo fixture can keep existing ergonomic APIs while increasingly
lowering through the same generic query descriptor path as non-demo schemas. The
old apply-side `top_created_at_desc` repair branch is still present as
compatibility ballast for bundles emitted before this cleanup.

Validation: `cargo test -p mini-jazz-sqlite` passes with 228 whole-system tests.

## 2026-05-26 04:22 PDT

Pinned missing-policy-dependency rejection as permanent rather than parked. An
edge rejects an untrusted write whose referenced policy row is missing, later
receives that policy row, and then replays a complete bundle for the same
transaction; the transaction stays rejected with the original durable detail.

Discovery: current fate merging already gives us the simple model we wanted:
authoritative rejection is replayable durable state. If we ever want a parked
"awaiting dependencies" state, it should be a separate state machine decision,
not an accidental consequence of late dependency arrival.

Validation: `cargo test -p mini-jazz-sqlite` passes with 228 whole-system tests.

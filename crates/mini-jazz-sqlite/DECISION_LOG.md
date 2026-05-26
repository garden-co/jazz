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

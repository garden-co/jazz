# Invariant Coverage Decision Log

## 2026-05-27 13:02 PDT

Goal: broaden whole-system invariant coverage on top of the query refresh
planner / multi-value SQL PR. This PR should be mostly tests and confidence,
not new product mechanics. If a new invariant test exposes a real bug, fix it
in place and record the learning here.

Initial coverage targets:

- batched refresh should be equivalent to individual refreshes for every
  descriptor family we currently batch;
- grouping boundaries should be explicit: descriptors that differ by
  branch/table/field/operator/order/limit should not batch;
- repair semantics should hold under update/delete/rejection/policy changes;
- chunked multi-value SQL should be semantically identical to unchunked
  same-shape page refreshes;
- semantic system fields (`id`, `$createdBy`) should stay supported where
  previous query APIs already supported them;
- idempotence, restart/reconnect, branch-source, lens, and subscription
  invariants should get extra tests where existing coverage is thin.

## 2026-05-27 13:05 PDT

First slice added five query-refresh invariant tests and they pass:

- batched refreshes match individually applied refreshes for a mixed observed
  query set spanning eq/ne/contains/in/top-created-at/top-field descriptors;
- refresh planning does not batch across descriptor shape boundaries;
- top-field multi-value SQL refreshes stay correct past the 400-value chunk
  boundary;
- applying the same query-scope refresh twice is idempotent after a row leaves
  scope;
- semantic system-field page refreshes (`id`, `$createdBy`) match individual
  application.

Next widening pass: move beyond planner-level equivalence into topology,
durability, policy/fate, branches, lenses, subscriptions, and history/export
invariants.

## 2026-05-27 13:08 PDT

Second slice added five broader invariant tests and they pass:

- row-level causality/read-set recording for updates: whole-row write item,
  previous-row read, and observed tx id are all visible from public inspection
  APIs;
- rejected fate with detail repairs query-scoped current projection, is
  idempotent under replay, and syncs through the rejection APIs;
- observed query refreshes are branch-bound and fail if the source is not
  checked out to the observed branch;
- rename-lens query refresh keeps an already observed row current across schema
  versions while exposing only semantic field names;
- observed-query subscriptions emit deterministic semantic diffs after a batched
  refresh and polling is idempotent once no new state changed.

Useful discovery: branch query scopes are bound to the checked-out branch for
refresh, but rows from main can still be visible in a draft branch according to
branch visibility rules. The branch invariant is therefore "refresh with the
right branch context", not "branch rows are physically isolated from main".

## 2026-05-27 13:10 PDT

Third slice added recursive and topology invariants:

- recursive query refresh batching matches per-root refresh after both subtree
  additions and tombstones;
- a cold client can receive a query scope from core after data moved
  client -> edge -> core, then observe a later upstream change via reconnect
  refresh.

This turns the new module into a cross-section suite: planner equivalence,
chunking, query-scope repair, row-level causality, fate/rejection, branch
context, lenses, subscriptions, recursive queries, and multi-hop topology.

## 2026-05-27 13:15 PDT

User asked for roughly 10x more invariants and explicitly encouraged inventing
new ones that make sense from the spec. I am widening the invariant module with
many small, API-level whole-system tests rather than copying existing long
scenario tests.

New invariant families to try:

- observed-query lifecycle: repeated descriptors dedupe, forgotten descriptors
  stop refresh, branch mismatches fail closed;
- query semantics: duplicate IN values, null/not-null, empty refreshes, stable
  page boundaries;
- replay/idempotence: stale bundles must not undo newer fate, repeated sync
  must not grow current projection or duplicate receipts;
- durability: durable reopen keeps current projection and query-derived state
  coherent without relying on test-only rebuild;
- topology/auth: trusted edge/core/client paths keep user attribution and
  policy results stable;
- branches: source metadata changes are idempotent and cannot leak unrelated
  branch rows through observed refresh;
- lenses/defaults: semantic rows remain stable across schema versions and
  omitted/default fields survive sync/rebuild.

## 2026-05-27 13:20 PDT

The invariant module now has 48 passing tests. Added 36 tests beyond the first
PR slice, covering:

- observed query lifecycle: dedupe, forget, empty refresh, deterministic
  descriptor ordering, subscribe branch checks;
- query semantics: duplicate `IN` values, `!= null`, case-sensitive contains,
  magic `id`, magic `$createdBy`, unknown table/field failure without observed
  interest;
- replay/fate: bundle replay idempotence, edge+global acceptance monotonicity,
  rejection beats stale pending replay, accepted fate can arrive before history;
- projection/durability: rebuild equivalence after mixed fate, durable current
  projection after reopen, durable rejection metadata after reopen;
- transaction modes: local exclusive fails without writing history, mergeable
  same-row updates can follow each other, omitted update fields are preserved,
  invisible deletes fail without transaction creation;
- branches: unknown checkout failure does not switch branch, base epoch
  mismatch is idempotent, direct source cycles fail without partial source
  changes, backing rows match branch API, source removal repairs query results;
- auth/policy/topology: trusted attribution bypass preserves created_by,
  untrusted policy failure is atomic for a multi-row transaction;
- subscriptions: ordered page order-only changes produce deterministic semantic
  diffs and rejection detail is reported once.

New/clarified candidate invariants discovered while writing tests:

- ordered subscription diffs should promise semantic determinism, but the exact
  diff variant (`Moved` vs `Updated`/remove/add combination) should remain an
  implementation detail unless the product API standardizes it;
- partial untrusted exports that cannot validate policy may be rejected
  atomically rather than marked awaiting-dependency if the transaction is
  malformed/insufficient as an export, while genuine cache misses with coherent
  read/write context can await deps;
- magic fields (`id`, `$createdBy`) have query semantics independent of user
  columns with the same surface names;
- query export failures must not create durable observed interest rows;
- accepting the same transaction at edge and then global is monotonic and
  idempotent for receipts/current projection.

## 2026-05-27 13:37 PDT

Stepping back over the full spec suggests these weak/nonexistent invariant
areas are most important for a stable iteration foundation:

1. Settlement / tier gating: distinguish row received, query settled, and safe
   to publish, especially while history, fates, catalogue, and policy deps
   arrive out of order.
2. Crash/reconnect state machines: crash after history before fate, fate before
   projection repair, observed-query replay before refresh, local write before
   upstream sync.
3. Authority validation over predicate/range read sets: row, absence, and
   policy deps are covered better than predicate/range validation.
4. No-op and normalization transaction semantics: empty explicit transactions
   create no tx; multiple staged same-row mutations normalize to final state.
5. Upsert semantics: create-or-update behavior across mergeable/exclusive and
   multi-tier sync is still underspecified and under-tested.
6. Catalogue revision semantics: unknown catalogue, permission-only changes,
   index-only compatibility, missing permissions, and catalogue as its own lane.
7. Policy privacy / non-leakage: external errors should not reveal hidden row
   existence.
8. Branch permissions and backing rows: branch handles/checkouts/exports should
   depend on visible/authorized backing rows, not only metadata presence.
9. Conflict metadata shape and resolution intent: resolved value plus candidate
   tx ids plus explicit resolution semantics need stronger product-shaped tests.
10. Deterministic default ordering everywhere: every unordered query and diff
    path should have stable, documented ordering.
11. Harness-level randomized topology invariants: deterministic shuffle,
    duplicate, delay, and replay of messages should converge.
12. Future placeholders: files/blobs, privacy/encryption, and tooling/admin
    invariants need placeholder tests or TODOs before implementation hardens.

I will work through these in order inside this PR. For unimplemented product
areas, add focused TODO/pending tests or log the required product decision
rather than inventing a throwaway API.

## 2026-05-27 13:41 PDT

Worked through the first several roadmap areas:

- Transaction no-op / normalization:
  - added tests for empty explicit transactions, same-row update
    normalization, and insert-then-update normalization;
  - implemented normalization in `TransactionBuilder::commit`;
  - empty explicit commit currently returns an empty tx id because the prototype
    API still returns `Result<String>`. Product API should likely become
    `Result<Option<TxId>>` or avoid exposing a tx id for no-op commits.
- Settlement / tier gating:
  - added a test showing an awaiting-dependency transaction is not visible and
    does not publish subscription diffs until the missing policy dependency
    arrives, then clears awaiting state on the same public tx id.
- Crash/reconnect state:
  - added durable recovery for fate-before-history delivery across reopen.
- Reordered/duplicated topology:
  - added a client -> edge -> core -> client convergence test with duplicated
    and reordered table bundles.
- Catalogue fail-closed:
  - added an incompatible/missing catalogue state test that fails without
    partial apply.
- Upsert:
  - added `Runtime::upsert_row` and `TransactionBuilder::upsert_row`;
  - tested create-vs-update and interaction with same-row transaction
    normalization.

The invariant module is at 57 passing tests after this slice.

Still weak / later in this PR if time permits:

- predicate/range read-set validation for exclusive transactions;
- richer policy non-leakage shape for public errors;
- branch backing-row permission gating once branch rows are more product-shaped;
- conflict metadata product shape and explicit resolution intent;
- deterministic seeded network scheduler rather than one hand-written reordered
  topology;
- placeholder/pending invariants for files, encrypted fields, and admin
  tooling.

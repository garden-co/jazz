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

## 2026-05-27 15:11 PDT

Started the requested hour-long autonomous pass. New targets for this stretch:

- conflict metadata / explicit resolution intent;
- deterministic default ordering across apply order;
- policy privacy / safe public denial shape;
- a small deterministic replay-schedule test as a first step toward a real
  randomized topology harness.

Discovery while writing policy privacy tests: hidden-policy denial and genuinely
missing dependency are intentionally not the same state in the prototype. A
known-but-hidden parent rejects with `policy_denied` and no public detail; a
missing dependency can be `awaiting_deps` because the edge may simply not have
the policy-influencing row yet. The future non-leakage contract should therefore
be phrased around public error/detail shape for terminal denial, not around
forcing all missing-vs-hidden cases into one immediate outcome.

## 2026-05-27 15:13 PDT

Added and green-tested another set of stabilizing invariants:

- conflict resolution records a semantic branch-local choice and clears current
  conflict metadata while preserving candidate provenance;
- default unordered query results converge across different incoming apply
  orders and currently sort by public row id;
- terminal hidden-parent policy denial exposes only safe public detail
  (`write_policy_denied`, table, attempted row id) and no hidden dependency id;
- a small deterministic replay schedule with duplicated/delayed bundles
  converges to the same state across replicas.

This is still not a full randomized topology harness, but it is a useful
stepping stone: the test shape is explicit schedule -> apply bundle -> compare
semantic state.

## 2026-05-27 15:15 PDT

Added ignored placeholder tests for product pillars that the invariant suite
should make visible even before the prototype has those APIs:

- file/blob bytes must not bypass row policy;
- encrypted fields must not participate in server-side plaintext querying;
- catalogue publication should require admin/tooling authority and fail closed
  without permissions;
- exclusive predicate/range read sets should reject if a matching row appears
  between read and validation;
- branch checkout/export should be gated by readable branch backing rows.

These are intentionally `#[ignore]` tests rather than abstract notes. They give
future work a concrete executable shape while keeping this PR green.

## 2026-05-27 15:18 PDT

Added two auth/tier invariants and fixed one real ordering bug they surfaced:

- trusted `run_as_user` enforces read policy, while privileged attribution can
  read all rows and still attribute writes to a named user;
- exclusive forwarding to global uses the forwarded auth user for policy, not
  the edge/service attribution user;
- default unordered `read_rows` now orders by public row id instead of
  created-at/physical row number. The old ordering was deterministic within a
  node but not across replicas that applied the same rows in different bundle
  order.

Useful clarification: local trusted writes that will later be forwarded as
exclusive global work are still ordinary local writes in this prototype; the
exclusive semantics are expressed by `export_exclusive_transaction_forwarding`
and global acceptance. A local `.exclusive()` commit still correctly fails
without global acceptance.

## 2026-05-27 15:19 PDT

Added two transaction/export shape invariants:

- one simple write call seals one transaction, while an explicit multi-row
  transaction seals one transaction with multiple row versions;
- query-scope export includes the full history for rows in the result set, so a
  receiver can rebuild current projection from the query bundle without needing
  table-wide history.

The focused invariant module is now 65 passing tests plus 5 ignored product
placeholders.

## 2026-05-27 15:20 PDT

Added a small seeded scheduler helper and convergence test. It applies a fixed
series of table-history bundles under several deterministic duplicate/reorder
schedules and asserts every peer converges to source state after insert,
update, delete, and later insert.

This is not full randomized topology testing yet, but it is a better foundation
than one hand-written schedule: the API shape now looks like "generate schedule
from seed -> apply progress steps -> compare semantic state".

## 2026-05-27 15:22 PDT

Tried to resolve a spec/prototype contradiction around observed query
descriptors:

- changed `jazz_query_read` to a TEMP table so query interests would be
  connection-local rather than durable SQLite state;
- added a durable reopen test showing observed query descriptors disappear
  after restart while the synced rows/history remain;
- full-suite validation failed 18 existing durable reconnect tests.

Important discovery: this cannot be a storage-only change. If a durable node
keeps stale local facts but forgets the previous query descriptor/result scope,
then a fresh resubscribe whose current result is empty cannot repair rows that
left the predicate. We need an explicit resubscribe/query-settlement protocol
that distinguishes retained local facts from the authoritative current result
of a reissued query.

Decision for this PR: revert the TEMP-table implementation, keep the durable
non-persistence expectation as an ignored placeholder invariant, and leave the
current persisted-descriptor behavior in place until the replacement protocol
is designed.

Full crate validation is green again after backing out the storage-only change:
359 passed, 6 ignored.

## 2026-05-27 15:26 PDT

Tightened deterministic query ordering further:

- unordered predicate reads (`eq`, `contains`) now use public row id as their
  default tie-breaker instead of physical row number / created-at ordering;
- ordered top-page queries now use public row id as the semantic tie-breaker for
  equal sort values;
- added a replica-ordering test that applies the same bundle in reversed
  physical order and asserts predicate queries still return `note-a`,
  `note-b`, `note-c`.
- added a matching ordered-page tie-breaker test for equal user sort values.

This extends the earlier default `read_rows` fix: physical ids are still useful
inside SQLite, but should not leak into observable result ordering.

## 2026-05-27 15:29 PDT

Expanded upsert coverage:

- mergeable upsert create-then-update converges through client -> edge -> core
  -> second client, preserving omitted fields on the update;
- attempted an exclusive global upsert create-then-update invariant. Current
  exclusive validation rejects the second same-row upsert as `exclusive
conflict`, even though the updates are at increasing global epochs.

Decision: keep the mergeable topology test as passing coverage, and keep the
exclusive upsert case as an ignored placeholder. We need to decide whether
exclusive upsert over an existing row should be treated as a normal globally
ordered update, or whether the caller must carry a precise read set / expected
previous version to make it non-conflicting.

## 2026-05-27 15:29 PDT

Added four more ignored product placeholders so major gaps are executable
markers rather than prose-only TODOs:

- range observed facts / range read-set validation;
- async cache eviction of uninteresting local facts;
- as-of-time query API and timestamp-to-epoch mapping;
- stable public error codes and redacted details across promises, query errors,
  sync errors, rejection subscriptions, and global callbacks.

## 2026-05-27 15:32 PDT

Incorporated one of the explorer's high-signal recommendations:

- added a passing replay immutability test showing that once a peer has applied
  an accepted transaction/history version, replaying a forged bundle with the
  same public tx id and different row content does not rewrite current state.

Also tried an overlapping-query bundle merge invariant. It exposed that
`merge_bundles` currently refuses bundles with different scoped metadata
fingerprints. That may be the right fail-closed behavior, or we may need
well-defined union/scoped-fingerprint semantics for batching overlapping
exports. Kept it as an ignored placeholder for now.

## 2026-05-27 15:32 PDT

Added the rest of the explorer's protocol/product recommendations as ignored
placeholders:

- query/subscription settled-state barriers;
- compact reconnect summaries for active descriptor replay;
- catalogue observed facts that invalidate query interpretation when schema
  heads change;
- missing-permission catalogue fail-closed behavior;
- staged untrusted authority apply before publication;
- resolved conflict candidate provenance;
- generated-index/query-plan assertions for ordered page queries.

## 2026-05-27 15:34 PDT

Added a passing subscription diff ordering invariant for a mixed
remove/update/add poll. The current diff order is:

1. removals by row id from the previous result set;
2. updates by row id from the previous result set;
3. additions by row id from the next result set.

This is implementation-shaped, but it is useful stability: listeners and
semantic diff tests should not see nondeterministic event ordering.

## 2026-05-27 15:35 PDT

Added a restore invariant matching the recent design note that restore should
reuse insert semantics:

- restoring a deleted row creates a new transaction/history version;
- the restored row keeps the previous semantic field values;
- authorship is the restoring user, not the original creator.

This is intentionally product-shaped: restore is history append, not mutation
of the original version.

## 2026-05-27 15:37 PDT

Added and implemented global public-row-id uniqueness across tables:

- inserting the same public row id into a second table now fails before writing
  history/current rows;
- reusing a row id within the same table still supports update/upsert/restore
  semantics;
- row ids created only as unresolved refs are not treated as table ownership
  until a history/current row exists.

This matches the earlier design answer that row ids are globally unique, not
table-scoped.

Also added the paired ref invariant: unresolved refs may allocate/mention a
public row id without claiming table ownership. The target table can later
create the referenced row normally.

The same uniqueness rule now applies during sync apply: a remote bundle cannot
reuse a public row id already owned by a different table, and the failed apply
leaves existing rows intact.

## 2026-05-27 15:41 PDT

Added and fixed another fate monotonicity invariant: applying a stale/lower
global acceptance for the same transaction can no longer regress the
transaction's global epoch. `accept_global` now keeps the max existing epoch.

This is separate from accepting several transactions in the same global epoch:
the new invariant is only about repeated fate enrichment for one public tx id.

## 2026-05-27 15:44 PDT

Closed the second global-epoch monotonicity path. A red test showed that
replaying a stale accepted sync bundle could regress a transaction from global
epoch 10 to epoch 5 even though direct `accept_global` had been fixed.
`apply_bundle` now merges repeated tx fate with max-existing/max-incoming epoch
semantics, so direct authority APIs and sync replay agree.

## 2026-05-27 15:45 PDT

Added the paired receipt monotonicity invariant. Replaying a stale pending bundle
for a transaction that already has edge/global receipts keeps the durable
receipt tiers and global epoch intact. This gives the fate model a simpler
shape: fate enrichment is monotonic under duplicate/stale sync, not merely under
direct authority API calls.

## 2026-05-27 15:45 PDT

Added the rejection-detail version of the same monotonic fate story. Once a
transaction has terminal rejection detail, replaying an older pending bundle for
the same tx id cannot erase the rejection code/detail or make the row current.
This reinforces the mutable-fate decision: fate can be enriched, but stale sync
should not downgrade it.

## 2026-05-27 15:46 PDT

Added an upsert-after-delete invariant. Mergeable upsert over a deleted row
behaves like restore/insert semantics: it appends a new history version, makes
the row current again, and does not claim a previous-row read dependency because
there was no currently visible row. This makes the create/update/restore boundary
for upsert sharper while exclusive upsert remains intentionally unresolved.

## 2026-05-27 15:47 PDT

Added a stale query-refresh ordering invariant. A row can leave an observed
predicate and then re-enter with newer content; applying the newer refresh first
and the older leave-scope refresh second still leaves the peer at the newer
state and does not duplicate observed descriptors. The prototype already passes,
which is a good sign for tx ordering inside query-scoped refresh bundles.

## 2026-05-27 15:48 PDT

Added a stale-delete replay invariant. After a row is deleted and then restored
with append-only insert/restore semantics, applying an older bundle whose newest
version was the delete does not hide the restored row or add duplicate history.
This strengthens restore as ordinary append-only history under reordered sync.

## 2026-05-27 15:48 PDT

Added a query-refresh locality invariant. Refreshing an active descriptor whose
result row was deleted removes that row from the query, but does not eagerly
evict unrelated cached rows learned through a different active descriptor. This
matches the local-first cache decision: query refresh repairs query truth; cache
eviction is a separate async policy.

## 2026-05-27 15:50 PDT

Added an overlapping query-refresh dedupe invariant. Two active descriptors that
observe the same row can both refresh after an update; applying each refresh
twice leaves one logical row, two logical descriptors, and no duplicate current
state. While writing it, I discovered the default todo fixture does not include
project rows as policy dependencies in this path, so the test is deliberately
about overlapping descriptors rather than shared policy dependencies. A stronger
shared-dependency version should use an explicit ref-policy schema later.

## 2026-05-27 15:51 PDT

Added a stale branch-source replay invariant. If a merge branch removes a source
and later re-adds it, applying the newer refresh before an older source-removal
refresh keeps the re-added source rows visible and preserves current source
metadata. This is a compact guardrail for branch provenance under reordered
metadata sync.

## 2026-05-27 15:51 PDT

Full crate test pass after the latest invariant slice: 378 passed, 19 ignored.
The invariant module now has 83 passing tests and 19 ignored executable
placeholders. Next validation steps are clippy and diff hygiene, then I can
commit/push if clean.

## 2026-05-27 15:52 PDT

Validation remains clean after the branch-source and overlap additions: clippy
passed with `-D warnings`, `git diff --check` passed, and no debug print macros
were found in the touched source/test paths. I am still before the requested
one-hour floor, so I will use the remaining time for one more small invariant
or cleanup pass rather than stopping.

## 2026-05-27 15:52 PDT

Added duplicate untrusted rejection idempotence coverage. Applying the same
policy-invalid untrusted bundle twice at a trusted edge leaves one rejected
transaction, no current row, stable redacted rejection detail, and a rejection
subscription event only once. This covers a common retry/reconnect path rather
than only exotic failure ordering.

## 2026-05-27 15:54 PDT

Updated the PR description to reflect the current scope: 84 passing invariant
tests plus 19 ignored executable placeholders, the runtime fixes discovered by
the suite, and the latest full validation numbers.

## 2026-05-27 15:55 PDT

Self-review caught two cleanup items: a stale PR count and awkward traceability
wording. Updated the PR description to 86 passing invariant tests plus 19
ignored placeholders, and clarified the traceability wording around trusted peers
without a scoped user.

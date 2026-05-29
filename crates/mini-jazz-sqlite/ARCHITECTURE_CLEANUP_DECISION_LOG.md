# Architecture Cleanup Decision Log

## 2026-05-28 22:44 PDT

Goal: spend at least 6 hours moving the SQLite core spike toward a real generic
Jazz core rather than a prototype full of test-specific seams. Target end time:
2026-05-29 04:44 PDT or later.

Initial constraints:

- work on a new stacked branch from `codex/mini-jazz-sqlite-spec`;
- inspect open PRs against the spike in detail before and during changes;
- selectively merge/cherry-pick useful work, especially tests, while keeping
  the branch coherent;
- watch #973 because it is moving in another session;
- keep commits small enough to review and push after each coherent slice;
- prefer general runtime APIs and reusable test harness fixtures over hardcoded
  scenario helpers;
- keep this log timestamped with `date` before adding entries.

Open stacked PRs to inspect:

- #973 `codex/sqlite-core-deep-history-efficiency` - moving performance PR by
  Anselm;
- #972 `guido/mini-sqlite-sql-scoped-query-export` - SQL scope export work;
- #952 `guido/transplant-branch-permissions-query` - branch permissions query;
- #947 `guido/mini-jazz-sqlite-subscription-invariants` - subscription tier
  invariants;
- #945 `nico/jazz-sqlite-tx-isolation` - transaction isolation tests/WIP impl.

## 2026-05-28 22:46 PDT

First local code-shape read after pulling the latest spike: the main broken
window is still `src/runtime.rs` at roughly 7.9k lines. It mixes auth/session
state, local writes, transaction builder, authority apply, branch APIs,
query-scope export, repair, subscriptions, and diagnostics.

Useful first cleanup slice: extract auth/session semantics into a focused module
before bigger refactors. This is low risk and product-shaped: the distinction
between an untrusted client user, a trusted peer running as a user, trusted
admin/system work, and privileged attribution should be a crisp API boundary.

## 2026-05-28 22:48 PDT

Parallel PR inspection returned:

- #973 deep-history/perf PR: do not merge/cherry-pick wholesale yet. It is a
  moving 173-commit draft with dirty merge state. Keep the lessons: hot current
  projection, open operational history, sealed cold history blocks, and
  sidecar/deep-text ops as separate concerns. Maybe port narrow invariants/docs
  later.
- #945 transaction isolation: implementation is throwaway, tests/spec are
  valuable. The right model is transaction start snapshot plus own staged write
  overlay, excluding other transactions' staged writes and later commits.
  Reimplement through read contexts/visibility machinery rather than custom SQL.
- #947 subscription tiers: valuable D.8 semantics, but implementation duplicates
  visibility SQL. Port tests later; implement tier as visibility/query mode.
- #952 branch permissions: most semantically important Guido PR. Keep tests and
  branch-policy API ideas; port through `policy`/`ReadVisibility` rather than a
  parallel route.
- #972 SQL-scoped query export: strongest near-term performance/product value.
  CTE row-scope direction is right, but port carefully against current query
  repair/read-visibility code instead of merging blindly.

Integration strategy for this cleanup PR: first reduce broken-window pressure in
the base by extracting crisp generic modules. Then pull in tests from #945/#952/
#947/#972 as red/green slices. Avoid wholesale merges unless a PR becomes clean
and narrow.

## 2026-05-28 22:49 PDT

Second cleanup target: remove stale "attempt3 fixture" naming from the core.
The default todo app schema may still exist as a convenience/example while the
tests are migrated, but names like `attempt3_fixture` invite test-specific
thinking in core code. Rename to `todo_app_schema` as an incremental cleanup.

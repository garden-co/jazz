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

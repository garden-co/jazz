# Recursive Queries + Permissions Follow-up TODOs (TDD)

Status note: this list is the refreshed "what remains" plan after the latest gather/hop and policy write-path fixes.

## 1) Write-policy correctness unification

- [x] Red: add local write tests for complex clauses (`Exists`, `ExistsRel`, `Inherits`):
  - `query_manager::rebac_tests::local_insert_with_exists_rel_policy_denies_non_admin`
  - `query_manager::rebac_tests::local_update_with_check_inherits_denies_when_parent_is_not_updateable`
  - `query_manager::rebac_tests::local_update_using_exists_policy_allows_admin_and_denies_non_admin`
- [x] Green: route local write checks through simple+complex policy graph evaluation.
- [x] Red: add `delete_with_session` complex-clause regression test(s), then implement any missing evaluator parity.
  - `query_manager::rebac_tests::local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin`

## 2) Server compile error surfacing

- [x] Red: add test proving uncompilable query subscriptions return a client-visible error.
  - `query_manager::manager_tests::server_sends_error_for_uncompilable_query_subscription`
- [x] Green: emit explicit sync error payload instead of silently dropping failed subscriptions.
- [x] Hardening: add one assertion-focused test that validates error message context quality (query id + reason shape).

## 3) Permissions docs cleanup

- Docs note: no red-test requirement for docs-only updates.
- [x] Replace stale `policy.recursive(...)` docs with `policy.<table>.gather(...)` + `hopTo(...)`.
- [x] Update MVP constraints in advanced recursive section.
- [x] Add one end-to-end docs example showing: recursive gather -> post-gather hop -> where filter.

## 4) TS permissions compiler single-path cleanup

- [x] Red: add tests that enforce one canonical API/compile path (`definePermissions`) and fail on split behavior.
  - `permissions/index.test.ts::does not expose transitional definePermissionsV2 API`
  - `permissions/index.test.ts::compiles policy.exists(relation) to ExistsRel in definePermissions`
- [x] Green: remove `definePermissionsV2` and legacy conversion helpers (`legacyPolicyExprToV2`, predicate adapters), then route all callers through one path.
- [x] Cleanup: align type-inference tests (`packages/jazz-tools/src/permissions/type-inference.test.ts`) to the final single API.

## 5) IR-first Query shape cleanup in Rust

- [x] Red: add tests that fail when runtime depends on legacy query-field normalization fallback.
  - `query_to_relation_ir::normalize_query_rejects_legacy_recursive_join_projection_spec`
  - `graph::compile_query_with_recursive_join_projection_relation_is_rejected`
  - `manager_tests::recursive_query_with_legacy_join_project_step_is_rejected`
- [x] Green: remove residual normalization scaffolding / shape-compat fallback in `query_to_relation_ir`.
  - Removed legacy recursive join+`result_element_index` normalization path; only direct step and hop-based recursive shapes are accepted.
- [x] Cleanup: keep builder ergonomics intact while enforcing relation-IR-first execution boundaries.

## 6) Planner gap closure

- [x] Red: add join+policy interaction tests in query planning/execution.
  - `manager_tests::join_query_applies_policy_filter_on_joined_table`
- [x] Red: add multi-branch join tests that fail under first-branch-only behavior.
  - `manager_tests::join_query_with_multiple_branches_reads_all_branches`
- [x] Green: implement planner behavior for both suites, then remove first-branch shortcuts.
  - `compile_join_plan` now scans/merges all query branches and applies per-table select policies before joins.

## Suggested execution order

- [x] Slice A: finish item 1 (`delete_with_session` complex-clause parity).
- [x] Slice B: item 4 (TS permissions single-path cutover).
- [x] Slice C: item 5 (Rust IR-first cleanup).
- [x] Slice D: item 6 (planner gaps).
- [x] Slice E: remaining docs hardening in item 3.

# Making Tests More E2E — TODO

Remaining gaps in test coverage.

> Status quo: [specs/status-quo/making_tests_more_e2e.md](../status-quo/making_tests_more_e2e.md)

## Browser E2E: Auth/Policy Scenarios

**Priority: Medium**

Browser tests cover basic CRUD and persistence but lack:
- Permission policies (ReBAC) in browser context
- Multi-client scenarios (concurrent inserts, conflict resolution)
- Scope-based access control
- Session management

## RuntimeCore: Scope-Bypass Regression Test

**Priority: Low**

No explicit RuntimeCore test that:
1. Creates a schema
2. Inserts a row through the client API (auto-generates realistic metadata)
3. Verifies unauthorized clients can't access it

The bug is fixed via role-based auth, but a regression test at RuntimeCore level would prevent re-introduction.

## QueryManager Test Consolidation

**Priority: Low**

~91 QueryManager tests remain at a level below RuntimeCore. Some are valuable for internal logic verification, but candidates for consolidation into E2E RuntimeCore scenarios include:
- `insert_and_query()` patterns that could be E2E'd with actual message pumping
- Basic CRUD tests that duplicate what RuntimeCore tests already cover

## Cross-Schema Workflow E2E

**Priority: Low**

No RuntimeCore test for "user inserts in old schema, schema evolves, user queries new schema" — a full workflow exercising schema evolution through the public API.

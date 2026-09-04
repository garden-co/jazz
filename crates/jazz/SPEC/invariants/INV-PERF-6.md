# INV-PERF-6

- Status: now
- Coverage: untested

## Invariant

INV-PERF-6 current-row optimization must preserve deletion/restore visibility, including register witnesses. Identifiers: register_global_current_table_name(table), VersionLayer::Deletion, DeletionEvent::{Deleted, Restored}. Tests: deletion remove lines 1699-1748 and restore witness lines 1750-1818.

## Enforced by (tests)

NONE-FOUND

## Implementation

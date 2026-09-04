# INV-PERF-7

- Status: now
- Coverage: untested

## Invariant

INV-PERF-7 DurabilityTier::Global current-row reads and global current-row query graphs are allowed to use overwrite current tables rather than history argmax graphs, but must remain semantically equivalent to visible current rows. Identifiers: Node::current_rows lines 714-735, visible_current_graph lines 1182-1220, global_current_table_name, register_global_current_table_name, write_global_current_update.

## Enforced by (tests)

NONE-FOUND

## Implementation

# SQLite large-value trigger sets

This port brings the merged implementation from #4237 (`54be2ac12b`) into the
E2EE SQLite stack. It groups the triggers that retain and release large-value
roots. Each group covers at most 32 physical columns, keeping the generated
statements and conditions within the restricted SQLite host limits.

The stored SQL role is `jazz.sqlite.large-value-trigger-set.v1`. Its trigger
names contain `__lv_v1_`; fresh tables use one insert, delete and update trigger
per group, for both current rows and history. Additional groups are named after
their first physical column. The exact SQL for an initial two-column group and
a later one-column group is pinned in
`crates/jazz/fixtures/sqlite_large_value_trigger_set_v1.sql`.

An insert retains each chunked root; a delete releases each chunked root. An
update changes the count only for columns whose root changed. Inline and null
cells add no references. Equal roots in different columns count separately.
Each large-capable column keeps its partial index, so finding chunked rows does
not become a full table scan. Permissions, row encoding, chunk encoding and sync
messages are unchanged.

Existing stores keep the triggers already installed for their columns, including
the earlier per-column layout. Opening a store reads its physical columns and
only adds groups for newly admitted columns; it does not install overlapping
counters for existing columns. Schema growth installs a disjoint group for new
columns. Physical column names keep these groups stable across logical renames.
There is no trigger rewrite or chunk migration on reopen. This does not promise
that older binaries understand later E2EE catalogue or wire formats.

The upstream counter regression checks inserts, updates, unchanged columns,
later groups and deletion. A public database regression checks 65 nullable
columns spanning group boundaries, shared roots, schema growth, close/reopen,
and restoration after deletion. The restricted-host run uses the existing
`durable-object-limits` feature. Existing deployment tests retain their schemas,
assertions and deadlines. These are compatibility and correctness checks, not
a hosted product-performance comparison.

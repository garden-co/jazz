---
"jazz-tools": patch
---

**Rust: hopTo, gather, union, includeDeleted and history in `jazz-app`.** A typed query now covers the rest of the TypeScript client's query forms.

- **Hops.** `todos::query().filter(...).hop_to(todos::project_relation())` yields a `Query<Project>`: the rows a forward relation references, or, for a reverse relation, the rows that reference them. Hops chain. Filters, order and window after a hop apply to the rows it reached.
- **Gather.** `todos::query().id(leaf).gather(todos::parent_relation(), 10)` reads the rows a forward relation reaches recursively, up to a depth, seeded by the query's filters. `Step::via(edge_from, edge_to)` steps through an edge table, and `Step::filter` narrows the rows a step goes through.
- **Union.** `Query::union([a, b])` (or `Query::union_named` with labels) reads several queries of the same table. Like TypeScript's `union`, it takes only `order_by`, `limit` and `offset`.
- **Deleted rows.** `.include_deleted()` reads deleted rows too. `jazz_app::deleted_at()` is the column that tells them apart.
- **History.** Each table module has `history()`, a `Query<Version<Row>>` over the row's accepted versions. Each `Version` holds the row, the `transaction_id` that wrote it and when the authority accepted it (`accepted_at`). `.accepted(range)` and `.order_by_accepted_at(...)` filter and order versions by acceptance time. Subscribing to history is not supported yet.
- **By name.** `jazz_app::untyped::Query` builds the same queries from table and column names (`Table`, `Hop`, `GatherStep`), for bindings in other languages. The typed `Query<T>` wraps it, and `untyped::transaction_id` and `untyped::accepted_at` decode history metadata.
- Combinations TypeScript refuses (modifiers on a union, `include` after `gather`) fail with `Error::Unsupported`. `Query::into_untyped` now returns a `Result`.

Rust bindings move to format 4, which adds `history()` and reserves it as a function name. Regenerate them with `jazz-tools generate`.

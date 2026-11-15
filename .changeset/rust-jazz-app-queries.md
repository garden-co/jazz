---
"jazz-tools": patch
---

**Rust (`jazz-app`): more typed queries, and new rows take their required columns.**

- **Comparisons** take whatever converts into the column's type: `todos::title().eq("milk")`, and `todos::priority().gte(3)` on a nullable column.
- **New filters:** text and array columns have `contains`; nullable columns have `is_not_null`.
- **Built-in columns:** `jazz_app::id()`, `created_at()` and `updated_at()` (and each table module's `id()`) work in filters and `order_by`.
- **Panics:** a call that panics fails with `Error::Panicked`, and the client keeps running.

**Breaking for generated Rust bindings (format 2):**

- **`NewX`** derives `Default` only when every column may be left out, so a required column is never filled in silently. `NewX::new(…)` takes the required columns: `NewTodo { description, ..NewTodo::new(title, false) }`.
- **Module functions:** a column named `query` or `id` gets `query_column()` / `id_column()` in its table module.
- **`is_in`** takes anything whose items convert into the column's type. An empty literal (`is_in([])`, `is_in(Vec::new())`), or an `.eq(x.into())`, now needs a type annotation.
- **Regenerating:** run `npx jazz-tools generate` to update `src/jazz_app.rs`.

A `Cargo.toml` now marks a Rust app only when the crate depends on `jazz-app` in `[dependencies]` (or a target's dependencies). Workspace, dev- and build-dependencies don't count.

The docs gain a Rust Backend setup page, and Rust tabs on the reading and writing pages.

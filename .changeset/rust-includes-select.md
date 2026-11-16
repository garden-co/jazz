---
"jazz-tools": patch
---

**Rust: includes and select in `jazz-app-support`.** A typed query now includes related rows and reads partial rows, as `.include(...)`, `requireIncludes` and `.select(...)` do in TypeScript.

- **Includes.** `todos::query().include(todos::project_relation())` yields `(Todo, Option<Project>)` per row. A reverse relation includes a `Vec<U>`. Each further `include` adds a tuple element, up to eight. `.require()` works as TypeScript's `requireIncludes` does. On a forward relation whose column is not null, it includes the row itself (`(Todo, Project)`) and keeps only the rows whose referenced row is there. On a nullable one, the include stays an `Option` and every row is kept. Generated bindings mark nullable relations as `One<T, U, Nullable>`.
- **Array relations.** A relation over a `uuid[]` column is generated as `Refs<T, U>` and includes a `Vec<U>`. `.require()` on a not-null one keeps only the rows whose every referenced row is there, and `referencing(id)` filters by a referenced row.
- **Shaping an include.** `relation.with(query)` filters, orders, limits, offsets, selects or nests the included rows, for example `projects::todos_via_project().with(todos::query().filter(...).include(todos::parent_relation()))`.
- **Select.** `todos::query().select((todos::title(), todos::done()))` yields `jazz_app_support::Partial<Todo, (String, bool)>`, which holds the row's id and the selected values in order. A single column yields its value. A transformed column is selected as its stored value, as columns are typed everywhere in Rust, unlike TypeScript, which gives the view value.
- **Everywhere a query goes.** `all`, `one` and `subscribe` take these queries, and `Rows` keeps subscribed rows with includes in order.

Rust bindings move to format 3, which adds `Table::COLUMNS`. Regenerate them with `jazz-tools generate`.

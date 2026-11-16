---
"jazz-tools": patch
---

**Rust: `select` reads a transformed column as viewed.** In `jazz-app`, selecting a column with a transform now yields its view value through the app's transform, as a full row and TypeScript do. For example, `tasks::query().select(tasks::due_at())` yields the `String` the `isoDate` transform makes, where it used to yield the stored `Timestamp`. Conditions still compare the stored value. Generated bindings type such a column `Column<Task, Option<Timestamp>, DueAtView>`, where `DueAtView` is a generated `jazz_app::Pick`. Regenerate bindings that declare transforms with `jazz-tools generate`.

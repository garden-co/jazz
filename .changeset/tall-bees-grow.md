---
"jazz-tools": patch
---

Allow backend `JazzClient` and `SessionClient` query/subscribe calls to consume generated query builders directly. Query-builder payloads with `_schema` are now translated automatically to runtime query JSON (`relation_ir`), so backend code can call `context.forRequest(...).query(app.todos.where(...))` without manual `translateQuery(...)`.

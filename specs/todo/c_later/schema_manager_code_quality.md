# Schema Manager Code Quality — TODO (Later)

Minor non-blocking cleanup items:

1. 9+ one-line delegates to SchemaContext in manager.rs — minor boilerplate
2. `pending_schemas` is public in context.rs — lifecycle not enforced via private fields
3. Duplicate metadata building patterns in `persist_schema()` / `persist_lens()`
4. Similar error handling in `process_catalogue_schema()` / `process_catalogue_lens()` could be extracted
5. Draft lenses stored via catalogue processing but logging incomplete (`manager.rs:591` — `// TODO: proper logging`)

# QueryManager Constructor Unification — TODO (Later)

Two constructors with different behaviors:

1. `QueryManager::new()` — auto-subscribes to all object updates
2. `QueryManager::new_with_schema_context()` — does NOT auto-subscribe because `handle_object_update()` doesn't support multi-schema decoding

**Fix**: Make `handle_object_update()` schema-aware (detect branch schema via `branch_schema_map`, get appropriate descriptor, decode accordingly).

> `crates/groove/src/query_manager/manager.rs`

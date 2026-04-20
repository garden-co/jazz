---
"jazz-tools": patch
---

Stop persisting and rehydrating a bogus empty schema on dynamic-schema servers.

`SchemaManager::new_server` leaves the context uninitialized with a sentinel hash. Runtime construction then called `ensure_current_schema_persisted`, writing a placeholder `catalogue_schema` row whose content hashed to the empty-schema digest. On rehydrate that hash surfaced as an "unreachable schema hash" in every connection diagnostics call. The persist path now no-ops while uninitialized, and `process_catalogue_update` ignores empty schemas for forward-compatibility with sqlite files written by the pre-fix server.

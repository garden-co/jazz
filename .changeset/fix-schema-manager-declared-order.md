---
"jazz-tools": patch
---

Fix `SchemaManager::insert` to keep accepting values in the application's declared column order even when the runtime normalizes schema columns internally.

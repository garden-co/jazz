---
"jazz-tools": patch
---

Rust: remove `Db::activate_schema` and `Db::activate_schema_with_lenses`. A store installs its schema when it opens (core ships with its backend); use `Db::admit_schema_with_lenses` to admit a version while running, and reopen the store under it to make it current. Opening a store along the app's lenses now renumbers enum tags to the new declaration order and refuses a version that drops a branch column, as activation did. No stored or wire bytes change.

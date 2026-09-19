---
"jazz-tools": patch
---

Preserve structural column defaults in generated migration witnesses, including UUID-to-relation migrations. Validate each witness against its own canonical schema, while preserving ordinary default-only metadata changes without row transforms.

Correct CLI-computed schema hashes to match existing Rust/server identities for defaults, merge strategies, and branch bindings. Stored schema identities and wire formats do not change.

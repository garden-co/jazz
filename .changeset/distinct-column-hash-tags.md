---
"jazz-tools": patch
---

Give Double and Bytea columns distinct, cross-runtime structural schema hashes. Existing Bytea catalogue identities remain resolvable, and both schema-only and permissions-bearing deploys publish the corrected identity with an explicit durable legacy-to-current migration edge.

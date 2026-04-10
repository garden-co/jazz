---
"jazz-tools": patch
---

Fix enum literals in nested policies

Nested relation-backed permission filters now serialize enum literals as tagged runtime values instead of raw strings, so publishing permissions and loading them into `createJazzContext(...)` works for cases like `grant_role: "viewer"`.

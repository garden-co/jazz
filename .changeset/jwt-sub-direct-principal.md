---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
---

Simplify external JWT identity: `session.user_id` is now the JWT `sub` claim verbatim. The `jazz_principal_id` claim, the `external_identities` server mapping, and the hashed `external:…` fallback are removed. External providers must emit the desired Jazz user id as `sub` directly (e.g. via `getSubject: ({ user }) => user.id`). Also fixes `authMode` resolution in the policy evaluator and preserves `AnonymousWriteDeniedError` through the runtime write path.

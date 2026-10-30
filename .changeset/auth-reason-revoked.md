---
"jazz-tools": patch
---

Auth state now reports a revoked account identity as its own `"revoked"` failure reason. Accounts no longer try to renew a revoked identity; they stay unauthenticated so the app can sign the user out.

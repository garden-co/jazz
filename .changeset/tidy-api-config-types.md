---
"jazz-tools": patch
---

Breaking change: rename `CreateOptions` to `InsertOptions` and reject conflicting `secret`, `jwtToken`, and `cookieSession` values in `DbConfig` at compile time. After local-first authentication is resolved, `Db.getConfig()` returns the minted JWT without also exposing the original secret.

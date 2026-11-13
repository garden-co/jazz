---
"jazz-tools": patch
---

The app backend's sync route (`createJazz`) now reports a credential the client sent but that doesn't verify there (a JWT it can't verify, a malformed token, a backend client credential that doesn't match) as `invalid`, not `missing`: `db.getAuthState().error` and the session's auth status say `invalid`. A request with no credential still reports `missing`. A token with no signature refused by the Jazz server is reported as `invalid` too (its refusal message no longer says "missing").

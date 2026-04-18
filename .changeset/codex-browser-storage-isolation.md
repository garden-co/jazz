---
"jazz-tools": patch
---

Isolate browser-local Jazz persistence by default across users and app scopes.

`createDb()` now derives the default browser persistent namespace from both `appId` and the resolved authenticated principal when no explicit `dbName` is provided, preventing one user from reopening another user's OPFS-backed local cache. `BrowserAuthSecretStore` also now accepts scope hints like `appId`, `userId`, and `sessionId` so browser apps can avoid sharing one global local-first identity secret across unrelated sessions.

---
"jazz-tools": patch
---

Fix race condition in `AuthSecretStorage.set()` where `isAuthenticated` was set to `true` before the KV store write completed, causing spurious logouts in the BetterAuth client.

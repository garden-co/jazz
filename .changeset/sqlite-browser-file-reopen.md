---
"jazz-tools": patch
---

Revoke retained browser runtime handles when their owner closes, then reload IndexedDB files on reopen so cached data cannot hide external replacement, corruption or deletion. Allow callers to require an existing file instead of silently creating a replacement.

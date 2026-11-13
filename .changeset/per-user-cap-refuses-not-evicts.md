---
"jazz-tools": patch
---

Tabs of one signed-in user past the server's connection cap per user no longer evict each other in a reconnect loop. The server never evicts a live connection: past the cap it evicts only a connection that has gone silent for longer than a keepalive, and otherwise refuses the new connection with a retryable error. The client backs off, `getSyncStatus().lastError` names the cap, and the tab joins once another one closes. The cap goes from 16 to 128 per user and can be set with `--max-connections-per-user` (`JAZZ_MAX_CONNECTIONS_PER_USER`).

---
"jazz-tools": patch
---

Clients now reconnect with backoff when the server ends a connection because it is overloaded or still starting, and resend their pending writes. Previously browser and Node clients stopped syncing until the app called `reconnect()`.

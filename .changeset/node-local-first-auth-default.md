---
"jazz-tools": patch
---

Align Node backend local-first request authentication defaults with the sync server: disabled when `NODE_ENV` is `production` (case-insensitive), enabled otherwise. Explicit `allowLocalFirstAuth: true` and `false` still override the default. Production backends that relied on the previous implicit `true` must now set `allowLocalFirstAuth: true` to keep accepting local-first tokens. Sync-server account verification still applies. This option does not control local-first account creation by the backend.

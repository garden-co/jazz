---
"jazz-tools": patch
---

A client syncing over the HTTP stream now goes `offline: auth` when its session is refused (for example after `jazz.revokeUser`), as a socket client does, so its session asks the backend and signs a revoked user out. Before, it read the stream's close as a dropped connection and kept reopening streams the backend refused, staying `connecting`.

---
"jazz-tools": patch
---

Export `resolveRequestSession` from `jazz-tools/backend` so RPC handlers can derive Jazz sessions from request bearer JWTs without manual decoding.

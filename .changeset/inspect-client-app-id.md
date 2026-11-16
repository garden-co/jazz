---
"jazz-tools": patch
---

`jazz-tools inspect whoami` (and the MCP `whoami`) now also answers `clientAppId`: the id this app's browser clients name it by in their sync URLs (`/jazz/apps/<clientAppId>/…`). A browser derives it from the URL it syncs with, and the backend maps it to the store's `appId`, which is why the two differ.

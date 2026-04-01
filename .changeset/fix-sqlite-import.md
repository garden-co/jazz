---
"jazz-tools": patch
---

Fix MCP docs server failing to use SQLite/FTS5 backend by preventing bundlers from stripping the `node:` prefix on `node:sqlite` imports.

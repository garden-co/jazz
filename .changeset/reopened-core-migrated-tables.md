---
"jazz-tools": patch
---

Fix writes to a table or column a migration added being rejected with "value does not match type U64" once a SQLite core reopens (a dev-server restart, or a Vercel Sandbox core waking after its idle stop). A reopened core now knows every stored column by the type its schema version declared, not only the columns of the schema it reopens under.

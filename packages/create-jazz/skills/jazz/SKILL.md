---
name: jazz
description: Build or troubleshoot a Jazz application. Use for Jazz schemas, reads, subscriptions, writes, framework integration, sync behavior, authentication, permissions, migrations, or testing.
---

# Jazz

Start with the project's installed `jazz-tools` version, `schema.ts`, optional
`permissions.ts`, provider/client setup, and existing tests. Those are authoritative for a
specific app; do not invent an API from memory.

Use the typed schema and query builders rather than JSON-shaped schema, permission, or query
objects. Keep authorization in `permissions.ts`, not in client-side visibility checks. Treat
writes as local-first: use an explicit durability wait only when the operation needs a sync
receipt.

## Read only the relevant reference

- For schema shape, relations, column types, or migration work, read
  [schemas-and-permissions.md](references/schemas-and-permissions.md).
- For queries, subscriptions, writes, framework hooks, or sync expectations, read
  [application-data.md](references/application-data.md).
- For authentication, account bootstrap, sessions, or server-side setup, read
  [authentication.md](references/authentication.md).
- For tests, read [testing.md](references/testing.md).

When the installed source and the current public documentation differ, follow the installed
source and identify the version difference. Do not add separate caches around live Jazz queries
unless the application has an explicit independent requirement.

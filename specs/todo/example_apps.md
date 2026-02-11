# Example Apps — TODO

Reference applications demonstrating jazz2 capabilities.

## Overview

A set of example apps that serve as both documentation and integration tests:

- **Todo app** — canonical CRUD example (already have `todo-server-ts` and browser tests)
- **Collaborative text editor** — real-time multi-user editing
- **Chat app** — messages, rooms, presence indicators
- **Project management board** — Kanban with drag-and-drop, assignments
- **Photo gallery** — binary data, image serving, offline access
- **Inventory tracker** — complex queries, aggregations, reports

Each example should demonstrate: local-first offline, real-time sync, persistence, auth.

## Open Questions

- Which framework(s)? React, Next.js, Remix, plain HTML?
- Monorepo examples vs. standalone template repos?
- How to keep examples in sync with API changes (CI test them)?
- Progressive complexity: starter → intermediate → advanced?

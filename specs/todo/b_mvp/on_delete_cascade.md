# On Delete Cascade — TODO

Cascading deletes for foreign key relationships.

## Overview

When a parent row is deleted, child rows referencing it via FK should be automatically deleted (or nullified, depending on policy). This is standard SQL `ON DELETE CASCADE` / `ON DELETE SET NULL` semantics, but needs to work correctly in a distributed, conflict-merging environment.

## Open Questions

- How does cascade interact with concurrent edits on different peers? (Peer A deletes parent, Peer B updates child — who wins?)
- Should cascade be evaluated at write time or at merge/sync time?
- Do we need `ON DELETE RESTRICT` (prevent delete if children exist)?
- Cascade depth limits to prevent runaway deletions?

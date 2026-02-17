# Partial Schema Visibility — TODO (Later)

Ability to hide parts of the schema from clients.

## Overview

Some tables or columns may be server-internal (audit logs, admin fields, internal metadata). Clients shouldn't see these in their schema catalogue or be able to query them.

- Per-table visibility: "this table is server-only"
- Per-column visibility: "this column exists but isn't synced to clients"
- Catalogue filtering: server strips hidden items before sending catalogue to clients

## Open Questions

- Is this role-based (Admin sees everything, User sees subset)?
- How does this interact with policies — can a policy reference a hidden column?
- Does the client schema hash include or exclude hidden items?

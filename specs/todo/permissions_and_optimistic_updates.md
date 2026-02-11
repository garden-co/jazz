# Permissions & Optimistic Update DX — TODO

Declarative permission policies with good DX for optimistic local-first writes.

## Overview

Permissions are enforced **on the server only**. Local writes are applied immediately as optimistic updates, then confirmed or rejected by the server after sync.

### Permission Model

Inspired by Postgres row-level security, but adapted for JWT-native auth:

- Policies are declared per-table in the schema (not in backend code)
- Policies can inspect the JWT token (claims, roles, groups) directly
- The backend creates a **scoped client** for each request, acting as the user with that JWT — Jazz enforces permissions, not the backend code
- This moves permission logic from imperative backend code into declarative, testable policies

### Optimistic Update DX

The key DX challenge: local writes succeed instantly, but the server may reject them.

**Default behavior** (good for most apps):
- Writes appear immediately in the local query results
- If the server rejects, the write disappears (rollback)
- Reactive queries update automatically on rollback

**Explicit pending state** (for apps that need it):
- Rows/mutations carry a settlement tier indicator
- Developers can show "pending" / "confirmed" / "rejected" states in UI
- Query filter: "only show confirmed rows" or "show all including pending"

## Scoped Backend Clients

When a backend needs to interact with Jazz (e.g., for side effects, webhooks):

- Receive the calling user's JWT
- Create a Jazz client scoped to that JWT
- All queries/mutations through that client respect the user's permissions
- No need for imperative permission checks in backend code

## Open Questions

- Policy language: SQL-like `WHERE` clauses? DSL expressions? Both?
- How to test permissions? (Unit test policies against mock JWTs?)
- Can policies reference related tables? (e.g., "allow if user is member of the row's organization")
- Admin override: how do admin/peer clients bypass policies?
- How to communicate rejection reason to the client? (Generic "permission denied" vs. specific?)
- Offline duration: if a user is offline for days with optimistic writes, what happens when they come back and many are rejected?

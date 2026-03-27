# Problem: Orphaned Client State on the Server

## What's broken

When a Jazz client connects to a server, the server allocates per-client state: sync cursors (`sent_tips`, `sent_metadata`), query subscriptions, query scopes, session records, pending permission checks, and outbox entries. This state enables incremental sync on reconnect — the server only sends commits the client hasn't seen.

When a client disconnects, the server retains all this state so the client can reconnect and resume. This is intentional (see `routes.rs:252` — "Keep logical client state across disconnects so reconnect with the same client_id can resume query forwarding state").

The problem: **there is no path from "disconnected" to "cleaned up."** Clients that are permanently gone (uninstalled app, cleared browser data, abandoned session, expired JWT) leave orphaned state per client that accumulates forever. The state is held in a plain in-memory `HashMap<ClientId, ClientState>` — not persisted, not paged. Estimated ~20KB per client based on struct sizes (queries, sent_tips, metadata sets), though actual size depends on app usage patterns.

Beyond memory, stale clients with active query scopes cause the server to compute and enqueue sync updates for ghosts — wasting CPU on every tick.

## Who is affected

- **Server operators** (and Garden cloud infrastructure) — memory grows without bound as clients churn. In a multi-tenant cloud with many apps, orphaned state across thousands of apps compounds into a real resource problem.
- **Indirectly, all users** — degraded server performance and increased hosting costs as stale state accumulates.

## Concrete examples

1. A user visits a Jazz-powered web app, uses it for 10 minutes, closes the tab. Their `ClientState` (queries, sent_tips, session) lives on the server forever.
2. A mobile app is uninstalled. The server has no way to know — the client simply never reconnects. State persists.
3. A cloud deployment running 50 apps, each with 1000 daily users and ~10% returning: after a month, ~45,000 orphaned client states per app × 50 apps × 20KB ≈ 43GB of stale in-memory state.

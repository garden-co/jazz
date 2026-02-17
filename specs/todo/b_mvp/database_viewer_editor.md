# Database Explorer, Introspection, and DevTools

Add first-class introspection tooling for Jazz in two forms:

1. Chrome DevTools extension for local client Jazz state.
2. Standalone web app for remote Jazz server state (admin-authenticated, tier-aware).

This document supersedes the previous minimal TODO and captures the current design direction.

## Problem

Today we can query and mutate data, but we do not have a dedicated way to inspect:

- live schema versions and branch mappings
- policy definitions and effective policy paths
- actual data in tables (including soft/hard delete state)
- live query subscriptions and settlement behavior across tiers

Developers need a "what is in the system now" tool that works both for local browser state and remote servers.

## Goals

- Reuse existing query/mutation code paths as much as possible.
- Keep inspector read/write behavior aligned with normal runtime behavior.
- Support admin-only auth bypass for inspector operations where required.
- Add live-query introspection without polluting that introspection with inspector-owned meta-queries.
- Provide one conceptual inspector model that can be rendered in:
  - Chrome DevTools panel
  - standalone admin web app

## Non-goals (MVP)

- Full SQL playground beyond existing `Query` model.
- Full conflict-resolution UI.
- Policy authoring/editing UX.
- Time travel and branch graph visualization.

## Current System Snapshot (Important Constraints)

- Query execution and subscriptions are handled by `QueryManager`.
- Sync transport is `/sync` (POST) and `/events` (binary stream).
- Server auth currently supports JWT, backend impersonation, and admin secret (admin currently used for catalogue writes).
- Schema evolution is managed by `SchemaManager` with lenses and hash-based composed branches.
- Query settlement already supports persistence tiers (`worker`, `edge`, `core`) via `QuerySettled` and `PersistenceAck`.
- `WasmSchema`/JS schema surfaces currently expose table/column structure but not policy AST.
- Catalogue schema encoding currently drops policy data.

## Product Shape

Use a shared inspector core with two adapters, not two separate implementations.

### Shared Core

`jazz-inspector-core` (UI state + data model):

- schema explorer
- table data grid
- row inspector
- policy viewer
- live query viewer
- sync/tier status viewer

### Adapter A: Chrome DevTools (local)

Connects to local client Jazz runtime state in page context.

### Adapter B: Standalone Web App (server)

Connects to a Jazz server via dedicated admin introspection endpoints.

## Integration Strategy: Reuse Existing Execution Paths

### Principle

Inspector query/mutation should call the same internals used by normal app queries/mutations, with explicit execution options.

### New Internal Execution Options

Add an execution context/options object passed through query and mutation entry points:

- `auth_mode`: `enforced | admin_bypass`
- `visibility`: `public | hidden_from_live_query_list`
- `propagation`: `normal | local_only`
- `origin`: `app | inspector`

Default app behavior remains unchanged (`enforced + public + normal + app`).

Inspector meta-queries use:

- `admin_bypass` (server-only, with strict auth gate)
- `hidden_from_live_query_list`
- `local_only` (do not forward upstream)
- `origin=inspector`

## Live Query Introspection Without Self-Pollution

### Requirement

When viewing live queries, inspector-owned subscription traffic must not appear in the list by default.

### Plan

Extend subscription state metadata (local and server-side) with:

- `origin`
- `visibility`
- `created_at`
- `query_id` and context info

Add list APIs:

- `list_live_queries(include_hidden: bool)`
- default `include_hidden=false`

Inspector meta-queries always set hidden visibility.

## API Surfaces

## Local Runtime Introspection Surface (WASM/NAPI)

Add read-only inspector APIs exposed to JS from runtime bindings.

Examples:

- `getInspectorSchemas()`
- `getInspectorPolicies()`
- `listInspectorLiveQueries(includeHidden?: boolean)`
- `inspectTableRows(table, filters, pagination, options)`
- `inspectMutate(table/object, operation, payload, options)`

These should route to existing `RuntimeCore`/`SchemaManager`/`QueryManager` operations with execution options.

## Server Admin Introspection Endpoints

Add dedicated endpoints under `/admin/introspection/*`.

Examples:

- `GET /admin/introspection/schemas`
- `GET /admin/introspection/policies`
- `POST /admin/introspection/query`
- `POST /admin/introspection/mutate`
- `GET /admin/introspection/live-queries`
- `GET /admin/introspection/sync-state`

Auth requirements:

- `X-Jazz-Admin-Secret` required
- plus explicit inspector opt-in header (example: `X-Jazz-Inspector: 1`)

This avoids accidental policy bypass through generic endpoints.

## Auth and Safety Model

- Admin bypass is allowed only on explicit admin introspection routes (or equivalent internal API paths).
- Normal `/sync` and app query APIs keep current auth semantics.
- Inspector endpoints should be deny-by-default if admin auth is not configured.
- CORS for admin introspection routes should be more restrictive than current permissive defaults.

## Policy Visibility

We need policy data in inspector payloads.

Current blockers:

- WASM/JS schema export omits policies.
- catalogue schema encoding currently omits policies.

MVP approach:

- expose policy metadata directly from in-memory runtime/schema manager for introspection APIs.
- do not wait for catalogue format changes to ship local/server inspector read-only policy views.

Follow-up:

- version schema encoding to include policies for full syncable policy introspection.

## Multi-tier Awareness

Standalone inspector should support tier-aware views by:

- surfacing node tier (`worker`, `edge`, `core`) in responses
- surfacing settlement tier for live queries
- optionally comparing snapshots from multiple nodes to highlight divergence

Initial approach:

- one node per connection, with optional multi-node compare mode in UI.

## UX Scope (MVP)

- Schemas tab: schema hashes, branch names, current/live/pending status, lens links.
- Policies tab: table policies (SELECT/INSERT/UPDATE/DELETE, effective delete fallback).
- Data tab: table list, row grid, filters/sort/pagination, row details.
- Live Queries tab: active subscriptions, origin, table, branches, settled tier.
- Sync tab: connected clients/servers, outbox/inbox counters, recent acks/query-settled.

## Phasing

### Phase 1: Local DevTools Read-only

- add runtime inspector APIs in WASM/NAPI
- add JS hook registration for DevTools discovery
- implement extension panel (schema + data + live queries)

### Phase 2: Server Standalone Read-only

- add `/admin/introspection/*` routes
- add admin auth + inspector header gate
- implement standalone app against these endpoints

### Phase 3: Controlled Mutations

- enable inspector mutations through normal write paths + execution options
- keep audit metadata (`origin=inspector`)

### Phase 4: Policy/Schema Fidelity and Advanced Views

- include policies in encoded schema catalogue format
- improve multi-tier compare and sync diagnostics

## Testing Plan

- Unit tests for execution option propagation defaults and inspector overrides.
- Unit tests that hidden inspector subscriptions do not appear in default live-query listings.
- Auth tests for admin introspection route gating (missing/invalid headers).
- Integration tests that inspector query/mutation reuses existing code paths and behavior.
- End-to-end tests:
  - local DevTools runtime introspection
  - standalone admin introspection against server

## Open Questions

- Should inspector-origin writes be tagged in commit metadata for explicit audit trails?
- Should admin introspection have rate limits separate from app traffic?
- Do we need a dedicated streaming endpoint for live-query/sync diagnostics, or is polling sufficient for MVP?
- How much of sync-manager internal state should be exposed vs summarized?

## Immediate Next Steps

1. Add execution options plumbing with no behavior change under default options.
2. Add hidden/origin metadata to subscription state and live-query list API.
3. Add minimal server admin introspection endpoints for schemas, rows, and live queries.
4. Expose corresponding WASM/NAPI APIs for Chrome DevTools local mode.

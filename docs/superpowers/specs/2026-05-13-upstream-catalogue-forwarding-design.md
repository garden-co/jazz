# Upstream Catalogue Forwarding Design

## Context

Jazz currently has two overlapping concepts for multi-server deployments:

- `upstreamUrl` marks a server as an edge node and starts server-to-server sync with the upstream core.
- `catalogueAuthority*` separately decides whether catalogue HTTP requests are handled locally or forwarded to another authority URL.

That split makes catalogue routing more configurable than the prototype needs. It also creates an awkward state where an edge can sync to one upstream while forwarding catalogue requests somewhere else, or reject local catalogue writes even though the intended authority is already known through `upstreamUrl`.

## Goal

Make `upstreamUrl` the only source of truth for catalogue authority.

If a server has no `upstreamUrl`, it is the core and handles catalogue HTTP endpoints locally. If a server has `upstreamUrl`, it is an edge and forwards catalogue HTTP endpoints to that upstream.

## Non-Goals

- Preserve the old `catalogueAuthority*` configuration surface.
- Add a compatibility alias for `catalogueAuthorityUrl`.
- Change the catalogue sync lane itself.
- Change how client/runtime query paths use locally installed catalogue state.

This is a prototype, so removing the old configuration is preferable to carrying deprecated aliases.

## Architecture

`ServerBuilder` should derive two related pieces of topology from `upstreamUrl`:

- `ServerTopology::Core` or `ServerTopology::Edge`, as it does today.
- An optional HTTP catalogue upstream base URL for edge servers.

The HTTP catalogue upstream is derived from the same configured `upstreamUrl`:

- `http` stays `http`
- `https` stays `https`
- `ws` becomes `http`
- `wss` becomes `https`

The base path is preserved in the same spirit as `upstream_ws_url`, so an upstream such as `https://core.example.com/jazz` forwards catalogue requests under `https://core.example.com/jazz/apps/<appId>/...`.

## Configuration Surface

Remove these public options:

- CLI flags and env vars:
  - `--catalogue-authority`
  - `JAZZ_CATALOGUE_AUTHORITY`
  - `--catalogue-authority-url`
  - `JAZZ_CATALOGUE_AUTHORITY_URL`
  - `--catalogue-authority-admin-secret`
  - `JAZZ_CATALOGUE_AUTHORITY_ADMIN_SECRET`
- NAPI/TypeScript dev server options:
  - `catalogueAuthority`
  - `catalogueAuthorityUrl`
  - `catalogueAuthorityAdminSecret`
- Rust API:
  - `CatalogueAuthorityMode`
  - `ServerBuilder::with_catalogue_authority`

Keep `upstreamUrl`, `peerSecret`, and `adminSecret`.

## Auth

Edge servers validate `X-Jazz-Admin-Secret` locally before forwarding catalogue HTTP requests. This assumes the core and its edges are configured with the same `adminSecret`.

If local validation fails, the edge returns the existing unauthorized response and does not call the core. If local validation succeeds, the edge forwards the request to the upstream with the same `X-Jazz-Admin-Secret` header received from the caller.

In edge mode, `adminSecret` becomes required because catalogue forwarding needs local validation. `peerSecret` remains required for upstream sync.

## Route Behavior

Core mode stays local for all catalogue endpoints.

Edge mode forwards all catalogue HTTP endpoints to the upstream:

- `GET /apps/:appId/schemas`
- `GET /apps/:appId/schema/:hash`
- `GET /apps/:appId/admin/schema-connectivity`
- `GET /apps/:appId/admin/permissions/head`
- `GET /apps/:appId/admin/permissions`
- `POST /apps/:appId/admin/schemas`
- `POST /apps/:appId/admin/permissions`
- `POST /apps/:appId/admin/migrations`

The forwarding helper should preserve method, JSON body, content type for forwarded JSON bodies, and relevant response status/content type/body from the upstream response.

## Error Handling

Invalid upstream catalogue base URLs should fail server startup, matching the current startup-time validation style for invalid upstream WebSocket URLs.

If the upstream catalogue request cannot be reached, the edge should return `502 Bad Gateway` with an internal error body, matching the current forwarding behavior.

## Specs

Update the status-quo specs that currently say edge catalogue publish endpoints reject writes. The new status quo is:

- Core servers own catalogue authority.
- Edge servers forward catalogue HTTP GET and POST endpoints to their configured upstream.
- Edge servers still receive catalogue state through the sync lane for runtime/query use.
- Edge servers validate admin secret locally before forwarding, assuming shared admin secret across core and edges.

## Testing Strategy

Prefer route-level integration tests because this is externally visible HTTP behavior.

Tests should cover:

- An edge server with `upstreamUrl` forwards catalogue GET requests to the upstream.
- An edge server with `upstreamUrl` forwards catalogue POST requests to the upstream.
- Invalid admin secret on an edge is rejected locally and does not hit upstream.
- A core server without `upstreamUrl` continues to handle catalogue endpoints locally.
- Server startup with `upstreamUrl` and missing `adminSecret` fails with a clear error.
- CLI parsing no longer exposes `catalogueAuthority*`.
- TypeScript/NAPI dev server options no longer expose or pass through `catalogueAuthority*`.

Existing forwarding tests should be rewritten around `upstreamUrl`, not `CatalogueAuthorityMode`.

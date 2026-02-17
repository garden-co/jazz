# Multi-Tenant Sync Server (MVP) — TODO

Build a hosted sync server for Jazz2 first adopters that supports many app IDs on one deployment, with manual provisioning through an internal HTTP API.

This is a new server crate, separate from `jazz-cli`. No backwards compatibility with the single-tenant CLI server routes is required.

## Context

The current `jazz-cli` server is single-tenant:

- One fixed `app_id` at process start (`crates/jazz-cli/src/commands/server.rs`)
- One runtime/storage for that app
- Auth config is process-wide, not app-scoped

For hosted use, we need app-scoped configuration and isolation while keeping the MVP operationally simple.

## MVP Scope

- Single region, single process instance
- Multi-tenant by app ID (path-based routing)
- Manual app provisioning via internal API
- Per-app auth config:
  - `app_name`
  - `jwks_endpoint` (JWT verification)
  - `backend_secret`
  - `admin_secret`
- All app registry data stored in Jazz itself (meta app)
- Eventual consistency for app config propagation is acceptable
- Scale target: tens of apps, tens of concurrent clients per app

## Non-Goals (MVP)

- No dashboard/self-serve onboarding
- No multi-region or sharded data plane
- No compatibility layer for old `/sync` and `/events`
- No `iss`/`aud` claim enforcement yet (signature verification only)
- No automated billing, quotas, or rate limiting

## Top-Level Architecture

```
                         Internal Operators / Script / Future GUI
                                       |
                                /internal/apps/*
                                       |
                      +-------------------------------------------+
                      |          multi-tenant server crate        |
                      |-------------------------------------------|
Public clients  ----> | /apps/:app_id/events                     |
                      | /apps/:app_id/sync                       |
                      | /health                                   |
                      |                                           |
                      | AppRegistry (in-memory cache)             |
                      |  <- hydrated from meta app ->             |
                      |                                           |
                      | WorkerPool (fixed threads ~= CPU cores)   |
                      |   worker_0: app runtimes A, D, F...       |
                      |   worker_1: app runtimes B, C, E...       |
                      +---------------------+---------------------+
                                            |
                              +-----------------------------+
                              | Meta app runtime (Jazz)     |
                              | table: apps                 |
                              +-----------------------------+
```

### Isolation Model

Each app gets:

- Its own `RuntimeCore`/`TokioRuntime` instance
- Its own `SchemaManager::new_server(sync_manager, app_id, "prod")`
- Its own storage path on disk

This avoids unsafe cross-app mixing in one runtime and keeps behavior close to the existing single-app server.

## Execution Model and Fairness

Use one process with many app runtimes, scheduled explicitly by the server:

- No thread-per-app
- Fixed worker thread count (typically `num_cpus`)
- Sticky app placement: `app_id -> worker` (consistent hash)
- Each worker owns runtimes for its assigned apps and runs a fair local scheduler

### Scheduler shape

- Per worker: runnable queue of app IDs with pending work
- Fairness policy: round-robin / deficit round-robin across runnable apps
- Per app quantum uses bounded work (message/tick budget) then yields
- Requeue app if backlog remains

This keeps isolation per app while avoiding OS scheduler overload from huge thread/process counts.

### Tick budgeting direction

Current `RuntimeCore`/`QueryManager::process()` paths are mostly run-to-completion. The scheduler therefore starts with coarse fairness (app-level quanta) and then incrementally adds finer budgets where needed:

- Inbox processing budget
- Subscription settle budget
- Outbox flush/send budget

Optional future step: deadline-aware work splitting inside `immediate_tick` once the process phases are chunkable.

### Transport scaling note

Do not rely on a single broadcast channel with per-client filtering for large fanout. Use per-client routing queues (or sharded routing maps) so send work scales with addressed clients, not all connected clients.

## Tenant Routing

Public routes:

- `GET /apps/:app_id/events`
- `POST /apps/:app_id/sync`
- `GET /health`

Notes:

- Clients can keep existing transport behavior by setting `server_url` to include app path, e.g. `https://sync.example.com/apps/<app_id>`.
- Old root routes (`/events`, `/sync`) are not exposed by this crate.
- Meta app routes may be exposed at `GET /apps/:meta_app_id/events` and `POST /apps/:meta_app_id/sync` for an internal GUI/workflows.

## Meta App (App Registry Stored in Jazz)

### Why

App config must be stored in Jazz, not in a side database.

### Meta App Record Schema

`apps` table (logical schema):

- `app_id` (UUID string, immutable, primary key)
- `app_name` (string)
- `jwks_endpoint` (string URL)
- `backend_secret_hash` (string)
- `admin_secret_hash` (string)
- `status` (`active | disabled`)
- `created_at` (timestamp)
- `updated_at` (timestamp)

Secrets are generated/provided via internal API, returned once on creation/rotation, and stored as hashes (not plaintext).

### Bootstrap

The server starts with bootstrap config for the meta app itself:

- `META_APP_ID`
- `META_APP_DATA_DIR`
- `INTERNAL_API_SECRET`
- `SECRET_HASH_KEY` (for HMAC/derived secret hashes)

Then:

1. Start meta app runtime.
2. Load app rows from meta app into in-memory registry.
3. Start or update per-app runtimes from that registry.

### Reconciliation

- Write-through on local internal API mutations (apply registry changes immediately in current process)
- Background periodic full refresh from meta app (for resilience and future multi-instance support)
- Eventual consistency is acceptable in MVP

## Internal HTTP API (Manual Provisioning)

All internal endpoints require `X-Jazz-Internal-Secret: <secret>`.

### Create app

`POST /internal/apps`

Body:

```json
{
  "app_name": "Acme Notes",
  "jwks_endpoint": "https://auth.acme.com/.well-known/jwks.json",
  "backend_secret": "optional-generated-if-missing",
  "admin_secret": "optional-generated-if-missing"
}
```

Behavior:

- Generate immutable `app_id` (UUID)
- Generate secrets if omitted
- Store hashed secrets in meta app
- Create/start app runtime
- Return cleartext secrets once

Response:

```json
{
  "app_id": "uuid",
  "app_name": "Acme Notes",
  "jwks_endpoint": "https://auth.acme.com/.well-known/jwks.json",
  "backend_secret": "cleartext-once",
  "admin_secret": "cleartext-once",
  "status": "active"
}
```

### List apps

`GET /internal/apps`

Returns non-secret fields for all apps.

### Get app

`GET /internal/apps/:app_id`

Returns non-secret fields for one app.

### Update app

`PATCH /internal/apps/:app_id`

Supports:

- `app_name`
- `jwks_endpoint`
- `status` (`active` / `disabled`)
- `rotate_backend_secret` (bool)
- `rotate_admin_secret` (bool)

If rotation is requested, return new cleartext secret in that response only.

## Auth Behavior (Per-App)

Public app routes resolve app config from the path `:app_id`.

### Accepted auth modes

1. Backend impersonation:
   - `X-Jazz-Backend-Secret`
   - `X-Jazz-Session`
2. JWT:
   - `Authorization: Bearer <jwt>`
   - Validate signature using app `jwks_endpoint`
3. Admin auth:
   - `X-Jazz-Admin-Secret` for catalogue/admin operations

### JWKS validation

- Cache per-app JWKS keys with TTL
- On unknown `kid` or signature failure, force one refresh then retry verify
- Fail closed if verification still fails

### Loud TODO

MVP verifies JWT signatures only. Claim-level validation (`iss`, `aud`, tenant binding) is deferred and must be added before broader production usage.

## Runtime and Storage Layout

Example:

```
<data_root>/
  meta/
    groove.surrealkv
  apps/
    <app_id_1>/groove.surrealkv
    <app_id_2>/groove.surrealkv
```

Each app runtime can be lazily started on first request, or eagerly started from registry on boot. MVP should prefer eager start for simpler failure visibility.

## Operational Notes

- Single instance only for MVP
- Basic health checks:
  - `/health` (process up)
  - internal health may include meta app connectivity and app runtime counts
- Logging:
  - app_id-tagged request logs
  - auth failure reasons without secret material
- Worker/scheduler metrics:
  - runnable apps per worker
  - per-app queue depth
  - scheduler wait time / quantum utilization

## Rollout / Implementation Plan

1. Create new crate (working name: `jazz-multi-server`) in this repo.
2. Copy/reuse transport route behavior from `jazz-cli` and switch to app-path routing.
3. Implement worker pool (`N` threads), app-to-worker placement, and fair runnable queues.
4. Implement per-app runtime manager and storage layout.
5. Implement meta app runtime and `apps` schema.
6. Implement internal provisioning API and secret hashing.
7. Implement per-app JWT/JWKS validation with cache + refresh.
8. Replace broadcast-based event fanout with direct per-client routing.
9. Add integration/load tests for app isolation, fairness, and provisioning flows.
10. Deploy single instance with HTTPS and restricted internal API access.
11. Later: extract with `git filter-repo` into private repo before launch.

## Test Plan (MVP)

- Provision two apps (`A`, `B`) with different secrets/JWKS.
- Assert writes/subscriptions in `A` never appear in `B`.
- Assert JWT valid for `A` is rejected on `B`.
- Assert backend/admin secrets are app-scoped.
- Assert disable status blocks public sync/events.
- Assert secret rotation invalidates old secret.
- Assert app registry reload restores runtime map after process restart.

## Open Questions (Remaining)

- Secret hashing algorithm choice (`HMAC-SHA256` keyed hash vs password hash)
- Registry refresh interval and conflict behavior in future multi-instance deployments
- Fairness policy details (plain RR vs deficit RR, quantum sizing, priority lanes)

# Unified Auth Methods (Anonymous, Demo, External JWKS) — TODO

Unify `jazz-tools server` and `jazz-cloud-server` around three supported auth methods:

1. `anonymous` auth: one client-generated token per device by default
2. `demo` auth: multiple client-generated tokens per device to simulate multi-user flows
3. `external` auth: bearer JWT validated against a configured JWKS endpoint

This spec also defines upgrade from `anonymous`/`demo` principals to external identities without data loss.

## Why

Jazz1 had a beloved anonymous-first onboarding loop. Jazz2 should preserve that while keeping a production path to external auth. Today, auth behavior is split and inconsistent across server targets.

## Goals

- Anonymous-by-default app bootstrapping with no auth provider setup
- Demo multi-user simulation via token switching on one device
- Production external auth via JWKS-only JWT verification
- One auth model and resolver behavior across `jazz-tools` and `jazz-cloud-server`
- Seamless local principal upgrade to external identity
- Remove custom `jwt_secret` auth path from `jazz-tools`

## Non-Goals (MVP)

- No token minting service for anonymous/demo mode
- No full account merge workflow when two existing principals collide
- No policy language redesign
- No configurable principal-claim name beyond `jazz_principal_id` (defer)
- No automatic subscription/session rebinding on auth upgrades (defer)

## Core Terms

- `local token`: client-generated opaque string used for anonymous/demo identity
- `principal_id`: internal identity key used in `Session.user_id`
- `external identity`: provider-specific subject (`iss`, `sub`) from external JWT

## Supported Auth Methods

### 1) Anonymous auth

- Intended default for real users before auth integration
- Typical client behavior: generate one stable token and persist it per device/app
- Transport headers:
  - `X-Jazz-Local-Mode: anonymous`
  - `X-Jazz-Local-Token: <token>`

### 2) Demo auth

- Intended for development and product demos
- Same mechanism as anonymous, but caller may create/switch many local tokens
- Transport headers:
  - `X-Jazz-Local-Mode: demo`
  - `X-Jazz-Local-Token: <token>`

### 3) External auth (JWKS)

- JWT bearer token validated using app-configured JWKS endpoint
- Transport header:
  - `Authorization: Bearer <jwt>`

## Session Resolution Priority

Per request, resolve session in this order:

1. Backend impersonation (`X-Jazz-Backend-Secret` + `X-Jazz-Session`)
2. External JWT (`Authorization` + JWKS)
3. Local token (`X-Jazz-Local-Mode` + `X-Jazz-Local-Token`)
4. No session

If an endpoint requires session and none resolves, return `401`.

## Principal Model

Always execute ReBAC and ownership policies against a stable `principal_id`.

### Local token principal mapping

For `anonymous` and `demo`, derive a deterministic principal from token:

- `principal_id = local:<base64url(sha256(app_id || ":" || mode || ":" || token))>`

Rationale:

- No server-side token minting required
- No plain token storage required for read path
- Stable identity for same token across sessions/devices

### External principal mapping

Preferred path:

- Read `jazz_principal_id` claim from external JWT and set `Session.user_id` to that value

Fallback path:

- Resolve via server mapping table keyed by provider identity (`issuer`, `subject`) to `principal_id`

The fallback is required for:

- claim-template misconfigurations
- migrations from older deployments
- recovery from auth provider metadata drift

## Upgrade Flow (Anonymous/Demo -> External)

### Endpoint

- `POST /auth/link-external` (or app-scoped equivalent)

### Required inputs

- valid external JWT
- local mode + local token headers

### Behavior

1. Resolve local principal from token
2. Resolve external identity from JWT
3. If external identity unlinked: link to local principal
4. If already linked to same principal: idempotent success
5. If linked to different principal: return `409` conflict

### Result

- Subsequent external sessions resolve to the same `principal_id`
- Existing data remains visible because `Session.user_id` does not change

## Server Configuration Model

Per app config stores:

- `external_jwks_endpoint: Option<String>`
- `allow_anonymous: bool`
- `allow_demo: bool`
- existing admin/backend secret hashes

Defaults:

- `allow_anonymous = true`
- `allow_demo = true`
- `external_jwks_endpoint = None`

If JWKS is unset, external auth is disabled for that app.

## Client/SDK Model

Add explicit local auth config in `AppContext`/`DbConfig`:

- `localAuthMode?: "anonymous" | "demo"`
- `localAuthToken?: string`

Transport behavior:

- If local auth configured, send local auth headers on `/sync` and `/events`
- If JWT configured, send bearer token
- Header precedence should match server resolver priority where applicable

Worker bridge auth updates should update the full auth state, not JWT-only.

## Runtime/Sync Implications

Session upgrades must affect active query subscriptions.

Requirement:

- server-side `QuerySubscription` handling should use the currently bound client session for user clients, not blindly trust stale session payloads
- when a client session changes, server subscriptions for that client must be re-evaluated

Status: deferred post-MVP.

## Jazz Tools / Cloud Server Unification

- Keep one auth method matrix and one resolution order
- Remove `jwt_secret` path from `jazz-tools server` CLI and middleware
- Use JWKS-based external auth in both servers
- Keep backend/admin secret behavior aligned across both servers

## Migration Plan

1. Add local auth headers + principal derivation in both servers
2. Add per-app flags (`allow_anonymous`, `allow_demo`, `external_jwks_endpoint`) in cloud metadata
3. Add local auth config and header emission in TS runtime + worker
4. Remove `jwt_secret` support from `jazz-tools`
5. Add link endpoint and mapping table for external upgrades
6. Add query-session rebinding for subscription correctness

## Test Plan

- Anonymous token can connect, sync, reconnect, and retain identity
- Demo mode can switch tokens and produce isolated identities
- External JWT validates via JWKS and rejects invalid tokens
- Endpoint gating by allowed methods behaves correctly
- Link flow:
  - new link succeeds
  - repeat link idempotent
  - conflicting link returns `409`
- After link, old data remains accessible under external auth
- Query subscriptions re-scope after session/auth upgrade

## Open Questions

- Should linking be a dedicated endpoint or implicit when both auth forms appear together?
- Should anonymous and demo use separate per-app quotas/rate limits?
- Should we expose principal inspection/debug endpoints for developer tooling?
- Should principal claim name be configurable per app/provider (instead of fixed `jazz_principal_id`)?

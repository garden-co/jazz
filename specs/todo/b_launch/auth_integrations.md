# Jazz Auth (Hosted + Better Auth Compatible) - TODO

Built-in auth product for Jazz apps, implemented as a Node.js sidecar around Better Auth and exposed through a Jazz-owned SDK and hosted UI.

## Overview

Jazz should offer a zero-config auth experience that feels built in:

- browser apps start anonymous by default
- developers enable hosted auth for their app without wiring their own auth stack first
- app code calls a tiny Jazz auth API (`signIn`, `signUp`, `signOut`, `completeRedirect`)
- Jazz hosts the credential UI and returns a JWT that the sync server already knows how to validate via JWKS

Under the hood, auth is not implemented inside the sync server itself. Instead, Jazz runs an auth sidecar process in Node.js using Better Auth plus Jazz-specific adapter/hooks/JWT configuration.

This sidecar is the default hosted auth implementation and also defines a "self-host compatible Better Auth" escape hatch:

- developers can later paste a generated Better Auth config into their own Next app, Hono server, or separate auth service
- that self-hosted Better Auth instance uses the same Jazz adapter, schema, plugin set, principal mapping, and JWT/JWKS profile
- hosted Jazz Auth and self-hosted compatible Better Auth should be able to operate concurrently against the same auth data

The default Jazz developer experience should not mention Better Auth at all. Better Auth is an implementation detail until the developer explicitly chooses to self-host the compatible stack.

## Why

We want three properties at once:

1. Jazz keeps its beloved anonymous-first onboarding loop
2. production apps get a polished built-in auth story instead of stitching together a separate auth product
3. developers who outgrow the hosted UX can drop down to a compatible self-hosted auth stack without changing their data model

This design keeps Jazz core narrow:

- the sync server remains a JWT/JWKS consumer
- auth account/session/linking behavior lives in the auth sidecar
- app schemas do not need to include core auth tables

## Goals

- Zero-config hosted auth for Jazz apps
- Redirect-first auth flow with popup support
- Anonymous-by-default browser startup
- Hosted login/signup UI branded as Jazz, not Better Auth
- App-owned pre-click UI and layout
- Stable `jazz_principal_id` JWT claim for sync server auth
- Self-host compatible Better Auth mode as an explicit escape hatch
- Hosted Jazz Auth and compatible self-hosted Better Auth can run concurrently against the same auth data
- No auth tables required in the app's own schema
- Principal changes handled with full reload instead of live session rebinding

## Non-Goals (MVP)

- No iframe auth flow
- No requirement that hosted and self-hosted deployments share browser cookies across different origins
- No arbitrary Better Auth configuration compatibility; only Jazz-compatible Better Auth is in scope
- No automatic merge of an existing remote account with unrelated anonymous local data
- No enterprise SSO control plane in the first cut; WorkOS and similar providers are follow-up work
- No auth implementation inside `jazz-tools` / `jazz-cloud-server` process itself

## Product Modes

### 1. Hosted Jazz Auth (default)

Jazz runs the auth sidecar and hosted UI for the app.

Developer experience:

- enable auth in dashboard / app config
- call Jazz auth helpers from the client
- optionally override copy, button design, provider chooser, and email input in the app

The app is unaware that Better Auth is used underneath.

### 2. Self-Host Compatible Better Auth (escape hatch)

Jazz provides a generated Better Auth config snippet plus env values for developers who want to own routes/UI/server deployment.

Compatibility means:

- same auth tables and adapter behavior
- same Jazz principal mapping hooks
- same JWT/JWKS profile
- same Better Auth major/minor compatibility target and plugin allowlist

This mode is intended for:

- custom UI in Next or another full-stack framework
- separate auth services owned by the developer
- gradual migration away from Jazz-hosted auth without rewriting identity data

### 3. Ejected / arbitrary Better Auth

Developers may customize Better Auth beyond the Jazz-compatible profile, but once they do, seamless coexistence with hosted Jazz Auth is no longer guaranteed.

Jazz should describe this clearly as an unsupported compatibility boundary, not a bug.

## High-Level Architecture

### Auth sidecar

A Node.js service that:

- mounts Better Auth at an internal base path
- serves Jazz-owned hosted auth pages
- stores auth data through the Jazz Better Auth adapter
- mints JWTs for Jazz sync access
- exposes a JWKS endpoint for sync server validation
- owns principal mapping and auth intent state

### Sync server

The sync server does not run Better Auth. It only:

- validates JWTs against the configured JWKS endpoint
- reads `jazz_principal_id` and standard claims
- turns that into `Session.user_id` / `Session.claims`

This keeps the current `external` JWT auth path intact and makes Jazz Auth an auth provider layered on top of it.

### Hosted deployment boundary

The sidecar is a separate process, but hosted Jazz should present it as one product surface via routing/proxying. From the app's point of view, auth should feel integrated with the Jazz server product, even if the implementation is split.

## Better Auth Compatibility Contract

Hosted Jazz Auth and self-hosted compatible Better Auth only interoperate if they use the same Jazz compatibility profile.

That profile includes:

- pinned Better Auth version range
- pinned plugin allowlist
- Jazz Better Auth adapter configuration
- Jazz principal mapping hook/plugin
- JWT/JWKS plugin configuration
- account-to-principal claim mapping (`jazz_principal_id`)
- route and callback semantics expected by the Jazz SDK

This should be expressed as code, not documentation alone. Provide a package or generated snippet built around a shared helper, for example:

```ts
import { betterAuth } from "better-auth";
import {
  jazzAuthAdapter,
  jazzCompatiblePlugins,
  jazzCompatibleHooks,
} from "@jazz/auth/better-auth";

export const auth = betterAuth({
  baseURL: process.env.AUTH_BASE_URL!,
  basePath: "/api/auth",
  secret: process.env.JAZZ_AUTH_SECRET!,
  database: jazzAuthAdapter({
    appId: process.env.JAZZ_APP_ID!,
    serverUrl: process.env.JAZZ_SERVER_URL!,
    adminSecret: process.env.JAZZ_ADMIN_SECRET!,
  }),
  plugins: jazzCompatiblePlugins(),
  hooks: jazzCompatibleHooks(),
});
```

The generated config should still be recognizable Better Auth config so developers can continue using Better Auth client/server APIs directly.

## Auth Data Ownership

Jazz should not require developers to add auth tables to their application schema.

Instead:

- Better Auth core tables live in a system-owned auth schema / namespace via the Jazz adapter
- Jazz sidecar adds only the extra records it needs for compatibility
- app-level tables remain focused on application data

Minimum Jazz-owned auth data beyond Better Auth core tables:

- account/user to `jazz_principal_id` mapping
- short-lived auth intents
- optional compatibility metadata / versioning

Later we can expose selected auth records to apps through generated read-only helpers, but that is not required for MVP.

## Principal Model

### Anonymous local principals remain the bootstrap path

Browser clients continue to default to anonymous local auth as described in `unified_auth_methods.md`.

Local principal derivation stays the same:

- `local:<base64url(sha256(app_id || ":" || mode || ":" || token))>`

### Auth sidecar owns account-to-principal mapping

Jazz Auth should move anonymous-to-account linking out of Jazz core and into the auth sidecar.

The sidecar owns the decision:

- when a new auth account should adopt the current anonymous principal
- when an auth account should keep its existing principal
- when a conflict requires the current anonymous data to remain separate

### JWT claim contract

Every JWT issued for sync access must include:

- `iss`
- `sub`
- `jazz_principal_id`
- optional `claims`

The sync server should continue to treat `jazz_principal_id` as the preferred `Session.user_id`.

## Auth Intent Flow

Jazz Auth needs a first-class auth intent so the app can own pre-click UI while the sidecar owns the sensitive credential step.

### Why

The intent captures:

- app id
- desired screen (`sign-in` or `sign-up`)
- selected provider / auth method
- optional `loginHint` (for example prefilled email)
- callback URL
- popup vs redirect mode
- current local principal context, if present

This keeps the actual hosted auth step small and lets the app render its own buttons, email field, and surrounding layout before handing off.

### Start flow

1. App renders its own CTA / provider picker / optional email field
2. App calls Jazz SDK helper
3. SDK creates an auth intent with the sidecar
4. Sidecar returns a signed short-lived intent reference / URL
5. SDK redirects or opens popup to hosted Jazz auth page

### Finish flow

1. Hosted page completes auth through Better Auth
2. Sidecar resolves or creates the Jazz principal for that account
3. Sidecar returns a one-time code to the app callback, not a raw JWT in the URL
4. App calls `completeRedirect()` or popup completion helper
5. Sidecar exchanges the code for a Jazz sync JWT and auth metadata
6. SDK stores the new JWT and reloads the app

## Anonymous-to-Account Behavior

### New account from anonymous session

If signup begins from an anonymous Jazz client and the target account does not yet have a Jazz principal, the sidecar should adopt the current anonymous principal for that new account.

Result:

- no data migration is required
- the issued JWT uses the same `jazz_principal_id`
- the SDK still reloads for simplicity and consistency

### Existing account sign-in

If the user signs into an existing account, the sidecar should issue that account's existing `jazz_principal_id`.

If it differs from the current anonymous principal:

- the SDK reloads into the authenticated principal
- the current anonymous data remains separate
- Jazz does not attempt implicit account merge

Explicit merge / claim flows can be added later as product work, but they are not part of MVP.

## Client SDK Surface

The primary Jazz auth API should be tiny and headless:

- `signIn(options?)`
- `signUp(options?)`
- `signOut(options?)`
- `completeRedirect()`
- `getSignInUrl(options?)`
- `openSignInPopup(options?)`

Options may include:

- `provider`
- `loginHint`
- `redirectTo`
- `screen`

Framework wrappers can expose hooks/components on top of this, but the contract should remain plain runtime primitives first.

### Anonymous bootstrap API

Do not make `becomeAnonUser()` a required browser API in MVP.

Browser Jazz clients already know how to bootstrap anonymous local auth. Requiring an explicit callback would add ceremony without buying us anything.

If we need an explicit helper later for non-browser clients or testing, add `ensureAnonymous()` as a thin utility instead of making it the primary path.

## UI Ownership

### What the app owns

- button design
- surrounding layout
- copy and marketing context
- provider choice UI
- optional inline email input

### What hosted Jazz Auth owns

- credential collection / validation screen
- OAuth redirects and callbacks
- password reset / verification flows
- issuance of Jazz-compatible JWTs

This gives apps inlinability before the auth boundary without embedding the sensitive auth step itself.

## Redirect and Popup

### Redirect is the default

It is the simplest flow for:

- mobile web
- strict browser privacy settings
- OAuth providers that dislike popup blockers and iframe embedding

### Popup is optional

Popup support is useful for desktop web apps that want less navigation churn, but it should reuse the same auth intent and completion flow as redirect mode.

### No iframe

Do not support iframe login/signup UIs.

Reasons:

- third-party cookie restrictions
- storage partitioning
- provider frame restrictions
- more complex clickjacking and `postMessage` surface
- worse debugging and product ergonomics

## Principal Changes and Reloads

Jazz runtime currently does not support changing principals on a live client without recreating the DB/session graph.

MVP behavior:

- after successful sign-in completion, reload the app
- after sign-out, reload the app
- reload even when the principal remains the same, if that keeps the implementation simpler

This is acceptable in MVP and lets us avoid threading auth upgrades through live subscriptions before we have proper rebinding semantics.

## Better Auth Feature Allowlist

Given Better Auth's fast release cadence and bug/security churn, Jazz should deliberately keep the compatibility profile narrow.

Initial allowlist:

- email/password
- email one-time code or magic link
- OAuth / OIDC providers
- JWT/JWKS support

Deferred:

- API keys
- organizations / active org switching
- enterprise SSO management UI
- broad plugin surface area not needed for Jazz Auth launch

## Better Auth Risk Mitigation

Better Auth is active, but also high-churn. Jazz should adopt it with guardrails:

- pin Better Auth to an explicit compatibility version range
- expose only a small audited plugin set in Jazz-compatible mode
- maintain Jazz-owned integration tests around redirect, popup, signup, sign-in, sign-out, password reset, and JWT/JWKS behavior
- keep Jazz SDK and hosted UI thin wrappers over a shared compatibility profile
- be ready to upgrade quickly when Better Auth ships security fixes

The compatibility contract should let us upgrade hosted Jazz Auth centrally while still telling self-hosters exactly which version/config profile they need to match.

## Security

- Never place long-lived JWTs directly in redirect URLs
- Use one-time code exchange on redirect/popup completion
- Use `state` and PKCE-style anti-replay protections for provider flows
- Keep sync JWTs short-lived
- Prefer refresh through sidecar session state rather than issuing long-lived bearer tokens
- Treat popup completion as same-origin / explicit-origin `postMessage` only
- Keep Better Auth CSRF and origin protections enabled in compatible mode
- Distinguish between identity compatibility and cookie/session sharing across origins

## Hosted Routes

Suggested externally visible routes:

- `/auth/sign-in`
- `/auth/sign-up`
- `/auth/callback`
- `/auth/popup-complete`
- `/auth/sign-out`
- `/.well-known/jwks.json`

Suggested internal Better Auth mount:

- `/api/auth/*`

Hosted Jazz routes should remain stable even if internal Better Auth routes evolve.

## Self-Host Compatible Better Auth

The escape hatch should be explicit in the Jazz product:

- dashboard action or CLI command generates a Jazz-compatible Better Auth config snippet
- generated env vars include the values needed for principal mapping and JWT/JWKS compatibility
- developers can mount the config in Next, Hono, Express, or another Node environment

Supported promise:

- hosted Jazz Auth and self-hosted compatible Better Auth can read/write the same auth data
- both can mint Jazz-compatible JWTs for the sync server
- developers may use Better Auth client/server SDKs directly once they self-host

Unsupported promise:

- arbitrary Better Auth plugin mixes remain compatible forever
- different origins automatically share browser session cookies

## Dashboard / Control Plane Impact

The developer dashboard should eventually manage:

- hosted auth enabled/disabled
- allowed auth methods
- provider credentials
- generated self-host compatibility snippet
- Better Auth compatibility version
- JWKS endpoint shown to self-hosted sync deployments if needed

This is the control plane layer on top of the sidecar, not part of the sync server itself.

## Relationship to Existing Jazz Auth Paths

This spec does not remove generic external JWT auth.

Instead:

- generic external JWT auth remains the primitive supported by sync servers
- Jazz Auth becomes one concrete JWT-issuing auth provider for that primitive
- existing local auth bootstrap stays in place

Over time, hosted Jazz Auth may replace some direct `link-external` usage for first-party Jazz apps, but the lower-level JWT/JWKS path should remain available.

## Phased Rollout

1. Keep current anonymous/demo/external JWT support as-is
2. Add Node auth sidecar with Better Auth, Jazz adapter, and JWKS endpoint
3. Add Jazz principal mapping and auth intent flow
4. Ship hosted Jazz auth pages with redirect completion
5. Add popup flow
6. Add generated self-host compatible Better Auth config
7. Add provider management in dashboard

## Test Plan

- Hosted signup from anonymous client adopts the anonymous principal
- Hosted sign-in to existing account returns that account's principal and reloads cleanly
- Redirect completion exchanges one-time code for Jazz JWT without putting the JWT in the URL
- Popup completion works with strict origin checks
- Sync server accepts hosted-sidecar JWTs via JWKS
- Self-hosted compatible Better Auth mints JWTs accepted by the same sync server
- Hosted and self-hosted compatible Better Auth can operate concurrently against the same auth data
- Sign-out reloads and returns to anonymous bootstrap state
- Generated compatibility config stays in lockstep with hosted profile

## Open Questions

- Should compatible self-host mode require the exact same Better Auth patch version, or a tested semver range?
- Should sync JWT issuance be handled directly by Better Auth's JWT plugin, or by a Jazz wrapper endpoint layered on top of Better Auth session state?
- How much of Better Auth's session cookie model should we preserve versus treating it as an implementation detail behind Jazz APIs?
- Should we expose read-only auth/user helpers in generated Jazz app clients later?
- Should passwordless email be the default hosted option, with password auth opt-in?

# Jazz Cloud CLI: login and dashboard parity

Status: draft. The Cloud side lives in `garden-co/infra`
(`services/cloud/packages/dashboard/src/app/api/v1`). Nothing here is deployed
until the dashboard ships that API and WorkOS CLI Auth is enabled for the
dashboard's AuthKit client.

## Why

Before this change, automation had one kind of credential for Jazz Cloud: the
per-app admin secret. It is long-lived, root for the app's sync server, stored
in plaintext `.env` files, and the only things it could do outside the sync
server were change auth settings, read rollout status and apply upgrades. Every
other dashboard action (teams, creating, claiming or deleting apps, rotating
secrets) needed a browser. Agents worked around this by discovering the admin
API with the admin secret.

The CLI should be the main, agent-friendly way to manage apps, with the same
powers as the dashboard and without handing agents a root secret.

## Parity audit (dashboard vs CLI)

| Dashboard action                                                 | Who                        | CLI before                                   | CLI now                                                                                |
| ---------------------------------------------------------------- | -------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------- | ------------ |
| Sign in / out, switch team                                       | user                       | none                                         | `login`, `logout`, `whoami`                                                            |
| List teams                                                       | member                     | none                                         | `teams list`                                                                           |
| List apps                                                        | member                     | none                                         | `apps list [--team]`                                                                   |
| Create app (name, region)                                        | admin                      | none (`create-jazz` makes _unclaimed_ apps)  | `apps create <name> [--team] [--region]`                                               |
| Claim unclaimed app                                              | admin                      | none                                         | `apps claim <appId> --name <n> [--admin-secret]`                                       |
| View app settings                                                | member                     | none                                         | `apps get <appId>`                                                                     |
| Auth settings (JWKS / public key, issuer, audience, local-first) | admin                      | admin-secret `PATCH /api/apps/:id/auth` only | `apps auth get                                                                         | set <appId>` |
| Rotate admin + backend secrets                                   | admin                      | none                                         | `apps secrets rotate <appId>`                                                          |
| Delete app (type name to confirm)                                | admin                      | unclaimed apps only, with admin secret       | `apps delete <appId> --confirm <name>`                                                 |
| Rollout status                                                   | member                     | admin-secret `GET /api/apps/:id/status`      | `apps status <appId> [--wait]`                                                         |
| Stable-release upgrades                                          | read: member, apply: admin | admin-secret `/api/apps/:id/upgrade`         | `apps upgrade <appId> [--release … --expected-version … --acknowledge-client-upgrade]` |
| Deploy schema / permissions / migrations                         | admin secret               | `deploy` with admin secret                   | unchanged in this stage (see _Stage 2_)                                                |
| Members: list, invite, promote/demote, remove, invitations       | admin                      | none                                         | not yet                                                                                |
| Logs (Loki), insights (Prometheus)                               | member                     | none                                         | not yet                                                                                |
| Billing, payment methods, coupons                                | admin                      | none                                         | intentionally dashboard-only                                                           |
| Open Inspector                                                   | member                     | `inspect` (self-hosted, in #3157)            | Cloud path in #3157                                                                    |

"Who" is the WorkOS organization role the dashboard requires. The CLI API
enforces the same roles because it calls the same `lib/team.ts` checks.

## Login

`jazz-tools login` runs the OAuth 2.0 device authorization grant (RFC 8628)
through **WorkOS CLI Auth**, using the dashboard's existing AuthKit client:

1. `GET <cloud>/api/v1/cli/config` returns the public WorkOS client ID and API
   base URL. Nothing about the flow is compiled into the CLI, so staging and
   local dashboards work with `--cloud-url` / `JAZZ_CLOUD_URL`.
2. `POST <workos>/user_management/authorize/device` with `client_id` returns a
   device code, a user code and a verification URL. The CLI prints the URL and
   code and opens the browser (`--no-browser` to skip). With `--json` it prints
   a `login_pending` JSON line first, so an agent can hand the URL to a human
   while the command keeps waiting.
3. The CLI polls `POST <workos>/user_management/authenticate` with
   `grant_type=urn:ietf:params:oauth:grant-type:device_code` at the given
   interval. It handles `authorization_pending`, backs off 5 s on `slow_down`,
   and stops on `access_denied` or `expired_token`.
4. On success WorkOS returns a user, a short-lived access token (a JWT signed
   by the client's JWKS; AuthKit's default lifetime is 5 minutes) and a
   rotating refresh token.

No client secret is involved, and the device code is never printed.

### Stored session

`~/.config/jazz/credentials.json` (or `$XDG_CONFIG_HOME/jazz`, `%APPDATA%\jazz`,
or `$JAZZ_CONFIG_DIR`) is written with an atomic rename, file mode `0600`,
directory mode `0700`. Format, version 1:

```json
{
  "version": 1,
  "profiles": {
    "https://v2.dashboard.jazz.tools": {
      "cloudUrl": "https://v2.dashboard.jazz.tools",
      "workos": { "clientId": "client_…", "apiBaseUrl": "https://api.workos.com" },
      "accessToken": "<jwt>",
      "accessTokenExpiresAt": 1790000000,
      "refreshToken": "<opaque>",
      "user": { "id": "user_…", "email": "ada@example.com" },
      "createdAt": "2026-09-28T17:00:00.000Z"
    }
  }
}
```

Profiles are keyed by dashboard origin. Any other `version` is rejected with an
instruction to log in again, rather than guessed at. No admin or backend
secret is ever written here.

The access token is refreshed when it is within 30 seconds of expiry, and once
more if the API answers 401. Refresh tokens rotate. If a refresh fails because
a concurrent `jazz-tools` process already rotated the token, the CLI uses the
token that process stored. If WorkOS rejects the refresh token, the profile is
deleted and the user is told to log in again.

`JAZZ_CLOUD_TOKEN` overrides the stored login with a bearer the caller already
holds. It is never refreshed.

The refresh token is the long-lived part of the login. It lasts as long as the
WorkOS session is allowed to (configured in WorkOS). A leaked credentials file
is equivalent to a leaked dashboard session: it can do what the user can do in
the dashboard, but not act as the app's root on the sync server. `logout`
deletes it locally. Server-side session revocation is still done in WorkOS or
by signing out everywhere.

## CLI API (`/api/v1` on the dashboard)

Every route except `cli/config` and `regions` requires
`Authorization: Bearer <WorkOS access token>`. The dashboard:

- verifies the RS256 signature against
  `https://api.workos.com/sso/jwks/<WORKOS_CLIENT_ID>` (cached), and checks the
  issuer (`https://api.workos.com/user_management/<client>`, overridable with
  `WORKOS_JWT_ISSUER`), expiry and a `user_…` subject;
- never reads cookies on these routes, so they are not exposed to CSRF;
- resolves the owning team of an app from the caller's own active
  memberships. A team ID is never trusted from the caller for app routes;
- reuses the dashboard's `lib/team.ts` checks, so members can read and only
  team admins can mutate;
- answers `Cache-Control: no-store` JSON. Errors have the shape
  `{ "error": <code>, "message": <text> }`, where the codes are
  `unauthenticated`, `forbidden`, `not_found`, `invalid_request`, `conflict`,
  `rate_limited`, `upstream_unavailable` and `internal`. Token material is
  never echoed.

| Method       | Path                                 | Notes                                                                 |
| ------------ | ------------------------------------ | --------------------------------------------------------------------- |
| GET          | `/api/v1/cli/config`                 | public; `{ apiVersion, workos: { clientId, apiBaseUrl } }`            |
| GET          | `/api/v1/regions`                    | public; core regions for `apps create`                                |
| GET          | `/api/v1/me`                         | user and teams                                                        |
| GET          | `/api/v1/teams`                      | teams with role and app count                                         |
| GET / POST   | `/api/v1/teams/:teamId/apps`         | list; create (admin) returns secrets once                             |
| POST         | `/api/v1/teams/:teamId/apps/claim`   | `{ appId, adminSecret, name }` (admin)                                |
| GET / DELETE | `/api/v1/apps/:appId`                | details without secrets; delete needs `{ confirmName }` (admin)       |
| GET / PATCH  | `/api/v1/apps/:appId/auth`           | partial update; merged result must pass the dashboard's rules (admin) |
| POST         | `/api/v1/apps/:appId/secrets/rotate` | returns new secrets once (admin)                                      |
| GET          | `/api/v1/apps/:appId/status`         | tenant manager now also accepts `{ teamId }` for status               |
| GET / POST   | `/api/v1/apps/:appId/upgrade`        | apply requires `acknowledgeClientUpgrade: true` (admin)               |

The existing admin-secret routes under `/api/apps/*` are unchanged.

### Example

```sh
$ jazz-tools login
To sign in to https://v2.dashboard.jazz.tools, open:

  https://…/device?user_code=RRGQ-BJVS

and confirm the code RRGQ-BJVS. Waiting...
Logged in to https://v2.dashboard.jazz.tools as ada@example.com.

$ jazz-tools apps create "Todo" --region eu-central-1 --json
{"appId":"…","teamId":"org_…","name":"Todo","region":"eu-central-1","adminSecret":"…","backendSecret":"…"}

$ jazz-tools apps auth set "$APP" --jwks-url https://auth.example.com/.well-known/jwks.json \
    --jwt-issuer https://auth.example.com --jwt-audience todo --allow-local-first-auth false
$ jazz-tools apps status "$APP" --wait
```

Edge cases:

- The user belongs to several teams and runs `apps create` without
  `--team`. The command exits 2 and lists the team IDs. Nothing is created.
- `apps delete` without `--confirm`, or with the wrong name, is refused before
  anything is deleted (client-side, then server-side).
- A member (not admin) runs `apps secrets rotate`. The API returns 403
  `forbidden` and nothing changes.
- The access token expires between commands. The next command refreshes it
  transparently.
- The refresh token was revoked. The command fails with `session_expired` and
  the stored profile is removed.

## Stage 2: deploy without the root secret

`jazz-tools deploy` and `migrations graph` still need the app's admin secret,
because the sync server only admits it. #3157 adds short-lived, app-scoped
**Inspector sessions** on the sync server: `POST /apps/:id/admin/inspector/sessions`,
authenticated by the admin secret, mints an HS256 token of at most 900 s with
capabilities `inspector:read` / `edit` / `admin`, where `admin` covers catalogue
publication.

The plan once #3157 (and #3651's single `/deploy` endpoint) lands:

1. Tenant manager gains `POST /internal/apps/:id/sessions { teamId, operator,
capabilities }`. It looks up the stored admin secret, calls the tenant's
   exchange endpoint server to server, and returns
   `{ accessToken, expiresAt, capabilities, serverUrl }`. The same endpoint
   backs #3157's `/inspector/token` and `/inspector/renew`.
2. The dashboard exposes `POST /api/v1/apps/:appId/sessions`. It requires team
   admin for `inspector:admin` or `edit` and membership for `read`, and uses the
   WorkOS user ID as the opaque operator.
3. When no admin secret is configured but a Cloud login exists, `deploy` and
   `migrations graph` fetch a session with `inspector:admin` and send
   `X-Jazz-Inspector-Token` instead of `X-Jazz-Admin-Secret`. The deploy
   endpoint must accept Inspector admission with `inspector:admin`.
   An explicit `--admin-secret` / `JAZZ_ADMIN_SECRET` keeps working and wins.

With that, an agent never holds the root secret. It holds a refreshable user
session plus 15-minute app tokens scoped to what the user may do.

## Rollout

1. In WorkOS, enable CLI Auth (device authorization) for the dashboard's
   AuthKit client in each environment.
2. Deploy tenant manager (status by `teamId`), then the dashboard. The API is
   additive; nothing existing changes behaviour.
3. Release the CLI. Older dashboards answer 404 on `/api/v1/cli/config`, which
   the CLI reports as "does not support CLI login yet".

## Open questions

- Should `login` also offer a loopback-redirect (authorization code + PKCE)
  flow for desktops, or is the device flow enough?
- How long should WorkOS sessions (and therefore stored refresh tokens) live
  for CLI logins, and should they differ from browser sessions?
- Members, logs and metrics in the CLI API: same shape, next stage.

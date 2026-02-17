# WorkOS Platform Provisioning for Jazz Apps — Open Questions

Integrate Jazz multi-tenant app provisioning with WorkOS Platform API so each Jazz app gets:

- a WorkOS team/org container
- a WorkOS environment
- an automatically configured `jwks_endpoint` in Jazz app config

This note captures the unresolved product/implementation questions before coding.

## Current Context

- We already provision Jazz apps through internal API (`POST /internal/apps`) and store app config in the meta app.
- Jazz app config currently includes:
  - `app_name`
  - `jwks_endpoint`
  - `backend_secret`
  - `admin_secret`
- WorkOS Platform API doc (provisional) provides:
  - OAuth client-credentials token endpoint
  - create team (`/platform/teams`)
  - create environment (`/platform/teams/:team_id/environments`)
  - create API key (`/platform/teams/:team_id/environments/:environment_id/api_keys`)
  - invite team member (`/platform/teams/:team_id/invitations`)

## Proposed MVP Flow (Subject to Answers)

1. Create Jazz app via internal provisioning endpoint.
2. Provision WorkOS resources for that app.
3. Derive/set `jwks_endpoint` from the new WorkOS environment.
4. Persist WorkOS linkage fields in app registry.

## Clarifying Questions

1. Should WorkOS provisioning run inline in `POST /internal/apps` (blocking), or async with `provisioning_status`?
2. Since WorkOS team creation requires `admin_email`, should `admin_email` become required in Jazz app provisioning payload?
3. Team/environment naming:
   - team name = `app_name`?
   - environment name fixed (`staging`) or derived from app metadata?
4. Environment count for MVP:
   - one env per app (`staging`)?
   - or both `staging` and `production`?
5. Do we need to create/store WorkOS environment API keys now, or only team+environment+JWKS?
6. What exact JWKS URL format should be written to `jwks_endpoint`?
   - Is it derivable from returned `client_id`?
   - If yes, what template should we use?
7. Failure handling: if team creation succeeds but env/JWKS step fails, do we rollback WorkOS resources or keep-and-mark-failed for retry?
8. How should we handle team creation conflict (`409 user_already_exists`)?
   - fail provisioning
   - or support linking/reuse flow
9. Should Jazz app disable/delete also trigger WorkOS cleanup (team/env), or is cleanup out of MVP scope?
10. Configuration assumptions:
    - can server load WorkOS platform credentials from env vars?
    - can access tokens be cached in-memory until expiry (1 hour)?

## Likely Data Model Additions (Pending Decisions)

Potential app registry fields to add if we proceed:

- `workos_team_id`
- `workos_environment_id`
- `workos_environment_name`
- `workos_provisioning_status` (`pending | ready | failed`)
- `workos_last_error` (nullable)

These are placeholders, not final schema decisions.

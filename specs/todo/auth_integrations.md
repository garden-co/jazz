# BetterAuth & WorkOS Integration — TODO

Integrate with external auth providers for production-grade authentication.

## Overview

The current auth is JWT-based with a shared secret. For production use, integrate with:

- **BetterAuth** — open-source auth library (email/password, OAuth, magic links)
- **WorkOS** — enterprise SSO (SAML, SCIM, directory sync)

The integration should map external user identities to jazz2 sessions and client roles.

## Hosted Integration Plan

- Auto-provision a WorkOS org per jazz app (via developer dashboard)
- Offer hosted BetterAuth instance as a less vendor-locked alternative
- See `developer_dashboard_billing.md` and `infra_and_dashboard.md` for the control plane

## Open Questions

- Where does the auth boundary sit? (Middleware before jazz2, or built into the server?)
- Session token format: keep JWTs or switch to opaque tokens with server-side lookup?
- How do external user IDs map to jazz2's session/client model?
- Row-level access control: can we derive permissions from WorkOS roles/groups?
- Self-hosted vs. cloud auth — both paths needed?

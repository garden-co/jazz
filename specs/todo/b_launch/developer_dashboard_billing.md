# Developer Dashboard & Billing Portal — TODO

Web UI for developers to manage their jazz2 apps, view usage, and handle billing.

## Overview

A hosted dashboard where developers can:

- Create and manage apps (app IDs, API keys, secrets)
- View usage metrics (storage, sync bandwidth, active connections)
- Configure server settings (regions, replication)
- Manage billing (plans, invoices, payment methods)
- View logs and debug sync issues

## Auth Integration

The dashboard should manage Jazz Auth configuration for each app: hosted auth enablement, provider setup, and generated self-host compatible Better Auth snippets. Enterprise SSO / WorkOS-style integrations are follow-up work layered on top of this. See `auth_integrations.md` for details.

## Open Questions

- Built with jazz2 itself (dogfooding) or separate stack?
- Billing provider: Stripe, Paddle, or Lemon Squeezy?
- Usage metering granularity (per-row, per-byte, per-connection?) — see `observability_first.md` for telemetry-driven billing
- Free tier limits?
- Multi-tenant architecture: shared servers vs. dedicated instances?

# Multi-Tenant Sync Server Deployment — TODO

Get a hosted multi-tenant sync server running so the team and first adopters can build apps without local Docker.

## Overview

Immediate goal: a deployed, multi-tenant version of the existing CLI sync server.

- Single region, single instance (scale later — see `../c_launch/infra_and_dashboard.md`)
- Multi-tenant: multiple app IDs on one server
- Manual provisioning of app IDs and secrets (no dashboard needed yet)
- Frequent releases expected, breakage tolerated at this stage
- HTTPS + auth (JWT with shared secret per app)

## What Exists

The `jazz-cli` crate already runs a sync server locally. This task is about deploying it as a shared service.

## Open Questions

- Deployment target for v1? (Fly.io single machine? EC2? Container on Railway?)
- Domain and TLS setup
- How to provision app IDs and secrets manually? (Config file? CLI command? Env vars?)
- Monitoring: at minimum, health check + crash alerts

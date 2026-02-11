# Hosted Multi-Tier Infrastructure — TODO

Design for the hosted/managed version of jazz2's server infrastructure.

## Overview

A production-ready hosted deployment topology:

- **Edge servers** — auto-scaling per region, close to users, handle sync + query settlement
- **Core server** — auto-scaling in a central region, coordination and global consistency
- **Store shard servers** — fixed count, rendezvous hashing for data distribution (see `sharding_design_sketch.md`)

The developer dashboard (see `developer_dashboard_billing.md`) is the control plane for this infrastructure, and auth integrations (see `auth_integrations.md`) handle per-app user management.

## Open Questions

- Deployment target: Kubernetes, Fly.io, Cloudflare, or custom?
- Edge server provisioning: how to auto-scale per region based on demand?
- How do edge servers discover and connect to core/shard servers?
- Multi-tenant isolation: shared processes with logical separation, or process-per-app?
- Self-hosted option: can developers run the same topology on their own infra?

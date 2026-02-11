# Data Export & External System Sync — TODO (Launch)

One-way data export from Jazz to external databases and services — as a **documented pattern**, not a first-class feature.

## Overview

Jazz is the source of truth. Data flows **out** to external systems for:

- **Analytics / reporting**: sync to Postgres, ClickHouse, BigQuery for SQL analytics
- **Search**: sync to Elasticsearch, Typesense, Meilisearch for full-text search
- **Compliance**: export to audit-friendly formats
- **Migration off Jazz**: full data export so developers are never locked in

This was a major gap in Jazz 1 that deterred potential adopters. Making export possible is a trust signal.

## Approach: Pattern on Webhook Subscriptions

Rather than building a dedicated connector/adapter framework, data export should be a **documented pattern** built on top of Jazz's existing primitives:

- A Jazz client with Peer role subscribes to relevant queries
- On each change, it writes to the external system (Postgres, ES, etc.)
- This is essentially a webhook subscriber that transforms and forwards data

### What we ship

- Documentation: "How to export Jazz data to Postgres" (and other targets)
- Example app / recipe demonstrating the webhook subscription pattern
- Possibly a small utility library for common transformations (Jazz row → SQL INSERT)

### What we don't build (yet)

- A managed connector framework
- Built-in adapters per target system
- A dedicated change-feed protocol

## Open Questions

- How to handle initial backfill (export all existing data, not just changes)?
- Schema mapping: how to translate Jazz schema to target schema?
- Should we provide a generic "export to JSON lines" utility?
- How to handle schema migrations in the external system when Jazz schema changes?

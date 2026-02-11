# Data Export & External System Sync — TODO

First-class one-way sync from Jazz to external databases and services.

## Overview

Jazz is the source of truth. Data flows **out** to external systems for:

- **Analytics / reporting**: sync to Postgres, ClickHouse, BigQuery for SQL analytics
- **Search**: sync to Elasticsearch, Typesense, Meilisearch for full-text search
- **Compliance**: export to audit-friendly formats
- **Migration off Jazz**: full data export so developers are never locked in

This was a major gap in Jazz 1 that deterred potential adopters. Making export a first-class feature is a trust signal.

## Design Direction

- One-way: Jazz → external system. Jazz does not read back from external systems.
- Change-feed based: leverage the existing sync protocol / reactive queries to stream changes
- Connector model: pluggable adapters per target system (Postgres connector, ES connector, etc.)
- Could run as a Jazz client with Peer role that subscribes to all relevant queries and writes to the external system

## Open Questions

- Real-time streaming vs. periodic batch export?
- Schema mapping: how to translate Jazz schema (with lenses) to target schema?
- Should connectors run as sidecar processes, built-in server plugins, or external services?
- How to handle schema migrations in the external system when Jazz schema changes?
- Backfill strategy for initial export of existing data?
- Is the change feed the sync protocol itself, or a higher-level abstraction?

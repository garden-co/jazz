# Serverless KV Storage Adapter — TODO

Storage adapter for serverless platforms that lack filesystem access.

## Overview

Not all deployment targets have a real filesystem for native storage engines. Serverless environments (Cloudflare Workers, Vercel Edge, Deno Deploy) often provide:

- KV stores (Cloudflare KV, Vercel KV)
- SQLite adapters (Cloudflare D1, Turso)
- Simple object storage (R2, S3)

The IoHandler interface needs an adapter that maps to these primitives. The key requirement is support for **key-value storage + range queries over keys** (for indices).

## Design

- SQLite-based adapters are the easiest path (SQLite is B-tree based, provides range queries natively)
- Pure KV stores may need a sorted-key scheme for range scan emulation
- Performance is secondary — serverless nodes handle a small subset of data as a client to the main infra

## Open Questions

- Is SQLite available on all target serverless platforms?
- If only KV is available, how to implement efficient range queries? (Key encoding scheme?)
- Sync semantics: serverless node as a peer client, or as a lightweight edge server?
- Cold start latency: how to minimize data loading on first request?
- Can we reuse the same IoHandler trait or do we need an async variant for KV stores?

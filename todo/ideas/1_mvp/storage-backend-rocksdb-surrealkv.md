# Storage backend: RocksDB on server, SurrealKV on RN

## What

Replace Fjall with RocksDB on the server and evaluate SurrealKV as the RN storage backend.

## Why

Fjall is a risky long-term bet.

Fjall was adopted primarily to have something that works equally good on servers and mobile apps.

But the server and mobile constraints are different — we can pick the best option for each:

- **Server:** RocksDB is the most battle-tested embedded KV store available
- **RN:** SurrealKV could be a viable alternative to Fjall

## Who

Core infra team

## Rough appetite

big

## Notes

- Need to validate SurrealKV on RN (build toolchain, performance, stability)
- RocksDB on the server is low-risk; the main work is the integration layer
- Migration path from Fjall needs consideration

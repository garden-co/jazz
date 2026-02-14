# Legacy WASM persistence problem (archived)

## Context

This document captured a persistence failure mode in the previous browser storage engine used before `opfs-btree`.

## Root issue (historical)

The legacy engine depended on a WAL + snapshot lifecycle where snapshotting was required to make recovered WAL state durable across repeated hard restarts.

In the browser worker runtime, snapshotting was not reliable, so state could survive one restart via WAL replay, then be lost on the next restart after WAL rotation.

## Resolution path

The project moved to:

- `OpfsBTreeStorage` for WASM/OPFS durable worker storage
- `SurrealKvStorage` for native storage

This archived note remains only as historical background for why the storage migration happened.

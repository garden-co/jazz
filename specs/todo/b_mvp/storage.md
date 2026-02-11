# Storage — TODO

Remaining work for storage and platform bindings.

> Status quo: [specs/status-quo/storage.md](../../status-quo/storage.md)

## Phasing

- **MVP**: Multi-tab leader election, browser E2E verification
- **Launch**: Compression strategy

## MVP: Multi-Tab Leader Election

Currently only single-tab OPFS access works (exclusive `SyncAccessHandle` lock). Need leader election so multiple tabs can coordinate:

- One tab's worker owns OPFS
- Other tabs sync through the leader via BroadcastChannel or SharedWorker
- Leader failover on tab close (accept potential loss — fire-and-forget semantics)

## MVP: Browser E2E Verification

A comprehensive E2E suite beyond the current 10 browser tests would exercise:

- Reload → Recovery from OPFS
- Multi-tab coordination (once leader election is done)
- Edge cases in worker bridge lifecycle

## Launch: Compression Strategy

Rely heavily on compression (LZ4 or zstd) since row data is mostly text:

- Pages cached in memory in compressed form
- Decompress only the small number of rows being actively read
- Data flows through the system mostly compressed (storage, sync, wire)
- Often faster than micro-optimizing integer types — fewer bytes = fewer cache misses + less I/O

Needs benchmarking to choose between LZ4 (faster, lower ratio) and zstd (slower, better ratio). May use both: LZ4 for hot path, zstd for cold storage / wire.

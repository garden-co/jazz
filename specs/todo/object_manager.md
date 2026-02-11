# Object Manager — TODO

Remaining work items for the object layer.

> Status quo: [specs/status-quo/object_manager.md](../status-quo/object_manager.md)

## Blob & Truncation Test Coverage

The old blob and truncation tests relied on the async request/response API (`take_requests`, `push_response`, `StorageRequest`, `StorageResponse`). These were removed during the synchronous storage rewrite.

New tests using the `Storage` trait directly are needed:

- Blob associate → load round-trip via `MemoryStorage`
- Blob deduplication (same content hash)
- Blob garbage collection during truncation
- Truncation with blob associations spanning multiple commits

> See comment at `crates/groove/src/object_manager.rs:2289-2293`

## Memory Profiling Refinement

`memory_size()` (`object_manager.rs:1087-1191`) provides estimates but could be more accurate for:

- Variable-length blob data
- Subscription overhead per branch
- HashMap overhead factors

# Object Manager — TODO (Launch)

Remaining work items for the object layer.

> Status quo: [specs/status-quo/object_manager.md](../../status-quo/object_manager.md)

## Memory Profiling Refinement

`memory_size()` (`object_manager.rs:1087-1191`) provides estimates but could be more accurate for:

- Variable-length blob data
- Subscription overhead per branch
- HashMap overhead factors

## Superseded

**Blob & Truncation Test Coverage** — the old blob tests relied on the async storage API which was removed. The blob abstraction itself is being replaced by binary columns with FK refs (see `../b_mvp/binary_columns_and_fk_refs.md`). New test coverage should target the binary columns design instead.

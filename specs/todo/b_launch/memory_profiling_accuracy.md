# Memory Profiling Accuracy — TODO (Launch)

The current `memory_size()` / `estimate_memory_size()` helpers in the query and
sync layers provide useful estimates, but they could be more accurate for:

- Variable-length binary data
- Subscription overhead per branch
- HashMap overhead factors

Current entry points:

- `crates/jazz-tools/src/query_manager/manager.rs`
- `crates/jazz-tools/src/query_manager/graph.rs`
- `crates/jazz-tools/src/sync_manager/mod.rs`

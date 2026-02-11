# Memory Profiling Accuracy — TODO (Launch)

`memory_size()` (`object_manager.rs:1087-1191`) provides estimates but could be more accurate for:

- Variable-length blob data
- Subscription overhead per branch
- HashMap overhead factors

Note: old blob tests are superseded by `../b_mvp/binary_columns_and_fk_refs.md`.

# Memory Profiling Accuracy — TODO (Launch)

`memory_size()` (`object_manager.rs`) provides estimates but could be more accurate for:

- Variable-length binary data
- Subscription overhead per branch
- HashMap overhead factors

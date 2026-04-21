# Session UUID Format Mismatch — TODO (Launch)

Session claim comparison uses Debug format which may differ from JSON string format, causing false negatives in IN checks.

Relevant session/policy code now lives in:

- `crates/jazz-tools/src/query_manager/session.rs`
- `crates/jazz-tools/src/query_manager/policy.rs`

# Session UUID Format Mismatch — TODO (Launch)

Session claim comparison uses Debug format which may differ from JSON string format, causing false negatives in IN checks.

> `crates/groove/src/query_manager/session.rs`

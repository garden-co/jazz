# Duplicated batch bookkeeping storage

## What

Local batch records duplicate sealed submissions and settlements that are also stored in dedicated durable tables, increasing persistent size and widening the replay state surface.

## Priority

medium

## Notes

- Where:
  - `crates/jazz-tools/src/batch_fate.rs`
  - `crates/jazz-tools/src/storage/mod.rs`
  - `crates/jazz-tools/src/runtime_core/writes.rs`
- `LocalBatchRecord` currently embeds `sealed_submission` and `latest_settlement`.
- Storage also persists sealed submissions and authoritative settlements separately.
- Direction: slim `LocalBatchRecord` to the minimal writer-side tracking state and load durable batch artifacts from their dedicated tables when needed.

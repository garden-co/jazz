# Row storage common-case encoding

## What

The flat row formats encode common singleton and empty cases verbosely, especially visible branch frontiers and empty metadata, which wastes space on the dominant row shapes.

## Priority

medium

## Notes

- Where:
  - `crates/jazz-tools/src/row_histories/mod.rs`
- Most visible rows have a branch frontier equal to the current batch id.
- Most rows do not carry metadata.
- Direction: make singleton frontier and empty metadata implicit or nullable in storage, instead of always encoding arrays.

# Text-encoded storage enums

## What

Flat row storage currently encodes enum-like fields as text, which is larger than necessary and adds avoidable decode overhead on hot storage paths.

## Priority

low

## Notes

- Where:
  - `crates/jazz-tools/src/row_histories/mod.rs`
  - `crates/jazz-tools/src/row_format.rs`
- Affected fields include row state, confirmed tier, delete kind, and similar fixed-domain values.
- Direction: switch these storage encodings to compact integer tags once the structural row-shape cleanup has landed.

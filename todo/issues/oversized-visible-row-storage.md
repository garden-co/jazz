# Oversized visible row storage

## What

The visible-row region stores a full current history row plus visibility bookkeeping, which duplicates history-only fields and makes the hot visible prefix heavier than it needs to be.

## Priority

medium

## Notes

- Where:
  - `crates/jazz-tools/src/row_histories/mod.rs`
  - `crates/jazz-tools/src/query_manager/graph_nodes/materialize.rs`
- Visible queries need the current row data, provenance, batch identity, delete bits, and tier/frontier state.
- The current visible entry also carries parents, metadata, timestamps, and other fields already available from history.
- Direction: split visible storage into a compact visible head and keep history-only fields in the history region.

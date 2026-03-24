# Complex Merge Strategies

## What

Per-column/per-table merge strategies beyond LWW (counters, sets, rich text, custom logic).

## Why

LWW breaks down for concurrent counters (should sum), sets (should union), rich text (positional merging), and domain-specific rules.

## Who

App developers building collaborative features.

## Rough appetite

big

## Notes

Potential strategies: G/PN-Counter, MV-Register, OR-Set, RGA/Yjs-style sequences, custom WASM merge functions. Open questions around schema declaration syntax, storage format, interaction with lenses, and CRDT metadata overhead.

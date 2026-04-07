# Complex Merge Strategies

## What

Per-column/per-table merge strategies beyond LWW (counters, sets, rich text, custom logic).

## Notes

- LWW breaks down for concurrent counters, sets, rich text, and domain-specific merge rules.
- Main consumers are app developers building collaborative features.
- Potential strategies: G/PN-Counter, MV-Register, OR-Set, RGA/Yjs-style sequences, and custom WASM merge functions.
- Open questions: schema declaration syntax, storage format, interaction with lenses, and CRDT metadata overhead.

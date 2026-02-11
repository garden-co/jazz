# Complex Merge Strategies — TODO

Beyond last-writer-wins: richer conflict resolution for collaborative data.

## Overview

LWW (last-writer-wins) works for simple fields but breaks down for:

- **Counters** — concurrent increments should sum, not overwrite
- **Sets / lists** — concurrent adds should union, not replace
- **Rich text** — concurrent character insertions need positional merging
- **Custom business logic** — "highest bid wins", "union of tags", etc.

Need a way to declare per-column or per-table merge strategies beyond the default.

## Potential Strategies

- **Counter CRDT** — G-Counter / PN-Counter for numeric fields
- **MV-Register** — keep all concurrent values, let app resolve
- **LWW-Register** — current default, keep for simple fields
- **OR-Set** — observed-remove set for collection columns
- **RGA / Yjs-style** — for ordered sequences and rich text
- **Custom merge functions** — user-supplied Rust/WASM functions

## Open Questions

- How to declare strategy in schema? (`CREATE TABLE ... MERGE STRATEGY counter`?)
- Storage format: do different CRDTs need different object representations?
- How do merge strategies interact with lenses/schema migration?
- Can merge strategies be changed after data exists?
- Performance: some CRDTs (e.g., RGA) have significant metadata overhead

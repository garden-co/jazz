# Explicit Indices — TODO (MVP)

Developer-declared indices in the schema language.

## Overview

Replace auto-index-all-columns with explicit index declarations. The team agreed this is the right trade-off: slightly worse DX that aligns with relational database norms, but guarantees good performance when done correctly.

### What to Support

- Single-column indices
- Compound indices (critical — permission queries combine e.g. `organization_id` + `created_at`)
- Foreign key columns indexed by default
- Index rebuild when adding an index to a table with existing data

### Slow Query Warnings

Surface warnings when the query engine performs a full table scan. This lets developers (or LLMs) identify which indices to add.

## Open Questions

- Schema language syntax? (`@@index([col_a, col_b])`? `col.indexed()`?)
- How do explicit indices interact with schema lenses / migrations?
- How do per-branch indices interact with index declarations?
- Index rebuild strategy: online (background) or blocking?

## Future: Smart Automatic Indices

See `../d_later/smart_automatic_indices.md` — observe query patterns and create narrow, per-value indices on demand. Deferred from MVP.

# Explicit & Smart Indices — TODO

Transition from auto-index-all-columns to explicit developer-declared indices, with a path toward smart automatic indices later.

## Overview

Currently all columns are indexed automatically. The team agreed this is expensive on writes and a poor default. The plan:

### Phase 1: Explicit indices (MVP)

Developers declare indices in the schema language:

- Single-column indices
- Compound indices (critical — permission queries combine e.g. `organization_id` + `created_at`)
- Foreign key columns indexed by default (good default, cheap to maintain)

Explicit indices align with how relational databases work. Developers find this normal and manageable.

### Phase 2: Smart indices (post-launch)

The system observes query patterns and creates narrow, specific indices on demand:

- Per-value indices (e.g., one index per `organization_id` value rather than a global index)
- Automatic compound index creation based on observed filter+sort combinations
- Lazy creation: only index data that queries actually need
- Warnings / dev tools for full table scans on slow queries

## Interaction with Other Features

- Compound indices are essential for permission queries (JWT claims + user filters)
- Index pages can be evicted if objects are evicted (see `storage_limits_and_eviction.md`)
- Different index types (vector, geospatial) are future extensions beyond B-tree

## Open Questions

- Schema language syntax for explicit indices? (`@@index([col_a, col_b])`?)
- Index rebuild strategy when indices are added to existing data?
- How do explicit indices interact with schema lenses / migrations?
- Should we surface "full table scan" warnings in dev tools / observability?
- How do per-branch indices interact with explicit index declarations?

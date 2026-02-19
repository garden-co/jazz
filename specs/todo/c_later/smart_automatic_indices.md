# Smart Automatic Indices — TODO (Later)

Query-driven automatic index creation, replacing manual index management.

## Overview

Instead of developers declaring indices, the system observes query patterns and creates narrow, specific indices on demand:

- Per-value indices (e.g., one index per `organization_id` value rather than a global index)
- Automatic compound index creation based on observed filter+sort combinations
- Lazy creation: only index data that queries actually need
- Drop unused indices after a period of no queries

## Prerequisites

- Explicit indices must be working first (see `../a_mvp/explicit_indices.md`)
- Observability / slow query detection helps inform which indices to auto-create
- Future index types (vector, geospatial) may follow different auto-creation strategies

## Open Questions

- How to observe queries without significant overhead?
- Index creation budget: how many auto-indices before we stop?
- How to handle index thrashing (queries change frequently)?
- Can we suggest explicit indices to developers based on observed patterns (advisory mode)?

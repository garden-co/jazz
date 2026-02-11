# Full Point-in-Time Queries — TODO (Later)

Query across rows at a historical timestamp.

## Overview

Beyond per-object time travel (see `../c_launch/per_object_time_travel.md`), this enables:

- `SELECT * FROM todos AS OF '2025-01-15T10:00:00Z'` — all rows at a point in time
- Diff queries: what changed between two timestamps across a table?
- Historical aggregations

## Why Later

The team agreed this is low priority for launch:

- Current indices don't support historical queries — would require full table scan
- Primary use case for history (auditing, individual object history) is served by per-object time travel
- Acceptable for historical cross-table queries to be expensive initially
- Smart historical indices can be explored if apps actually use this and are willing to pay the cost

## Prerequisites

- Per-object time travel (launch)
- Smart indices (later) — historical index variants
- Branch infrastructure maturity

## Open Questions

- Syntax: `AS OF` clause? Function? Query parameter?
- Can reactive subscriptions work with historical queries? (Subscribe to "the state as of a moving window"?)
- Storage cost of retaining enough history for arbitrary point-in-time reads

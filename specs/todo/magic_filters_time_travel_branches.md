# Magic Filters for Time Travel & Branch Selection — TODO

Query-time filters that let users read historical states and select branches.

## Overview

Expose time-travel and branch selection as query filters rather than separate APIs:

- `SELECT * FROM todos AS OF '2025-01-15T10:00:00Z'` — point-in-time read
- `SELECT * FROM todos ON BRANCH 'feature-x'` — read from a specific branch
- `SELECT * FROM todos BETWEEN '2025-01-01' AND '2025-01-31'` — history range
- Diff queries: what changed between two points in time?

These build on the existing branch infrastructure (`{env}-{schemaHash}-{userBranch}`) and object history.

## Open Questions

- Syntax: SQL extensions vs. function-call style vs. query parameter?
- Granularity: per-row snapshots or per-table?
- Performance: need efficient temporal indexing or is scanning history acceptable?
- How do time-travel reads interact with reactive queries (subscribe to "as of" a moving window)?
- Branch merging UI — how do users resolve divergent branches?

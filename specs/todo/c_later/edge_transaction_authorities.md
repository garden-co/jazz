# Edge Transaction Authorities — TODO (Later)

Smart latency-minimizing transaction authorities at edges.

## Overview

Extension of `../b_launch/globally_consistent_transactions.md`. Instead of a single global authority, place transaction authorities at edge nodes to minimize round-trip latency for region-local transactions.

- Partition authority responsibility by data region or table
- Edge authorities handle local transactions with low latency
- Cross-region transactions escalate to a global coordinator
- Conflict resolution between edge authorities for overlapping scopes

## Open Questions

- Partitioning strategy — geographic? per-table? per-app?
- Consistency model between edge authorities — strong or eventual?
- How to handle authority failover without losing in-flight transactions

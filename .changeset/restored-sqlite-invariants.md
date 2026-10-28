---
"jazz-tools": patch
---

Restore five guarantees from before the SQLite port:

- A transaction's write policies are checked against everything the transaction writes.
- Permission advice answers can no longer resolve a different request.
- The Rust client reports a fatal sync error instead of stalling.
- A later schema version can add a `branchBy` column that has a default.
- A store keeps a refused transaction's payload until its host has reported the refusal.

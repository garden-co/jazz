---
"jazz-tools": major
---

BREAKING CHANGE: remove `LocalUpdatesMode`, `QueryPropagation`, and their `localUpdates` and `propagation` query options from the public API. Read tiers now determine how subscriptions treat the caller's own local writes: `local-first` and `remote-if-possible` publish them immediately, while `remote` defers them until the remote view observes them. Local-only reads are not supported by the public query API.

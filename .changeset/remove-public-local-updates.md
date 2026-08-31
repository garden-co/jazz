---
"jazz-tools": major
---

BREAKING CHANGE: remove `LocalUpdatesMode` and the `localUpdates` query option from the public API. Read tiers now determine how subscriptions treat the caller's own local writes: `local-first` and `remote-if-possible` publish them immediately, while `remote` defers them until the remote view observes them.

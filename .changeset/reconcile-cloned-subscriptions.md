---
"jazz-tools": patch
---

Reconcile canonical query membership before cloning it to a new subscriber. Canonical transitions are published exactly once to every established sibling, with their existing authorization progress and receipt pairing, before the new clone's fallible reset is assembled. Established siblings therefore converge even when that reset fails, without changing the public Rust rehydration return type. Replaced maintained receivers are also unregistered eagerly.

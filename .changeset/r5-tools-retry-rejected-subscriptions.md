---
"jazz-tools": patch
---

A framework subscription (`useAll` and friends) that was rejected now subscribes again when the sync link connects again, for example after `db.reconnect()`, instead of keeping the error for as long as the component stays mounted.

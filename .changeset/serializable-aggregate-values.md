---
"jazz-tools": patch
---

Fix serializable transactions that read an aggregate (a `count`, `sum` and the like) committing even after the aggregate changed. A "book if under capacity" transaction could overbook under concurrent bookings. The read set now records each aggregate group with its values, so a changed value makes the commit conflict. Clients before this release keep validating aggregates as they did, by group only, until they upgrade.

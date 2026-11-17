---
"jazz-tools": patch
---

Fix serializable transactions that read an aggregate (`count`, `sum`, `avg`, `min`, `max`, grouped or not) committing even after the aggregate changed. A "book if under capacity" transaction could overbook under concurrent bookings. The read set now records each aggregate group with its values, so a changed value, or a group that appears or disappears, makes the commit conflict.

**Upgrade servers (and Jazz Cloud) before clients.** The server accepts only value-checked aggregate reads. Until a client upgrades, its serializable transactions that read an aggregate now conflict on every commit instead of committing unchecked. A client on this release talking to an older server sees the same: its aggregate reads conflict until the server upgrades.

---
"jazz-tools": patch
---

Handle the tiers removed in alpha.57 at runtime. `wait({ tier: "edge" })` on an already applied write now waits for `"global"` and warns once, instead of rejecting after the write committed (which made retrying callers duplicate rows). `db.all`, `db.one`, `db.subscribe`, transaction reads and `useAll` now reject `"remote-if-possible"` and `"edge"` read tiers with an error naming the replacement, instead of silently running at the default tier.

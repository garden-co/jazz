---
"jazz-tools": patch
---

Report the tiers removed in alpha.57 at runtime. `db.all`, `db.one`, `db.subscribe`, transaction reads and `useAll` now reject `"remote-if-possible"` and `"edge"` read tiers with an error naming the replacement, instead of silently running at the default tier. `wait({ tier: "edge" })` now rejects with `The "edge" tier was removed. Use "global" for server-confirmed writes. The write was already applied; do not retry it.`, instead of `unknown durability tier edge`; the write itself is unaffected and still syncs.

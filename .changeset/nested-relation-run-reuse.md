---
"jazz-tools": patch
---

Make nested includes cheaper to read and to keep live. Within one read, each relation's SQL is now built once and only the parent value is rebound, and each relation's rows are read once per distinct parent value. A write to a table that a live query includes, such as a presence stamp on a profile shown under every message, no longer rebuilds and re-runs the child query for every parent row.

---
"jazz-tools": patch
---

`jazz-tools validate` now handles a project upgraded from alpha.59. It moves alpha.59's migrations to `migrations/legacy/` before loading anything, where it used to crash on their old migration DSL. It also moves snapshots that no remaining migration reaches to `migrations/legacy/snapshots/`, which fixes `createJazz`'s "disconnected roots" error after the upgrade. When no snapshot matches `schema.ts`, it keeps the newest one and tells you to run `jazz-tools migrations create` before the app starts.

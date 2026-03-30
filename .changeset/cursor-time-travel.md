---
"cojson": patch
"jazz-tools": patch
---

Add cursor-based time travel for CoValues

Introduces the ability to create cursors (frontier snapshots) on loaded CoValues and later reload them at that exact point in time. Cursors encode the full frontier state of a CoValue and its resolved children, enabling read-only historical views.

- Add `createCursor()` and `cursor` getter to CoValue instances
- Support loading CoValues by cursor via `load()` and `ensureLoaded()`
- Add `useCurrentCursor` option to capture the current state as a cursor
- Prevent mutations on cursor-loaded (time-travel) CoValues
- Validate cursor root ID and resolve query compatibility (subset check)

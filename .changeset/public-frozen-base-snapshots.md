---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Expose `db.captureSnapshot()` and typed, versioned snapshot references for frozen branch bases. Add persistence helpers, explicit unavailable-history checks, native foreground capture support, and a maintained example covering restoration and draft edits.

Existing compatible React Native builds continue to support existing operations. Only snapshot capture requires an updated native build; older builds report `UnsupportedSnapshotError` when capture is called.

Open frozen subscriptions retain their captured base values through later writes and transaction fate changes. Reopening still requires the captured history to remain available.

The TypeScript `snapshot` option now requires a `SnapshotRef` string. Replace hand-built snapshot field objects with `db.captureSnapshot()` or restore a saved string with `parseSnapshotRef()`; field objects passed to this option now throw `SnapshotRefFormatError`.

"cojson": patch
"cojson-storage-indexeddb": patch
---

Avoid unhandled IndexedDB shutdown race errors by treating close-time transactions as safe no-ops and guarding finished transaction cleanup.

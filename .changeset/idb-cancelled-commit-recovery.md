---
"jazz-wasm": patch
"jazz-tools": patch
---

Fix a browser write cancelled while its IndexedDB commit was pending (for example by a torn-down page or a cancelled task). Reads could return the unsaved rows, and every later write failed until the page reloaded. Storage now reloads from IndexedDB on the next operation, so only writes IndexedDB confirmed are visible and later writes work normally. The IndexedDB page format is unchanged.

---
"jazz-tools": patch
"jazz-rn": patch
---

Fix React Native clients reading no rows from a server with schema history. The native relay opened its store from the current schema alone, so it stored columns under different names than the server. It now receives the migrations the app ships, as Node and browser clients do. This raises the native relay ABI to 5, so a JavaScript bundle needs a native build from the same release.

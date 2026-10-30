---
"jazz-tools": patch
---

Fix session reads with recursive or gather permission policies failing with "graph contains a dependency cycle" in release builds (NAPI and WASM) when one query input is reached through two paths.

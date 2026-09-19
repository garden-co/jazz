---
"jazz-tools": patch
---

Release pending read ownership when native, WASM, or React Native consumers cancel or close. Preserve transaction read ordering and defer query coverage cleanup safely while storage work owns the node.

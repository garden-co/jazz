---
"jazz-tools": patch
---

`jazz.permissions.json` refuses `maxDepth` on `allowedTo` along a reverse relation (`allowedTo.read(<reverse relation>, maxDepth: n)` or `allowedTo.read(via t.col, maxDepth: n)`), as the TS DSL did, instead of accepting a bound the engine then dropped. It also caps a recursive relation's `maxDepth` at 64, as the DSL's `gather` did.

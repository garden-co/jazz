---
"jazz-tools": patch
---

In browsers, the `[jazz] slow query` warning is now one short line: the query's table, timings and index hint. Its query plan and SQL go to `console.debug` (DevTools' "Verbose" level) instead of `console.warn`, so end users no longer see them. Servers still log the full line.

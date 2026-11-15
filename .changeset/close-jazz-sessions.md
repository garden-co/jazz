---
"jazz-tools": patch
---

Add `closeJazzSessions()` in `jazz-tools/testing/sessions`. Register it with `afterEach(closeJazzSessions)` to close every Jazz session a test left open, so workers, sockets and timers don't leak into the next test. Works in Vitest and Jest, in Node and the browser. Nothing is tracked unless the module is imported.

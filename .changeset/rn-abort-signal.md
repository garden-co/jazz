---
"jazz-tools": patch
---

React Native: `write.wait()` no longer fails with "undefined is not a function". React Native's `AbortSignal` polyfill lacks `throwIfAborted()` and abort reasons, which write waits rely on; Jazz now adds them where they are missing.

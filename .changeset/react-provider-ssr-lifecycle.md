---
"jazz-tools": patch
---

Defer React `JazzProvider` client acquisition until the browser commit lifecycle so server rendering stays side-effect free and hydration starts from the configured fallback.

---
"jazz-tools": patch
---

Fix `createPolicyTestApp(...)` so policy test helpers no longer hard-code Vitest's `expect`.

Callers now pass the `expect` function explicitly, which keeps `jazz-tools/testing` policy assertions working when the test harness provides its own assertion context.

---
"jazz-tools": patch
---

Fix backend N-API timestamp writes when `Timestamp` values arrive from TypeScript as JS numbers.

`createJazzContext(...)` and other backend N-API mutation paths now accept integral epoch-millisecond timestamp payloads produced by the TS value converter, instead of rejecting modern dates as floating-point values during Rust deserialization.

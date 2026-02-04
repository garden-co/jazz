---
"cojson": patch
"jazz-tools": patch
---

Optimized FileStream.asBase64

Benchmark results (`@latest` → `@workspace`):

- `asBase64`: 6.65 → 29.32 op/sec (~4.4x)

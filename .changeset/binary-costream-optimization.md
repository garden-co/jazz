---
"cojson": patch
"jazz-tools": patch
---

Optimized FileStream.loadAsBlob and FileStream.asBase64

Benchmark results (`@latest` → `@workspace`):
- `Write 100KB`: 5.88 → 32.10 op/sec (~5.5x)
- `Write 1MB`: 4.80 → 29.14 op/sec (~6.1x)
- `Write 5MB`: 5.92 → 32.49 op/sec (~5.5x)
- `getChunks 1MB`: 6.02 → 32.89 op/sec (~5.5x)
- `getChunks 5MB`: 5.99 → 32.59 op/sec (~5.4x)
- `asBase64 1MB`: 5.93 → 32.34 op/sec (~5.5x)
- `asBase64 5MB`: 6.05 → 32.98 op/sec (~5.5x)

---
"cojson": patch
"jazz-tools": patch
---

Optimized FileStream.asBase64 and FileStream.asBlob

**Benchmark results with toBase64 native support (5MB file):**
- `asBase64`: 732.39 op/sec vs  49.78 op/sec (**+1371.36% faster**)
- `write`: 12.53 op/sec vs 12.19 op/sec (+2.79%)
- `getChunks`: 695.03 op/sec vs 153.89 op/sec  (**+351.64% faster**)

**Benchmark results (5MB file):**
- `asBase64`: 118.62 op/sec vs 47.16 op/sec (**+151.50% faster**)
- `write`: 12.53 op/sec vs 12.19 op/sec (+2.79%)
- `getChunks`: 93.31 op/sec (unchanged)


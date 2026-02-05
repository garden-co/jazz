---
"cojson": patch
"jazz-tools": patch
---

Improved FileStream base64 encoding performance by using `bytesToBase64url` instead of `btoa` with `String.fromCharCode`. Added native `toBase64`/`fromBase64` support in cojson when available.

**Benchmark results (5MB file):**
- `asBase64`: 732.39 op/sec vs  49.78 op/sec (**+1371.36% faster**)
- `write`: 12.53 op/sec vs 12.19 op/sec (+2.79%)
- `getChunks`: 695.03 op/sec vs 153.89 op/sec  (**+351.64% faster**)

---
"jazz-tools": patch
---

The native runtime codec reads postcard u64 values of up to 7 bytes as Numbers without going through BigInt, and copies small byte values with an indexed loop.

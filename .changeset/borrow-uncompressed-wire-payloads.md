---
"jazz-tools": patch
---

Reduce peak memory use when decoding uncompressed inbound wire payloads by borrowing the assembled message bytes.

---
"jazz-tools": patch
---

Evaluate each uploaded row's write policy once instead of twice when a server admits a client session upload, stop re-encoding and re-validating uploads that were already checked, and open an existing RocksDB store once instead of twice at startup.

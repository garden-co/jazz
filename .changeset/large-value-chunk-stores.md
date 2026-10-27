---
"@jazz/rust": patch
---

Add pluggable chunk stores for large values: `DbConfig::with_chunk_store` moves the bytes of settled large-value leaves out of the SQLite file into a directory (`FileChunkStore`) or any asynchronous byte store, while their metadata keeps committing with the rows. Stores without a chunk store are unchanged.

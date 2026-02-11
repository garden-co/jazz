# Storage Compression Strategy — TODO (Launch)

Rely heavily on compression (LZ4 or zstd) since row data is mostly text:

- Pages cached in memory in compressed form
- Decompress only the small number of rows being actively read
- Data flows through the system mostly compressed (storage, sync, wire)
- Often faster than micro-optimizing integer types — fewer bytes = fewer cache misses + less I/O

Needs benchmarking to choose between LZ4 (faster, lower ratio) and zstd (slower, better ratio). May use both: LZ4 for hot path, zstd for cold storage / wire.

# Canonical V1 leaf decoding without temporary payloads

V1 leaf nodes are ordinary Groove records with two fixed u8 fields and one
raw-bytes field. The canonical encoding is the shortest enum tag 0, those two
bytes and the complete raw payload. Comparing that header and payload directly
preserves exact canonical equality without constructing an owned enum, decoded
Values and a second full re-encoding. Branch decoding uses the existing codec.
All object/logical hash, format, kind, UTF-8, size, metric and traversal checks
remain. No wire or storage bytes change; no cache or retained state is added.

## Complete-read receipt

The native `local_blob_reads` fixture uses two resident MemoryStorage databases,
ordinary peer coverage, a reference, eight claims, 32 schema tables and 36
settled background queries. Every returned byte is checked. The source control
is the retained integration `cc5a303f1`, including authenticated chunk leases
from #3619. The measured candidate changes only leaf decoding. This receipt
is neither browser startup nor a cold storage/transfer measurement.

| Payload |   Control | Candidate | Less time |
| ------- | --------: | --------: | --------: |
| 256 KiB |  4.139 ms |  3.886 ms |      6.1% |
| 4 MiB   | 14.851 ms | 11.866 ms |     20.1% |
| 16 MiB  | 90.898 ms | 78.991 ms |     13.1% |

ABBA then BAAB, four fresh processes per order, nine reads per size/process;
the first read is retained in the raw receipt and excluded from these medians.
Both orders independently confirm the 4/16 MiB gains. Their full-materialization
count remains two. Runs had no concurrent owned build, test or profiler.

The short 32 KiB runs regressed 3.381 to 3.735 ms (10.5%); this case performs
zero full materializations. A longer independent ABBA+BAAB confirmation with
41 reads/process and sizes 32/256 KiB did not reproduce that regression:
32 KiB medians were 3.290/3.290 ms after discarding five warmup reads; even its
first measured reads 1–8 were 3.344/3.308 ms. 256 KiB was 4.161/3.854 ms.
There is no claimed small inline-read gain. All observations, including the
initial negative result and warmup reads, remain in `samples.csv`.

`receipt.json` records source-file and executable SHA-256 hashes and the exact
runtime-patch hash. The public codec corpus independently constructs ordinary
records for all three kinds, boundary sizes, all unknown format/kind bytes,
noncanonical tags, truncation, malformed UTF-8, oversized leaves and payload
suffix/object-hash behavior. Existing test assertions are unchanged.

## Reproduce

Build once, freeze the executable and measure without a compiler running:

```sh
cargo bench -p jazz --profile perf --no-default-features --features testing,transport-compression-zstd --bench local_blob_reads --no-run
JAZZ_BLOB_COVERAGE=1 JAZZ_BLOB_REFERENCE=1 JAZZ_BLOB_ACCOUNT=1 \
JAZZ_BLOB_SCHEMA_TABLES=32 JAZZ_BLOB_BACKGROUND=36 \
JAZZ_BLOB_BYTES=32768,262144,4194304,16777216 JAZZ_BLOB_REPEATS=9 \
  path/to/frozen-local_blob_reads
```

For the small-case confirmation use `JAZZ_BLOB_BYTES=32768,262144` and
`JAZZ_BLOB_REPEATS=41`. This benchmark emits read-poll, owner-tick and
foreground-tick attribution in each unprofiled receipt.

Tooling-friction: keep frozen endpoint executables and run a longer negative
control when the first short sample suggests an unrelated regression.

# Direct inline scalar encoding and owned decoding

Implementation: `174079da325521251a94aa93e4d76b0bf0d49fcd`, on #2810.

Primitive scalars use ordinary enum tag 2 and a record with one trailing raw
field. The payload is exactly the logical bytes. Encoding now borrows those
bytes and calls the ordinary variant-record encoder; owned decoding validates
the logical kind and copies the payload. Maps, Value vectors, intermediate
records and decode/re-encode comparison are unnecessary for this arm. Chunked
values retain their existing path. The physical format is unchanged.

## Optimized native member fixture

Same anonymized 39 subscriptions and 27,518 visible rows, preseeded Core and
empty relay/client. Clean committed trees; no concurrent build/test during
measurements. Settling excludes seed and post-readiness diagnostics.

| Measure                       |   #2809 | This slice |
| ----------------------------- | ------: | ---------: |
| Settling                      | 25.560s |    23.881s |
| Dominant subscription ready   | 26.049s |    24.374s |
| Collection/results, exclusive |  1.655s |     1.201s |
| Storage apply, exclusive      |  2.820s |     2.664s |
| Ingestion, exclusive          |  2.590s |     2.571s |
| Output decoding, exclusive    |  2.529s |     2.351s |

About 6.6% less settling time. This is a single-run comparison, not a confidence
interval. The 5s target remains unmet.

## Native todo checkpoint

1,500 rows, exactly 1,350 batch updates. RocksDB worker and memory foreground.
The prior measured tip was #2808; this comparison includes #2809 too. It excludes
browser/IndexedDB, JS, real network latency and external authentication.

| Measure                               |    #2808 | Current stack |
| ------------------------------------- | -------: | ------------: |
| Measured batch-update roundtrip       | 364.47ms |      317.24ms |
| Batch authoring                       |  36.90ms |       30.61ms |
| Initial publication                   | 116.68ms |      111.84ms |
| Initial receiver ingestion            |  54.19ms |       46.25ms |
| Initial receiver query                |  13.76ms |       13.89ms |
| Fresh receiver ingestion after update |  64.09ms |       52.44ms |

The measured roundtrip improves about 13%; fresh ingestion after the update
improves about 18%. All exact row/update assertions pass.

Validation: Groove library 744 passed, 2 ignored; Jazz library 2,005 passed,
2 ignored. All three scaling canaries and the two-seed maintained/one-shot
oracle at depths 10 and 1,000 pass. The new byte test independently constructs
ordinary enum/record encodings for Bytes, String and JSON. Changing the fast
encoder tag makes it fail; exact restoration passes. Scoped Clippy and
formatting pass. Full canonical CI, browser acceptance and independent review
are not claimed.

Tooling friction: grouped allocation callers exposed this shared scalar path;
full-stack rankings alone scattered its cost across many call sites.

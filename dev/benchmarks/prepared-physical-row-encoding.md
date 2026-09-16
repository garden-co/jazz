# Prepared physical row encoding

Implementation: `2099f3bad42418fc463d9355d76cd537a20550a5`, on #2803.

Physical writes previously decoded every history field into owned values,
constructed enum remaps per row, and encoded everything again. Current carriers
then structurally validated the freshly encoded result. The existing physical
write plan now prepares field actions and enum remaps once. Matching unchanged
fields copy their encoded spans; timestamps and enum-bearing fields retain their
required conversions. Descriptor mismatch uses the decoded fallback.

This changes neither storage bytes nor wire semantics. Test builds compare every
new physical record with the previous value-based encoder, including its
independent per-row enum remapping. Authored-column alias validation remains.

## Optimized native measurements

Same preseeded, anonymized 39-subscription member fixture with 27,518 visible rows,
empty relay/client and RocksDB WalNoSync. Settling excludes seed and final
diagnostic queries. Both runs have identical phase instrumentation and no
concurrent build/test activity.

| Measurement                           |   #2803 | This slice |
| ------------------------------------- | ------: | ---------: |
| Settling                              | 28.181s |    27.287s |
| Dominant subscription ready           | 28.670s |    27.781s |
| Ingestion, exclusive across nodes     |  3.372s |     2.571s |
| Storage apply, exclusive across nodes |  3.066s |     2.972s |

The targeted ingestion phase improves about 24%; overall settling improves 3.2%.
These are single large-fixture runs, not a confidence interval. Phase times are
components of settling and must not be added to it.

The separate native todo workload inserts 1,500 rows and updates exactly 1,350
of them in one transaction. It exercises a RocksDB worker and memory foreground,
wire encoding, publication, ingest, reopen and exact visible-result checks.
It excludes browser/IndexedDB, JS, real network latency and external auth.

| Measurement                        |    #2803 | This slice, first run |
| ---------------------------------- | -------: | --------------------: |
| Measured batch-update roundtrip    | 411.87ms |              378.65ms |
| Batch authoring                    |  46.08ms |               37.06ms |
| Worker batch ingest                | 147.77ms |              140.10ms |
| Fresh receiver ingest after update |  74.41ms |               61.71ms |

Three additional unchanged-code roundtrips measured 375.08, 371.34 and 386.93ms;
fresh post-update ingestion measured 62.83, 62.84 and 63.70ms. The parent has only
one recorded run here. Retain provisionally: the whole todo roundtrip improves
roughly 8–10%, while the large fixture gains less. This is not a claim of browser
performance or achievement of the 5s cold-load target.

## Validation

Jazz library: 2,005 passed, 2 ignored. Groove library: 743 passed, 2 ignored.
All three incremental canaries and the two-seed maintained/one-shot oracle at
churn depths 10 and 1,000 passed. Scoped Clippy and formatting passed.
Replacing the created-at millisecond conversion with packed HLC bytes made
`version_bearing_current_source_preserves_provenance_timestamps` fail at the
byte oracle. Exact restoration passed. No mutation remains.

Full canonical CI, browser acceptance and independent review are not claimed.

Tooling friction: optimized native binaries keep the iteration narrower than
WASM rebuilds, but separate feature sets still rebuild Jazz. Allocation samples
must span the full workload rather than hit their cap early.

# Removing discarded key-descriptor preparation

Implementation: `9795973039`, on top of #2802. Groove's old
`record_store_for_table` helper discarded its key-descriptor argument. Reads and
every pending physical write nevertheless constructed one. Direct RecordStore
construction removes that work and 39 net lines, with no cache or format change.

Initial optimized measurements (same native fixtures and options as the prior
reports; no concurrent builds/tests during timing):

| Measure                                                   |    #2802 | This pass |
| --------------------------------------------------------- | -------: | --------: |
| Permissioned fixture settle, 27,518 outputs               |  28.700s |   28.181s |
| Todo initial publication + ingest + query, RocksDB worker | 231.65ms |  218.15ms |
| Todo author + persist, 1,350 updates                      |  58.66ms |   50.61ms |
| Todo full measured update roundtrip                       | 425.03ms |  411.87ms |
| Todo fresh post-update foreground ingest                  |  80.25ms |   74.41ms |

The large-fixture gain is only about 2%; the todo roundtrip gain about 3%.
These are single-run observations. Retain this change because it deletes
obsolete work and code rather than adding complexity. It is not a major step
toward the 5s permissioned cold-load target.

The todo fixture runs 1,500 rows and updates 90% in one transaction. Roundtrip
sums the measured author, persist, upload build/codec, worker ingest, updated
publication, returned codec/ingest, and foreground-query phases. It excludes
application staging, JS, browser IndexedDB, external authentication and network
latency. Exact row-ID and completed-row sets are asserted. Compared with the
older #2787 receipt, fresh post-update foreground ingestion has fallen from
about 886ms to 74ms; that comparison includes all intervening improvements.

Validation: Groove library 743 passed, 2 ignored; scoped Clippy passes. Three
incremental canaries and a two-seed maintained-vs-one-shot oracle pass. No new
behavioral tests were needed for removal of a discarded argument. Full canonical
CI, browser acceptance and independent review remain outstanding.

Next investigation under #2789: physical history/current writes still reconstruct
whole records and repeatedly prepare enum remapping despite the existing
per-schema write plan. Keep authored enum identities and catalogue invalidation
semantics while moving preparation out of per-row work.

# Separating history installation from maintained query work

This follow-up to SLIM_MEMORY.md isolates the ordinary Jazz reset-bundle bulk
installer from transport, scope receipts and upstream query serving.

## Engine control

Set `JAZZ_CUSTOMER_HISTORY_COST` to the capture/fixture directory and
`JAZZ_CUSTOMER_STORAGE=all-memory`. Captures are decoded before timing. For each
of the two actual delivered datasets, create a fresh history-complete local
Db, then compare:

- No application subscriptions: install all bundles; query all 39 tables twice.
- Active application subscriptions: initialize the 39 permissioned queries on
  the empty database, install the same bundles, refresh and drain their results,
  then query all 39 tables twice.

Report subscription setup, bulk installation, result refresh/delivery and each
one-shot pass separately. One-shot timings include preparation and materializing
all application fields. Output IDs and fields are checked against the exported
fixture after timing. Active streams must return the exact expected ID sets.
The main installer is invoked through narrow `testing`-feature helpers; its
production implementation is unchanged. Explicit facade refresh replaces the
refresh normally invoked by inbound sync handling.

The receiver is history-complete for this controlled local experiment, avoiding
transport/authority-scope dependencies. The two dataset labels identify captured
payloads (46,740 and 27,518 unique transactions); they are not claims that this
control reproduces every behavior of a production Edge or Client. The smaller
capture is treated as a closed fixture for these queries.

The active case initially exposed #2892: pending terminal edits were lost on two
evaluation continuation paths. Seven subscriptions passed; adding the eighth
lost the group result. Preserving terminal edits and root ordering windows across
those paths makes all 39 subscriptions pass for both datasets, in three rounds.
These measurements include that candidate fix. It requires a focused regression
and broader verification before release.

## Engine result

Optimized native, memory storage, three paired rounds in one process. Medians in
milliseconds; these are controlled phase measurements, not a browser prediction.

| Captured dataset    | Active queries | Setup | Install | Refresh/delivery | First read pass | Second read pass |
| ------------------- | -------------: | ----: | ------: | ---------------: | --------------: | ---------------: |
| 46,740 transactions |             No |     0 |   1,707 |                0 |           1,433 |            3,293 |
| 46,740 transactions |            Yes | 1,002 |   3,414 |              624 |             717 |              706 |
| 27,518 transactions |             No |     0 |   1,077 |                0 |           1,005 |            1,937 |
| 27,518 transactions |            Yes | 1,015 |   2,414 |              610 |             547 |              522 |

The larger no-active install ranged 1,583–1,807 ms; active install 3,244–3,540 ms.
Do not interpret small differences as established wins. Active setup is on an
empty database. Installer time includes ordinary internal table/index maintenance
and any retained-query work it drives; it is not exclusively history logic.
Refresh/delivery is additional facade work after installation.

This rejects a history-only explanation of the overhead: installation without
application queries remains substantial, but maintained query work roughly doubles
installation time and adds delivery work. Reads also remain costly. The slower
second unretained one-shot pass is an unresolved observation, not evidence of a
beneficial warm cache. Active retained queries make subsequent reads cheaper, but
paid setup and incremental-maintenance costs must remain in the accounting.

## Reference control

`JAZZ_SLIM_HISTORY=1` adds a separate per-row/per-transaction history index,
transaction-fate map and current-winner selection to the slim reference. It
reads fate metadata from the capture, retains history independently of the
current map and selects accepted candidates using transaction ordering.
An untimed sensitivity check changes one transaction to Pending: its history
entry must remain present while its accepted-current entry is absent.

The reference explicitly rejects non-mergeable transactions, parents and
deletions. It is a shallow-history control, not a general merge algorithm.
All actual fixture versions are accepted and have no parents. Both modes retain
actual encoded version records and transaction bytes, and evaluate identical
permissioned queries with full output validation.

## Reference result

Three clean optimized rounds on the same source: flat median **269.019 ms**;
explicit-history median **306.623 ms**. Edge-sized decode/install rises from
112.651 to 136.761 ms; Client-sized decode/install from 66.403 to 79.571 ms.
Query times remain roughly 30 ms per node. The reference adds approximately
38 ms total for its explicit shallow-history bookkeeping. This does not price
Jazz's general semantics or prove they can be replaced by this reference.

## Counterfactual: omit post-write merge-head rebuilding

A `testing`-only flag, `JAZZ_HISTORY_SKIP_HEAD_REBUILD=1`, deliberately skips the
post-write history reread. This is not a supported mode and does not prove it is
safe to remove: #2883 explains the partial/out-of-order history obligations.
The changed experiment premise is a fresh memory receiver without application
subscriptions, rather than the earlier full-topology phase instrumentation.

Six fresh processes in baseline/skip/skip/baseline/baseline/skip order all passed
the shallow fixture's output checks. The median sum of the two measured install
phases was 2,587 ms baseline versus 2,434 ms skipped (~6%). Individual sums ranged
2,565–2,902 ms versus 2,179–2,458 ms, so the effect is noisy. Client-sized install
medians were 992 versus 845 ms; Core-sized medians 1,595 versus 1,589 ms. Do not
claim a precise universal speedup from this small experiment. Even this deliberate
omission leaves seconds of installer work; it is not the structural unlock.

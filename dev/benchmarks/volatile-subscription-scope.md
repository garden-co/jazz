# Volatile subscription scope

Thesis and outcome log: [#2913](https://github.com/garden-co/jazz/issues/2913).
Parent: #2953, `cc6884178064b38d43129f1471c5e9e8545e2ff7`.

Persist native data; keep subscription membership, authority receipts and
resume cursors in memory. Local-first evaluates eligible retained data and
pending writes. Remote waits for a fresh snapshot after restart.
Remote-if-possible uses the same remote gate online and local knowledge only
after explicit disconnect. Identity/claims/branch isolation is unchanged.

The changed premise is semantic, not a repeat of a rejected encoding shortcut:
we no longer require reconstructing an exact former remote scope on reopen.
The rejected-experiment ledger, preserved scope/persistence branches and PR
descriptions were searched before implementation. #2953 remains an intermediate
checkpoint; future optimization is not constrained to its cache representation.

Remove scope persistence/recovery and the JSIR codec rather than leave dormant
writers or a second mode. Retire legacy scope/cursor entries without decoding
their payloads; preserve native rows/history/pending writes and separately used
policy/availability metadata. Keep reserved legacy store/profile identities for
opening existing roots, not as new encoding contracts. No wire change beyond
#2953's v2 snapshot/delta format. In-process reconnect may reuse justified body
knowledge, but process restart must not recover a cursor or delta predecessor.

Correctness focus: native/permissioned reopen, offline local reads, fresh remote
settlement, exact policy isolation, malformed/legacy cache discard, eviction,
pending-write replay, W1 resumed result deltas, and native worker/relay restart.
Assertions about deliberately removed cache persistence will change; observable
row, ordering, isolation and settlement assertions must remain.

Local-current reads select retained Global-current and Ahead-current rows, not
a node-wide timestamp cut. Reopen must not promote a partial replica's clock
from discarded subscription receipts or collapse sparse transaction knowledge
into a complete prefix. Transaction snapshots retain their existing explicit
dots outside the core frontier. The nested reopen check waits for cold storage
hydration to finish before asserting its unchanged result, rather than treating
the initial pending empty snapshot as a completed read.

Measure identical native RocksDB WalNoSync todo insert/update/batch/reopen and
permissioned cold-load workloads against sealed #2953 binaries, then CodSpeed.
Prediction: removing per-received-row cache key hashing, encoding and writes
should help cold input installation and updates, with cheaper reopen; no
end-to-end percentage is claimed before measurement. Capture negative results
as carefully as improvements. Do not merge without approval.

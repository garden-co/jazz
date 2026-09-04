# Groove invariant registry

This is the human entry point for the out-of-band Groove invariant registry.
Numbered specification chapters remain normative; these records connect their
stable `INV-<AREA>-<n>` anchors to implementation and test receipts.

The authoritative records live in the one repository-wide
[`crates/invariant-registry.jsonl`](../../invariant-registry.jsonl) file: one
canonical JSON object per physical line, sorted by invariant ID then domain.
Filter Groove records with `rg '"domain":"groove"' crates/invariant-registry.jsonl`,
or find one directly with `rg '"id":"INV-QUERY-1"' crates/invariant-registry.jsonl`.

`dev/gates/invariant-registry.sh` validates the record shape, stable IDs,
duplicate IDs, cited Rust/TypeScript tests, covered-without-test mistakes, and
a one-time parity receipt for all 143 Groove records migrated from the legacy
table. The gate commits that frozen receipt's digest and Jazz/Groove record
counts, so removing a record together with its receipt line cannot erase
legacy coverage; newly introduced JSONL records need no receipt amendment.
It reports `now` + `untested` as visible documented debt without failing.
Jazz-owned IDs live in
[`../../jazz/SPEC/INVARIANTS.md`](../../jazz/SPEC/INVARIANTS.md).

`Status` is a closed planning vocabulary validated by the gate. `Coverage` is
deliberately free-form human receipt text, not an enum: only `✓` has a machine
meaning (it requires a cited existing test). Implementation anchors are
human-maintained pointers and are not mechanically validated because the
existing corpus intentionally permits abbreviated files, multi-symbol anchors,
and planned implementations; test citations are the validated contract.

## Open Questions

None.

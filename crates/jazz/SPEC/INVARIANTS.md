# Jazz invariant registry

This is the human entry point for the out-of-band Jazz invariant registry.
Numbered specification chapters remain normative; these records connect their
stable `INV-<AREA>-<n>` anchors to implementation and test receipts.

The authoritative records live in the one repository-wide
[`crates/invariant-registry.jsonl`](../../invariant-registry.jsonl) file: one
canonical JSON object per physical line, sorted by invariant ID then domain.
Filter Jazz records with `rg '"domain":"jazz"' crates/invariant-registry.jsonl`,
or find one directly with `rg '"id":"INV-API-1"' crates/invariant-registry.jsonl`.

`dev/gates/invariant-registry.sh` validates the record shape, stable IDs,
duplicate IDs, cited Rust/TypeScript tests, covered-without-test mistakes, and
a one-time parity receipt for all 334 Jazz records migrated from the legacy
table.
It reports `now` + `untested` as visible documented debt without failing.
Groove-owned IDs live in
[`../../groove/SPEC/INVARIANTS.md`](../../groove/SPEC/INVARIANTS.md).

`Status` is a closed planning vocabulary validated by the gate. `Coverage` is
deliberately free-form human receipt text, not an enum: only `✓` has a machine
meaning (it requires a cited existing test). Implementation anchors are
human-maintained pointers and are not mechanically validated because the
existing corpus intentionally permits abbreviated files, multi-symbol anchors,
and planned implementations; test citations are the validated contract.

## Reserved ids

Some ids and ranges were allocated during drafting but are not cited by any
Jazz chapter and therefore have no record yet. A record is created only when a
chapter cites the invariant; this avoids treating deliberate numbering gaps as
dangling references.

## Open Questions

None.

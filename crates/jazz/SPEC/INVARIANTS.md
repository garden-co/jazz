# Jazz invariant registry

This is the human entry point for the out-of-band Jazz invariant registry.
Numbered specification chapters remain normative; these records connect their
stable `INV-<AREA>-<n>` anchors to implementation and test receipts.

The authoritative records live in [`invariants/`](invariants/), one readable
Markdown file per invariant ID. That avoids unrelated invariant work sharing a
large formatted-table line. Find an invariant by filename (for example,
[`INV-API-1`](invariants/INV-API-1.md)), or with `rg INV-API-1
crates/jazz/SPEC/invariants`.

`dev/gates/invariant-registry.sh` validates the record shape, stable IDs,
duplicate IDs, cited Rust/TypeScript tests, covered-without-test mistakes, and
the migrated 316-record minimum Jazz inventory. It reports `now` + `untested` as
visible documented debt without failing. Groove-owned IDs live in
[`../../groove/SPEC/INVARIANTS.md`](../../groove/SPEC/INVARIANTS.md).

## Reserved ids

Some ids and ranges were allocated during drafting but are not cited by any
Jazz chapter and therefore have no record yet. A record is created only when a
chapter cites the invariant; this avoids treating deliberate numbering gaps as
dangling references.

## Open Questions

None.

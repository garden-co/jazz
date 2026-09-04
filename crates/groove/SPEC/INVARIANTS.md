# Groove invariant registry

This is the human entry point for the out-of-band Groove invariant registry.
Numbered specification chapters remain normative; these records connect their
stable `INV-<AREA>-<n>` anchors to implementation and test receipts.

The authoritative records live in [`invariants/`](invariants/), one readable
Markdown file per invariant ID. That lets unrelated work add or amend separate
records without contending on a formatted table. Find an invariant by filename
(for example, [`INV-QUERY-1`](invariants/INV-QUERY-1.md)), or with `rg
INV-QUERY-1 crates/groove/SPEC/invariants`.

`dev/gates/invariant-registry.sh` validates the record shape, stable IDs,
duplicate IDs, cited Rust/TypeScript tests, covered-without-test mistakes, and
the migrated 138-record minimum Groove inventory. It reports `now` + `untested` as
visible documented debt without failing. Jazz-owned IDs live in
[`../../jazz/SPEC/INVARIANTS.md`](../../jazz/SPEC/INVARIANTS.md).

## Open Questions

None.

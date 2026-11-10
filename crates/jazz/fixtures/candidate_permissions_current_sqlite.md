# Candidate permissions on the pinned SQLite runtime

`ExistsIncludingCreated` remains an opt-in permission source. It adds only
independently authorised, surviving root inserts from the same serializable
unit. The unmarked executor, callback and mergeable restrictions, authenticated
policy context, bounded dependency walks and final-state revalidation remain.

The historical `candidate_permissions_v1.md` describes the original owner’s
encodings. This port uses the current runtime’s existing catalogue and row
formats. It does not restore the retired catalogue payload codec or read an old
catalogue payload as a current bundle.

Current catalogue definitions use `jazz::catalogue::canonical_json`, including
its exact round-trip check and sorted object keys. The policy retains the named
JSON variant `ExistsIncludingCreated` with `table` and `condition`. The old
server’s binary policy tag 22 is historical; it is not a new tag in the current
catalogue. Current consumers without the named variant refuse it. The original
model fixtures still pin the eight-field JoinVia representation, old target
tags 0–2, new target tags 3–4 and canonical marker bytes `61 01`.

The current row format identifies a version by clock and writer. Candidate
sources retain that writer, use the captured accepted position, and exclude
as-sent shadows when selecting history. A pending version from another writer
cannot stand in for the publication being checked.

The derived UUID-array role is `jazz.sqlite.uuid-array-index.v2`. Its keys are
member UUID, current/history kind, row UUID, branch, clock and writer. Triggers
include state changes and omit as-sent shadows. Installation, backfill and
subsequent row writes remain atomic. `sqlite_uuid_array_index_v2.sql` pins the
new DDL. The v1 fixture remains byte-for-byte intact for its historical format;
v1 sequence keys are not reinterpreted as v2 clock/writer keys. This derived
role does not migrate an old row store or grant support for an otherwise
unsupported storage format.

Only the marked candidate executor prepares logical large-value comparisons.
It obtains exact cells through the existing indexed dependency walk from
captured policy roots, then uses the ordinary chunk reader. Ordinary queries
keep the current cell-comparison contract in SPEC 19.5. Preparation answers
live in connection-local TEMP scratch data, never durable or wire bytes. An
unprepared pair cannot pass through missing-answer NULL equality. Provisional
reads that would need this preparation still refuse explicitly. The proof-step
and SQLite instruction limits are not a byte or memory bound.

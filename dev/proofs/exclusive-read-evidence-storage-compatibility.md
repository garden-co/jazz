# Pending read evidence on the current SQLite runtime

## Decision

The current port preserves proof while an exclusive transaction is pending,
including reopen before its first terminal decision. It preserves validation,
transaction identity, terminal fate, idempotence and rejected-operation handling.
It does not add permanent post-settlement read-proof retention.

The historical #4152/#4156 proposal retained proof after settlement and introduced
a Groove epoch-two admission protocol. The bounded current-consumer assessment
found no demonstrated consumer or causal failure requiring that contract. The
user chose to omit it. The old proposal remains in Git history; it is not the
current implementation or a tested compatibility guarantee.

## Mixed versions

This port introduces no SQLite storage-format change, epoch-two manifest,
JSA1 admission receipt or legacy-writer migration. It uses the current SQLite
pending-proof and terminal-fate paths. Historical Groove images are not silently
admitted as current SQLite stores. An incompatible image must refuse without
publishing replacement metadata; already missing proof is not reconstructed
from a resend or from later observations.

## Security and correctness

Pending evidence remains bound to the original transaction and is validated
before acceptance. A retry must not replace stale reads with fresh observations,
convert the transaction into a mergeable write, or overwrite a known terminal
fate. Missing evidence is distinct from an empty read set. A settled transaction
is not promised a permanently queryable copy of its original read proof.

Creation-aware policy admission is the separately owned current-core prerequisite
#4374. Its authenticated governing schema, same-unit witness qualification,
read-set validation and atomic rejection do not depend on permanent proof history.

## Evidence

The current E13 qualification packet records actual pending/reopen and fate
controls at their execution heads. Historical codec fixtures and original test
bodies remain preserved source evidence, not claims that removed Groove modules
execute in the current SQLite workspace. No format migration is claimed.

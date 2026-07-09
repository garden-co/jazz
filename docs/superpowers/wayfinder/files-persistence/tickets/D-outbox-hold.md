# Outbox hold across restart

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [C — upload-resume records](C-resume-records.md)

## Question

How does "the file-writing commit unit holds at the outbox until release,
while independent units bypass it" survive a restart?

The in-memory outbox is not durable; durability lives in local batch
records / sealed submissions / batch fates keyed by `BatchId`
(`crates/jazz-tools/src/batch_fate.rs`), which restart recovery already
scans. Decide: how a held unit is represented — a new field/state on
`LocalBatchRecord`, a distinct record kind, or derivation from the presence
of an unreleased resume record (ticket C); how restart recovery
re-establishes holds and re-attaches them to in-flight uploads; how release
transitions the record into the ordinary submission path exactly once
(idempotent under crash-during-release); and how causally-dependent
transactions queue behind a held unit across restarts.

Blocked by C because the natural representation may simply reference the
resume record, and release is driven by its state machine.

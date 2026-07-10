# Outbox hold across restart

Type: `wayfinder:grilling`
Status: closed (resolved 2026-07-10, by the invisible-core pivot in
[B — device file store](B-staging-store.md))
Assignee: guido
Blocked by: [C — upload-resume records](C-resume-records.md)

## Question

How does the SDK-courtesy outbox hold — "a `fromBlob`-carrying commit unit
holds until its upload completes, while independent units bypass it" (per
[Descriptor persistence](A-descriptor-persistence.md), a client-side
courtesy, not a server gate) — survive a restart?

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

## Resolution (2026-07-10)

**It doesn't survive restart — by design.** The invisible-core pivot
(resolved in [B — device file store](B-staging-store.md)) removed all
durable upload state from core: the hold is an in-memory courtesy only.
After a restart, formerly-held transactions enter the ordinary submission
path and sync normally; with no staged body and no resume machinery in
core, the descriptor simply syncs bodyless and its URL 404s until a
future opt-in offline package (out of this map's scope) reinstates
durable holds as part of its resume story.

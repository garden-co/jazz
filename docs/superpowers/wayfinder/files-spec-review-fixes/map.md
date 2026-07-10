# Files spec review fixes — wayfinder map

Label: `wayfinder:map`
Tracker: local-markdown (this directory; tickets are files under `tickets/`)

## Destination

The spec pair — files PRD (`docs/superpowers/specs/2026-07-09-files-spec.md`)
and slice-1 (`docs/superpowers/specs/2026-07-10-files-persistence-spec.md`) —
amended until every finding of the 2026-07-10 three-model review
([consolidated](notes/review-consolidated.md)) is either **fixed** or
**explicitly accepted** as a stated semantic: review-clean, ready for
implementation tickets. All three tiers in scope (criticals/majors,
decision-bearing minors, doc-hygiene).

## Notes

- **Plan-don't-do override:** each ticket's resolution applies its
  amendments to the spec pair directly, in the same commit — the spec edits
  ARE the deliverable, as on the predecessor map.
- Review assets: [consolidated](notes/review-consolidated.md) (the working
  list), raw reports [opus](notes/review-opus.md), [glm](notes/review-glm.md),
  [deepseek](notes/review-deepseek.md).
- Predecessor map (all context on how the design got here):
  `docs/superpowers/wayfinder/files-persistence/map.md` — invisible-core
  pivot, MVP delete cut, opt-in offline package out of scope.
- Skills: `/grilling` + `/domain-modeling` for decision tickets;
  `/research` for the backend matrix.
- Standing preference: keep artifacts in-repo; terse, decision-dense
  grilling.

## Decisions so far

<!-- one line per closed ticket: gist + link -->

- [Backend conditional-write support matrix](tickets/A-backend-support-matrix.md)
  — conditional `CompleteMultipartUpload` is not portable (S3/MinIO yes,
  R2 ambiguous, Tigris undocumented) but conditional single-part PUT is;
  nosniff must be a deployment-layer requirement (only Tigris emits it
  natively); MPU-abort lifecycle also non-portable (S3/R2 only). Full
  matrix with citations: [backend-support-matrix](notes/backend-support-matrix.md).

## Not yet specified

- Whether to re-run the review panel after the sweep closes — a
  verification pass whose shape (full three-model panel vs one targeted
  check) depends on how the fixes land.

## Out of scope

- Anything reopening the invisible-core pivot, the MVP delete cut, or the
  opt-in offline package (predecessor map decisions; reviews accepted
  them).
- PRD/slice-1 user-story renumbering or a story mapping table (DeepSeek
  minor — accepted noise, not worth the churn).

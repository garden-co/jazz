# Branch deletion witnesses need branch register current

## What

Branch read-view subscriptions are capability-gated because the mainline `jazz_{table}_register_*_current` indexes do not represent branch deletion currency.

## Priority

high

## Notes

- Branch witness support needs a branch-aware register-current source: an arg-max over branch-scoped register rows layered over the base snapshot.
- `deletion_register_source_for_request` is the single resolver boundary where that source should be wired.
- Once the branch-aware source exists, remove the `SourceGap::BranchOverlay` gate for deletion register metadata.
- One-shot branch reads keep using branch current rows; maintained branch read-view subscriptions remain capability-gated until deletion witnesses can be served correctly.

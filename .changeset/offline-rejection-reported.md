---
"jazz-tools": patch
---

An offline write that a tightened deployment refuses is now reported. Waiting for a write at the `local` tier no longer counts as handling a rejection that arrives after the wait resolved, so `onMutationError` fires when the authority later refuses it and the row reverts. A later write built on the refused one now fails with the refused write's cause, for example "the write was built on a write the authority rejected (permission denied: …)", instead of a bare "built on a write the authority rejected".

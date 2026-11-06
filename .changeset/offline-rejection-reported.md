---
"jazz-tools": patch
---

An offline write that a tightened deployment refuses is now reported. Waiting for a write at the `local` (or `edge`) tier no longer counts as handling its rejection, so `onMutationError` fires when the authority later refuses it and the row reverts. A later write built on the refused one now fails with the refused write's cause, for example "the write was built on a write the authority rejected (permission denied: …)", instead of a bare "built on a write the authority rejected".

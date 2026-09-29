---
"jazz-tools": patch
---

Fix `jazz-tools migrations create` recovering a stale lock by taking a live one. When several generators ran against the same migrations directory, one could mistake a freshly acquired lock for the dead owner it had just read, move it aside and fail with "Quarantined migration lock owner did not match", letting two generators run at once. Stale-lock recovery now only ever removes the exact lock instance it observed.

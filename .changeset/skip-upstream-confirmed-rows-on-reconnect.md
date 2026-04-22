---
"jazz-tools": patch
---

Stop replaying upstream-confirmed rows to the server on reconnect. The client's full-storage sync now skips rows whose `confirmed_tier` is already above the node's own tier, so a user-role client no longer re-pushes subscription-delivered rows it never authored. Previously these replays were rejected by row-level update policies (e.g. "Update denied by USING policy — cannot see old row") on every reconnect.

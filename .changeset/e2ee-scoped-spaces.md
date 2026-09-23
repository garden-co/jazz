---
"jazz-tools": minor
---

Add explicitly configured scoped spaces, atomic account/group initial recipients,
signed grants and rotations, epoch history, independent recovery, permanently sealed
lineages, and interrupted initial delivery resumption. Deterministic scope-bound root
IDs and exclusive exact-row preconditions prevent competing or hidden roots. Reads
require live authority settlement; encrypted ordinary operations and durable offline
accepted-history persistence are separate layers.

Expose `spaceSchema` through `jazz-tools/e2ee` for application composition, and use
the consolidated recovery decoder for space status. Replay checks each root's
deterministic ID. Recovery status and device reads propagate full-history failures
rather than reporting them as unavailable deliveries.

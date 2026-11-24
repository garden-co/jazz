---
"jazz-tools": patch
---

**Swift: leaving a watch stream no longer deadlocks.** Ending iteration of `JazzSession.snapshots`, `clients`, `Client.session`, `syncChanges` or `mutationErrors` while jazz-ffi was delivering a state into that stream could hang both threads: the stream's termination waited for the delivery, and the delivery waited for the termination. The watch is now cancelled off the terminating thread. This hung `JazzSession.clients` intermittently when the user changed.

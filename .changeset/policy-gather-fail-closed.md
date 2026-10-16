---
"jazz-tools": patch
---

Reject hand-written relation policies whose `Gather` the permission engine would have lowered to broader access than the relation describes: a Gather on the right side of a join, column frontier or dedupe keys, recursive or seed hops that are not an inner join onto the target row id, a step that projects anything but the target row id, and a seed keyed by a claim comparison other than equality. Policies built with the `jazz-tools` permission builder never produce these shapes. A stored schema containing one now fails to compile when it is published or loaded, with an "is not supported in policies yet" error, instead of granting the wider access.

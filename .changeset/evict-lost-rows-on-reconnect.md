---
"jazz-tools": patch
---

Fix a client keeping rows after it lost read access to them. Two cases: a row the client wrote itself, when another client's write took its access in the same server tick, came back after the server evicted it; and rows the client held when it reconnected were not evicted when a write (its own offline write included) took its access while no live query listed them.

---
"jazz-tools": patch
---

Keep core ticks running while a foreground lease handoff drains started native work. The handoff closed the runtime before reading its transaction-time high-water, which stopped all core ticks; a native future that held the node lock until a tick completed its host continuation then never released it, so the handoff, and the client shutdown behind it, waited forever. Mutation admission still closes at the start of the handoff.

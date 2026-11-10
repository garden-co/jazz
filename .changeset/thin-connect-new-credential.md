---
"jazz-tools": patch
---

A thin client connected again with a different server URL or credential now ends any write that is waiting to be resent as `outcome_unknown`, as `updateAuth` already does, instead of resending it as someone else. An open connection bound to the old credential is also replaced: before, `connect()` kept it.

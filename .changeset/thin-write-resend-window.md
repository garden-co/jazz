---
"jazz-tools": patch
---

Thin clients now resend a write lost with its connection for at most 10 minutes after it was first sent. If no server answers within that window, the write ends as `outcome_unknown` instead of waiting to go out again, even while the client is offline. The authority no longer keeps the resend record of a thin write to a latest-only table forever: it drops it an hour after recording it, so storage stops growing with every such write. Thin writes to tables with history keep their record, which is also their history.

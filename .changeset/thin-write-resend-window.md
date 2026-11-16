---
"jazz-tools": patch
---

Thin clients now resend a write lost with its connection for at most 10 minutes after it was first sent. If no server answers within that window, the write ends as `outcome_unknown` instead of waiting to go out again, even while the client is offline. The authority no longer keeps the resend record of such a client's thin writes to latest-only tables forever: it drops it an hour after recording it, so storage stops growing with every such write. Thin writes to tables with history keep their record, which is also their history. Thin clients from earlier releases, which resend without a time limit, keep their records too, so a late resend from them is still applied only once. While servers are upgraded one by one, a thin write resent through a server on the other version can be applied twice: upgrade every server of an app together, or accept that for the rollout.

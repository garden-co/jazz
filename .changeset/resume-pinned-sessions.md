---
"jazz-tools": patch
"jazz-napi": patch
---

Clients whose sessions are pinned to a permissions bundle, which is every `createJazz` client, now resume their subscriptions from where they last settled when they reconnect, instead of being sent every row again. A client stamps each stored watermark with the bundle its session was pinned to, and the server resumes only when the reconnecting session is pinned to the same bundle; under another bundle the read opens fresh, so rows the new permissions hide are evicted and rows they newly allow arrive.

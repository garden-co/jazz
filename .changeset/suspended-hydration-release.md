---
"jazz-tools": patch
---

Fix query nodes leaking when a subscription was released while its first results were still loading from storage. A subscription opened on the same data during that window could also lose nodes it still needed. Both now keep exactly the nodes that live subscriptions use.

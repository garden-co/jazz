---
"jazz-tools": patch
---

Server subscriptions to the same query now share one compiled query graph even when their parameters differ, for example each client subscribing to a different chat id. Previously each distinct parameter value compiled its own copy of the graph, so server memory grew with every open subscription. Each subscriber still receives only its own rows, filtered by its permissions.

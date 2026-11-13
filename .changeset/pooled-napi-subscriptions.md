---
"jazz-tools": patch
"jazz-napi": patch
---

Node runtimes now read all their subscriptions in one native call per pump instead of one call per subscription. With many live queries, the main thread spends far less time checking subscriptions that have nothing new. A subscription whose update fails to encode still fails alone, and now receives the events that came before the failure.

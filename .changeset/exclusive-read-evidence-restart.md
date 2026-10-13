---
"jazz-tools": patch
---

Exclusive transactions that were still waiting for the server when the app restarted now replay with their original read evidence, so they are accepted instead of being rejected as `exclusive_conflict`. A transaction whose data really changed meanwhile is still rejected, and transactions written by older versions still behave as before.

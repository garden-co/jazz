---
"jazz-tools": patch
---

Transactions now read other nodes' unsynced writes that plain reads already return, including a previous node's writes after a runtime reopens as a new node.

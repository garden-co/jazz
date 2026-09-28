---
"jazz-tools": patch
---

Servers measure an outgoing sync message's size without encoding it a second time, and fan-out to subscribers no longer copies each update for the last recipient. This mainly saves memory when opening large subscriptions. The wire format is unchanged.

---
"jazz-tools": patch
---

A live subscription now keeps reporting the last result the server confirmed (at its tier) while the connection is down, instead of reporting it unsettled on some updates and not others. Treat a result as current when it is confirmed and the link is up. Each new connection, including a transport swapped in without a disconnect, starts every read over, so an answer or refusal from an earlier connection no longer carries over. After a reconnect, the client also drops policy support rows the reader lost access to while offline, including rows deleted meanwhile, such as a deleted membership that still granted local reads (#5097).

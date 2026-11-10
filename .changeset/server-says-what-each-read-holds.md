---
"jazz-tools": patch
"jazz-napi": patch
---

A row one subscription stops listing now stays on the device while another live subscription still holds it, whether as a result row or as a row it pulls in through an include, a nested array, a join, a gather edge, a policy input, or a union arm. Previously, after a reconnect, a row could disappear from a subscription that still needed it, because the server would not send it again. The server now tells the client which rows each live read holds, using a new `Holds` sync message sent with each read's opening and whenever that changes. The client drops a row only when no live read holds it. This adds a message to the wire, so clients and servers must upgrade together.

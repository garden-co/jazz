---
"jazz-tools": patch
---

Avoid cloning the entire per-object sent-batch set when syncing. The forwarding walk and the server and client queue paths cloned the whole `sent_batch_ids` set just to test membership of a single batch. Since that set grows with a row's history, forwarding a frequently-updated ("hot") row did work proportional to its accumulated history on every update. Membership is now checked by borrow, so forwarding is independent of how much history has already been sent.

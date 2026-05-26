---
"jazz-tools": patch
---

Avoid cloning the entire per-object sent-batch set on every queued row when syncing. The server and client queue paths cloned the whole `sent_batch_ids` set just to test membership of a single batch, making a forward of N batches O(N²). The membership is now checked by borrow, so forwarding a row with a long history is linear.

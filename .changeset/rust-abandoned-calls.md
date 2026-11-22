---
"jazz-tools": patch
---

Rust: a call whose caller stops waiting (its future dropped, as `tokio::time::timeout` drops it) loses no bookkeeping.

- A durable write (`jazz.durable(tier)`: insert, update, delete, upsert, restore, transactions, `OpenTransaction::commit`, streaming writes) whose caller stopped waiting may still apply, and a refusal of it is still reported: to `mutation_errors()` and to the next `wait_for_sync()`, as a plain write's refusal is. Before, it was reported nowhere.
- Once begun, `Jazz::close(timeout)` and `Jazz::delete_client_storage()` run to their end even if their caller goes away: the client stops and lets go of its store, and the store is deleted. Before, an abandoned close left the client running and its store held.

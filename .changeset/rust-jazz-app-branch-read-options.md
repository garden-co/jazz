---
"jazz-tools": patch
---

**Rust: a branch option on reads.** `ReadOptions::branch(view)` reads through a branch view, as TS's per-call `branch` and `base` query options: `jazz.all_with_options(query, ReadOptions::default().branch(Branch::from("draft").over("main")))`, and the same for `subscribe_with_options`. A frozen base (`over_snapshot`) and the other read options combine with it.

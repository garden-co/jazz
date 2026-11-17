---
"jazz-tools": patch
---

**Rust: a branch option on reads.** `ReadOptions::branch(view)` reads through a branch view, as TS's per-call `branch` and `base` query options: `jazz.all_with_options(query, ReadOptions::default().branch(Branch::from("draft").over("main")))`, and the same for `subscribe_with_options`. A frozen base (`over_snapshot`) and the other read options combine with it.

**Changed:** `OnBranch::all`, `one`, `get` and `subscribe` (from `Jazz::on_branch`) now read at the client's default tier, the same one `Jazz::all` and `Jazz::subscribe` use. On a backend or backend client that means the server's answer (`Remote`), where they used to read local data first. `all_with(query, tier)` and `subscribe_with(query, tier)` still read at the tier you give them.

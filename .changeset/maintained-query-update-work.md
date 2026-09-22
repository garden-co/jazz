---
"jazz-tools": patch
---

Reduce work for maintained query updates by deriving unbounded-window membership from changed records and constructing root-position maps only for outputs that use them. Reuse proven equivalent witness payloads across maintained roles while keeping incompatible executions isolated.

[PR #2921](https://github.com/garden-co/jazz/pull/2921), [PR #2922](https://github.com/garden-co/jazz/pull/2922), [Witness reuse implementation](https://github.com/garden-co/jazz/commit/611b30c4a8b9d0788a0d39d514b8002da529c48a).

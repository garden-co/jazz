---
"jazz-tools": patch
---

Fix `useAllSuspense` and `useOneSuspense` reporting a hook-order change (and a recoverable concurrent-render error) when their first result arrives before React yields.

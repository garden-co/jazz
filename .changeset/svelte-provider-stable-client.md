---
"jazz-tools": patch
---

Svelte: `JazzProvider` no longer remounts the app when the session republishes its snapshot with the same client. The in-place local-first upgrade (`client: () => import("jazz-tools/local-first")` without `localFirst: "now"`) publishes one, and the remount reset component state such as a half-typed form (#5459).

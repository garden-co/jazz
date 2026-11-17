---
"jazz-tools": patch
---

Breaking: `vercel()` takes a required `region`, the Vercel region its cores' Sandboxes run in and their Drives keep the data in. It used to default to `iad1`, a lasting choice made implicitly: a core's region is fixed when it is created. `vercel()` without `region` throws when it is called, so `next dev` reports it too, with the fix to copy. The region isn't taken from the function's `VERCEL_REGION` or the project's settings, which would let whichever deployment creates a core first pick its region.

```ts
vercel({
  namespace: ({ environment, branch }) =>
    environment === "production" ? "production" : perBranch(branch),
  region: "iad1",
});
```

Migration: add `region: "iad1"`, where earlier versions created every core. Sandbox and Drive names don't depend on the region, so your existing cores and their data stay as they are.

**Fixed:** a `region` that didn't match an existing core was silently ignored (the Sandbox is found by name, and `region` only applied when creating it). It now throws, naming the core's region, since moving a core to another region isn't supported yet (https://github.com/garden-co/jazz/issues/5441); the same holds for a core's Drive left in another region. The backend also warns once per process when the function runs in another region (`VERCEL_REGION`) than its core, since every request then crosses regions. `s3()` checks its bucket's region against the configured `region` instead of `iad1`.

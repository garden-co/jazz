---
"jazz-tools": patch
---

Breaking: `vercel()` takes a required `namespace`, which replaces `name`. A deployment's core is the Sandbox `jazz-<project>-<namespace>`. `namespace` is a string, or a function of the deployment's `{ environment, branch }`, where `environment` is `production`, `preview` or a custom environment's slug. `vercel()` without one throws when it is called, so `next dev` reports it too, and the error lists the usual choices:

```ts
vercel({ namespace: ({ environment }) => environment }); // one core per Vercel environment
vercel({
  namespace: ({ environment, branch }) =>
    environment === "production" ? "production" : perBranch(branch),
}); // production's core and one per preview branch
```

Migration: earlier versions created production's core and one core per preview branch by default. The second choice, with `perBranch` from `jazz-tools/backend`, keeps those exact Sandbox and Drive names, so your existing cores and their data stay as they are. `vercel({ name })` now throws, saying how to migrate. A full Sandbox name `name: "jazz-<project>-<rest>"` becomes `namespace: "<rest>"`. A preview without a Git branch (deployed from the CLI outside a Git checkout, or through the API) has no `perBranch` namespace, so return one of your own for it, e.g. `perBranch(branch) || "cli-previews"`. A namespace that is empty, or too long for Vercel's 128-character Sandbox names after the `jazz-<project>-` prefix, is refused with an error that says so. **Fixed:** on earlier versions, per-branch preview cores of branches whose slug is longer than about 4 characters failed when their Drive was created: Vercel caps Drive names at 64 characters, and the Drive was `<core name>-data`. A Drive name that would be longer is now shortened deterministically (the core name's first part and a hash of the whole), so those previews work. Every core whose Drive could be created before keeps its Drive name.

Each store now logs one line, once per process, saying what it opened: `jazz: store vercel namespace "production" (core jazz-prj-…-production)`, `jazz: store sqlite at .jazz/local`, `jazz: store memory …` or `jazz: store remote at <url> (app <id>)`.

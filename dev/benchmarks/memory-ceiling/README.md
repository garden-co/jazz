# Memory ceiling

How much data an example app can hold before the sync server or a client
crosses a fixed memory budget, in the app's own units: "416 labels (39,000
releases, …)", not "N rows". Speed benchmarks say how fast a query is; this
says when a tenant's server is OOM-killed.

```sh
# Measure the shipped release: install it once, outside the workspace.
mkdir -p /tmp/jazz-release && cd /tmp/jazz-release && echo '{"private":true}' > package.json
npm install jazz-tools@alpha jazz-napi@alpha
chmod +x node_modules/jazz-tools/bin/native/jazz-tools-linux-x64
cd -

node dev/benchmarks/memory-ceiling/run.mjs \
  --app examples/big-label/benchmarks/memory-ceiling.ts \
  --packages /tmp/jazz-release/node_modules \
  --out /tmp/memory-ceiling
```

Nothing is compiled: the server is the release's `jazz-tools server` binary
and the clients use its jazz-napi runtime. `--packages` points the app's
`jazz-tools` imports at that release, so the app's own `schema.ts` and
`permissions.ts` load unchanged under Node's type stripping (Node 22.18+).
Leave it out to use the workspace packages after `pnpm build:core`; pass
`--server-bin` for another server binary.

`.github/workflows/memory-ceiling.yml` runs it nightly against the `alpha`
dist-tag and on demand against any version or pkg.pr.new build. It is not a
CodSpeed workload: the answer is a count of app units from a search that runs
for up to an hour, not a timed function.

## Budgets

| Side   | Default                                                        | Why                                                                                                                                                                                                                                                                                                                                                                                  |
| ------ | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Server | 1 GiB (`--server-budget`, `MEMCEIL_SERVER_BUDGET`)             | The default tenant pod memory limit: `CORE_BASELINE.limits.memory` in garden-co/infra's tenant-manager (raised from 512Mi after OOMs in Sep/Oct 2026; request 256Mi). One app runs with a 4Gi override; pass `--server-budget 4GiB` to see its headroom.                                                                                                                             |
| Client | 512 MiB of growth (`--client-budget`, `MEMCEIL_CLIENT_BUDGET`) | One browser tab's Jazz client. Mobile Safari and Chrome on Android kill tabs at around 1–1.5 GB in total, and the page, its JS heap and the DOM need their share; 512 MiB for Jazz's data is the most a tab can give before low-memory phones reload it. Measured as the client process's RSS growth from before it connects, so Node's own baseline (about 120 MiB) does not count. |

The server's RSS is what the kernel's OOM killer compares with the pod
limit, so that is what is measured (sampled every 50 ms, and `VmHWM` for the
true peak of a fresh process).

## What a run does

For each scenario the adapter declares:

1. Starts the server binary as tenant pods do (`jazz-tools server <app>
--data-dir …`, `NODE_ENV=production`, `MIMALLOC_ARENA_RESERVE=0`, heap
   sampling at the shipped default) and deploys the app's schema and
   permissions.
2. **Grows** the data in steps (×1.5 by default) through the app's schema:
   a writer process replays the adapter's transactions. By default growth
   goes through the trusted backend path (`--writes import`), as an app's
   importer or demo-data route writes; `--writes app` sends every write as
   the user the adapter names, through the policies, which is far slower.
   The server's peak while importing is the **import** side.
3. **Probes** user writes at that size: `--probe` (20) of the adapter's
   everyday edits, one at a time, as the user, through the deployed
   permissions, after loading the rows they change as the app would. Records
   the server's peak (the **edit** side), and time and server CPU per edit.
4. **Restarts** the server on the data (crash loops happen on restart), then
   opens the app as its users do: one client process runs the adapter's
   representative queries as one user (the **client** side: its RSS growth),
   then a crowd of `--crowd` (8) other users reconnects. The server's peak
   across restart and reconnects is the **cold** side.
5. **Churn**: the same user opens the app and leaves `--churn` (3) more
   times; whatever the server still holds after each departure is per-session
   memory it never gives back.
6. Stops at the first step where any side crosses its budget, then finds the
   ceiling: the import crossing to a tenth of the step from the samples it
   took while writing, and the edit, cold and client crossings by bisecting
   (`--bisect`, 3 rounds) on a copy of the last data under budget.

Heap profiles (`GET /debug/pprof/heap`) are saved after every import, cold
load and disconnect, and summarized by allocation site (the nearest frame
that is not allocator plumbing). They only see Rust allocations: RocksDB's
memtables and block cache are in RSS but not in the profile.

Output, under `--out/<scenario>/`: `summary.md` (the table the workflow posts),
`summary.json`, `steps.json` (every measurement), `*.pb.gz` (open with
`go tool pprof`), and the server and worker logs.

## Writing an adapter

An adapter is a module next to the app, default-exporting:

```ts
export default {
  name: "big-label",
  app,          // the app's `defineApp(...)`
  permissions,  // the app's `definePermissions(...)`
  scenarios: {
    labels: {
      unit: "labels",                    // what the ceiling is counted in
      start: 16,                         // first step
      describe: (n) => ({ labels: n, releases: … }), // domain counts at n
      *transactions(from, to) { … },     // growth from n=from to n=to
      signIn: (actor) => [query, …],     // what the app loads first for a user
      probe: (n, count) => …,            // `count` user edits at size n
      client: (n) => ({ actor, reads }), // one user opening the app
      crowd: (n, size) => [{ actor, reads }, …],
    },
  },
};
```

A transaction is `{ actor: "backend" | accountId, ops: [{ table, id, values,
update? }] }`. `actor` is who the app would write it as: `"backend"` for the
trusted bootstrap path, otherwise the account a user session acts for
(`session.user.account` in the policies). Transactions must be a
deterministic function of the scale, so a bisection can rebuild any size,
and should be as small as the app's own writes are. `reads` are the app's
own query builders (`app.releases.where(…).limit(…)`), run with `db.all`;
start them with what the app loads first (BigLabel's label switcher), as
`signIn` does for the probe's edits: on jazz-tools 2.0.0-alpha.59 a new
session's first policy-filtered reads can come back empty until it has loaded
the rows its read policy depends on.

See `examples/big-label/benchmarks/memory-ceiling.ts`. Then add the adapter's
path to the workflow's `adapters` default.

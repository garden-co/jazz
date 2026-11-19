// Items 2-3: StagePlan through a native backend (Go `go-backend` or Rust `endpoint_server`), TS client, two users.
// Usage: SERVER=http://127.0.0.1:<p>/jazz JWKS=http://localhost:8690 APP_BINDINGS=<abs path to the app's jazz.generated.ts> npx tsx smoke-stageplan.ts
// (run with jazz-tools resolvable from this dir, at the same build as the backend)
import { createJazzClient } from "jazz-tools/client";
if (process.env.DEBUG_FETCH) {
  const f = globalThis.fetch;
  globalThis.fetch = (async (u: any, i: any) => {
    const r = await f(u, i);
    console.error(
      "FETCH",
      (u as any)?.method ?? i?.method ?? "GET",
      (u as any)?.url ?? String(u),
      r.status,
    );
    return r;
  }) as any;
}
if (process.env.DEBUG_FETCH && (globalThis as any).WebSocket) {
  const W = (globalThis as any).WebSocket;
  (globalThis as any).WebSocket = class extends W {
    constructor(u: any, p: any) {
      super(u, p);
      console.error("WS open", String(u));
      this.addEventListener("close", (e: any) => console.error("WS close", e.code, e.reason));
      this.addEventListener("error", () => console.error("WS error"));
    }
  };
}
const SERVER = process.env.SERVER!,
  JWKS = process.env.USERS_OIDC ?? "http://127.0.0.1:8791";
const { app } = await import(process.env.APP_BINDINGS!);
const tok = async (sub: string) =>
  (await fetch(`${JWKS}/token?sub=${sub}&aud=jazz:stageplan-users`)).text();
const results: string[] = [];
const check = (n: string, ok: boolean, d = "") => {
  results.push(`${ok ? "PASS" : "FAIL"} ${n}${d ? " — " + d : ""}`);
  console.log(results.at(-1));
};
async function client(sub: string) {
  const t = await tok(sub);
  const s: any = await createJazzClient({ serverUrl: SERVER, bearer: () => t } as any);
  await s.refresh?.();
  return s;
}
const refused = async (p: Promise<any>) => {
  try {
    await p;
    return false;
  } catch {
    return true;
  }
};
const a = await client("alice-" + Date.now()),
  b = await client("bob-" + Date.now());
const aAcc = a.getSnapshot().user?.accountId,
  bAcc = b.getSnapshot().user?.accountId;
check(
  "both users signed in with distinct accounts",
  !!aAcc && !!bAcc && aAcc !== bAcc,
  `${aAcc} / ${bAcc}`,
);
const show: any = await a.db
  .insert(app.shows, {
    name: "Gate show",
    venue: "Hall",
    date: "2026-12-01",
    doors: "19:00",
    chiefAccount: aAcc,
  })
  .wait({ tier: "global" });
const showId = show.value?.id ?? show.id;
await a.db
  .insert(app.tasks, { showId, title: "Rig lights", status: "todo", assigneeId: null, rank: 1 })
  .wait({ tier: "global" });
await a.db
  .insert(app.checklistItems, { title: "Bring tape", done: false, ownerAccount: aAcc })
  .wait({ tier: "global" });
check(
  "alice reads her show, task, checklist item",
  (await a.db.all(app.shows, { tier: "remote" })).length === 1 &&
    (await a.db.all(app.tasks, { tier: "remote" })).length === 1 &&
    (await a.db.all(app.checklistItems, { tier: "remote" })).length === 1,
);
check(
  "bob reads none of alice's shows",
  (await b.db.all(app.shows, { tier: "remote" })).length === 0,
);
check(
  "bob reads none of alice's tasks",
  (await b.db.all(app.tasks, { tier: "remote" })).length === 0,
);
check(
  "bob reads none of alice's checklist items",
  (await b.db.all(app.checklistItems, { tier: "remote" })).length === 0,
);
check(
  "bob can't insert a task into alice's show",
  await refused(
    b.db
      .insert(app.tasks, { showId, title: "x", status: "todo", assigneeId: null, rank: 2 })
      .wait({ tier: "global" }),
  ),
);
check(
  "bob can't create a show with alice as chief",
  await refused(
    b.db
      .insert(app.shows, { name: "fake", venue: "v", date: "d", doors: "d", chiefAccount: aAcc })
      .wait({ tier: "global" }),
  ),
);
check(
  "bob can't update alice's show",
  await refused(b.db.update(app.shows, showId, { name: "hijack" }).wait({ tier: "global" })),
);
const fail = results.some((r) => r.startsWith("FAIL"));
console.log(fail ? "STAGEPLAN FAIL" : "STAGEPLAN PASS");
process.exit(fail ? 1 : 0);

// Reference check (TS) for item 2: trusted + scoped backend clients and forUser privacy.
// The Rust/Go examples are checked against the same expectations.
// Env: SERVER (http://127.0.0.1:8780/jazz), OIDC (http://127.0.0.1:8790), APP_DIR, BACKEND (http://127.0.0.1:8780)
import { createJazzClient } from "jazz-tools/backend";
import { readFileSync } from "node:fs";
const SERVER = process.env.SERVER ?? "http://127.0.0.1:8780/jazz",
  OIDC = process.env.OIDC ?? "http://127.0.0.1:8790",
  APP_DIR = process.env.APP_DIR!,
  BACKEND = process.env.BACKEND ?? "http://127.0.0.1:8780";
const { app } = await import(`${APP_DIR}/jazz.generated.ts`);
const permissions = JSON.parse(readFileSync(`${APP_DIR}/jazz.permissions.json`, "utf8"));
const tok = (sub: string, aud = "jazz:stageplan-gate") =>
  fetch(`${OIDC}/token?sub=${sub}&aud=${aud}`).then((r) => r.text());
const results: string[] = [];
const check = (n: string, ok: boolean, d = "") => {
  results.push(`${ok ? "PASS" : "FAIL"} ${n}${d ? " — " + d : ""}`);
  console.log(results.at(-1));
};
const refused = async (p: Promise<any>) => {
  try {
    await p;
    return false;
  } catch {
    return true;
  }
};
const client = async (sub: string) =>
  (await createJazzClient({
    app,
    permissions,
    serverUrl: SERVER,
    driver: { type: "memory" },
    credential: () => tok(sub),
  } as any)) as any;
// users A and B need accounts for forUser
const acc = async (u: string) =>
  (await (await fetch(`${BACKEND}/account/${u}`, { method: "POST" })).json()).id;
const [aAcc, bAcc] = [await acc("user-a"), await acc("user-b")];
// trusted
const trusted = await client("gate-trusted-job");
const tShow: any = await trusted.db
  .insert(app.shows, {
    name: "System show",
    venue: "v",
    date: "2026-12-01",
    doors: "19:00",
    chiefAccount: aAcc,
  })
  .wait({ tier: "global" });
check("trusted writes (system identity) a show for A", !!(tShow.value?.id ?? tShow.id));
check("trusted reads all shows", (await trusted.db.all(app.shows, { tier: "remote" })).length >= 1);
const { client: tc } = trusted.getSnapshot();
const asA = await tc.forUser("user-a"),
  asB = await tc.forUser("user-b");
const aShow: any = await asA
  .insert(app.shows, { name: "A private", venue: "v", date: "d", doors: "d", chiefAccount: aAcc })
  .wait({ tier: "global" });
const aShowId = aShow.value?.id ?? aShow.id;
check("forUser(A) writes A's private show", !!aShowId);
check(
  "forUser(A) sees A's shows",
  (await asA.all(app.shows, { tier: "remote" })).some((s: any) => s.id === aShowId),
);
check(
  "forUser(B) can't see A's private show",
  !(await asB.all(app.shows, { tier: "remote" })).some((s: any) => s.id === aShowId),
);
check(
  "forUser(B) can't write a task into A's show",
  await refused(
    asB
      .insert(app.tasks, { showId: aShowId, title: "x", status: "todo", assigneeId: null, rank: 1 })
      .wait({ tier: "global" }),
  ),
);
check(
  "forUser(B) can't create a show with A as chief",
  await refused(
    asB
      .insert(app.shows, { name: "f", venue: "v", date: "d", doors: "d", chiefAccount: aAcc })
      .wait({ tier: "global" }),
  ),
);
await trusted.close();
// scoped (acts as user "scoped-agent")
const scoped = await client("gate-scoped-agent");
const sAcc = scoped.getSnapshot().user?.accountId;
check("scoped signs in as its user", !!sAcc, String(sAcc));
const sShow: any = await scoped.db
  .insert(app.shows, { name: "Agent show", venue: "v", date: "d", doors: "d", chiefAccount: sAcc })
  .wait({ tier: "global" });
check("scoped writes its own show", !!(sShow.value?.id ?? sShow.id));
check(
  "scoped can't see A's private show",
  !(await scoped.db.all(app.shows, { tier: "remote" })).some((s: any) => s.id === aShowId),
);
check(
  "scoped can't write into A's show",
  await refused(
    scoped.db
      .insert(app.tasks, { showId: aShowId, title: "x", status: "todo", assigneeId: null, rank: 1 })
      .wait({ tier: "global" }),
  ),
);
let threw = false;
try {
  await scoped.getSnapshot().client.forUser("user-a");
} catch {
  threw = true;
}
check("scoped has no forUser", threw);
await scoped.close();
// an untrusted workload (unlisted sub) is refused
let refusedUnknown = false;
try {
  const u = await client("someone-else");
  await u.db.all(app.shows, { tier: "remote" });
  await u.close();
} catch {
  refusedUnknown = true;
}
check("unlisted workload token is refused", refusedUnknown);
const fail = results.some((r) => r.startsWith("FAIL"));
console.log(fail ? "BACKEND-CLIENT FAIL" : "BACKEND-CLIENT PASS");
process.exit(fail ? 1 : 0);

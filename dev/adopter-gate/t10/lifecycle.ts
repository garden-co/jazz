// jazz#5592: a graceful shutdown that can't sync (backend down) leaves the Db open and usable;
// once teardown starts, operations fail with an explicit error. Env: A_URL, RESTART_A (command to restart instance A).
import { createJazzClient } from "jazz-tools/client";
import { execSync } from "node:child_process";
import { app } from "#app";
const A = process.env.A_URL!; let fails = 0;
const check = (n: string, ok: boolean, d = "") => { if (!ok) fails++; console.log(`${ok ? "PASS" : "FAIL"} ${n}${d ? " — " + d : ""}`); };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const id = (await (await fetch(`${A}/account/carol`, { method: "POST" })).json()).id;
const s: any = await createJazzClient({ serverUrl: `${A}/jazz`, bearer: () => "carol" } as any); await s.refresh?.();
const db = s.db;
await db.insert(app.notes, { title: "online", owner: id }).wait({ tier: "global" });
execSync(process.env.KILL_A!); await sleep(1500);
check("link drops when the backend goes away", db.getSyncStatus().link.state !== "connected", JSON.stringify(db.getSyncStatus().link));
db.insert(app.notes, { title: "offline write", owner: id });
// Graceful shutdown while offline: it can't upload; cancel it as a sign-out timeout would.
const drain = db.shutdown({ waitForSync: true }); let drainErr: any = null;
const settled = drain.then(() => "resolved", (e: any) => { drainErr = e; return "rejected"; });
await sleep(1000); db.abortGracefulShutdown?.();
const outcome = await Promise.race([settled, sleep(10000).then(() => "hung")]);
check("offline graceful shutdown doesn't hang once cancelled", outcome !== "hung", `${outcome} ${drainErr?.name ?? ""}: ${String(drainErr?.message ?? "").slice(0, 120)}`);
let usable = true; try { db.insert(app.notes, { title: "after failed drain", owner: id }); } catch (e: any) { usable = false; console.log("  err:", e?.message); }
check("after the failed drain the Db is still open (rejects, accepts a write)", outcome === "rejected" && usable);
execSync(process.env.RESTART_A!);
const t = Date.now(); let synced = false;
while (Date.now() - t < 20000) { try { const r: any = await Promise.race([db.all(app.notes, { tier: "remote" }), sleep(3000).then(() => [])]); if (r.some((n: any) => n.title === "after failed drain") && r.some((n: any) => n.title === "offline write")) { synced = true; break; } } catch {} await sleep(500); }
check("it reconnects and uploads the offline writes", synced, JSON.stringify(db.getSyncStatus().link));
await Promise.race([db.shutdown(), sleep(10000)]);
let msg = ""; try { db.insert(app.notes, { title: "after close", owner: id }); } catch (e: any) { msg = String(e?.message ?? e); }
check("operations after teardown fail explicitly", /shutting down or closed/.test(msg), msg.slice(0, 120));
console.log(fails ? `LIFECYCLE FAIL (${fails})` : "LIFECYCLE PASS"); process.exit(fails ? 1 : 0);

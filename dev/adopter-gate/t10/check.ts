// Alice and Bob on instance A (sockets); the sign-out request for Alice goes to instance B.
import { createJazzClient } from "jazz-tools/client";
import { writeFileSync } from "node:fs";
import { app } from "#app";
const A = process.env.A_URL!, B = process.env.B_URL!; let fails = 0;
const check = (n: string, ok: boolean, d = "") => { if (!ok) fails++; console.log(`${ok ? "PASS" : "FAIL"} ${n}${d ? " — " + d : ""}`); };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const ids: Record<string, string> = {};
async function user(u: string, base: string) { ids[u] ??= (await (await fetch(`${base}/account/${u}`, { method: "POST" })).json()).id; const s: any = await createJazzClient({ serverUrl: `${base}/jazz`, bearer: () => u } as any); await s.refresh?.(); return s; }
async function poll(f: () => boolean, ms: number) { const t = Date.now(); while (Date.now() - t < ms) { if (f()) return Date.now() - t; await sleep(25); } return -1; }
const alice = await user("alice", A), bob = await user("bob", A);
await alice.db.insert(app.notes, { title: "a1", owner: ids.alice }).wait({ tier: "global" });
await bob.db.insert(app.notes, { title: "b1", owner: ids.bob }).wait({ tier: "global" });
check("alice and bob sync via instance A", alice.db.getSyncStatus().link.state === "connected", JSON.stringify(alice.db.getSyncStatus().link));
const r = await (await fetch(`${B}/signout/alice`, { method: "POST" })).json();
check("revokeUser on instance B (none of alice's sessions there)", r.revokedHere === 0, JSON.stringify(r));
const took = await poll(() => { const l = alice.db.getSyncStatus().link; return l.state === "offline" && l.reason === "auth"; }, 5000);
check("alice's socket on instance A ends as auth (offline)", took >= 0, took >= 0 ? `${took} ms` : JSON.stringify(alice.db.getSyncStatus().link));
let rejected = false; try { await alice.db.insert(app.notes, { title: "after revoke", owner: ids.alice }).wait({ tier: "global", timeoutMs: 2000 }); } catch { rejected = true; }
check("alice's write after revoke doesn't sync", rejected);
await bob.db.insert(app.notes, { title: "b2", owner: ids.bob }).wait({ tier: "global" });
check("bob on instance A unaffected", bob.db.getSyncStatus().link.state === "connected");
writeFileSync(process.env.SIGNED_OUT!, "");
const again = await user("alice", A);
await again.db.insert(app.notes, { title: "a2", owner: ids.alice }).wait({ tier: "global", timeoutMs: 5000 });
const mine = await again.db.all(app.notes, { tier: "remote" });
check("alice signs in again and syncs", mine.some((n: any) => n.title === "a2"), mine.map((n: any) => n.title).join(","));
console.log(fails ? `REVOKE FAIL (${fails})` : "REVOKE PASS"); process.exit(fails ? 1 : 0);

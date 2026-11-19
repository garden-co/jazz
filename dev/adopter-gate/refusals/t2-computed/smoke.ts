import { app } from "./schema.js";
import { account, clientAs, check, done } from "./client.js";
const aId = await account("alice"); await account("bob");
const alice = await clientAs("alice"), bob = await clientAs("bob");
await alice.db.insert(app.tasks, { title: "Fix", score: 9, owner_id: aId, dueAt: "2026-12-01", tags: ["a", "b"], meta: { x: 1 } } as any).wait({ tier: "global" });
await alice.db.insert(app.tasks, { title: "Idea", score: 3, owner_id: aId, dueAt: null, tags: [] } as any).wait({ tier: "global" });
const rows: any[] = await bob.db.all(app.tasks.orderBy("score", "desc"), { tier: "remote" });
check("bob reads 2 rows", rows.length === 2);
check("computed priority", rows[0]?.priority === "high" && rows[1]?.priority === "low", `${rows[0]?.priority},${rows[1]?.priority}`);
check("computed label", rows[0]?.label === "Fix (9)", rows[0]?.label);
check("transform dueAt round-trips ISO date", rows[0]?.dueAt === "2026-12-01" && rows[1]?.dueAt === null, `${rows[0]?.dueAt}`);
check("array + json", JSON.stringify(rows[0]?.tags) === '["a","b"]' && rows[0]?.meta?.x === 1, "");
let refused = false; try { await bob.db.update(app.tasks, rows[0].id, { score: 1 }).wait({ tier: "global" }); } catch { refused = true; }
check("bob can't update alice's task", refused);
done();

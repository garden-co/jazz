import { app } from "./jazz.generated.js";
import { account, clientAs, check, done } from "./client.js";
const [aId, bId] = [await account("alice"), await account("bob")]; await account("carol");
const alice = await clientAs("alice"), bob = await clientAs("bob"), carol = await clientAs("carol");
const p = await alice.db.insert(app.projects, { name: "P1", owner_id: aId }).wait({ tier: "global" });
const pid = p.value?.id ?? p.id;
await alice.db.insert(app.projectMembers, { projectId: pid, user_id: bId }).wait({ tier: "global" });
await alice.db.insert(app.tasks, { projectId: pid, title: "T1", details: "d", status: "done" }).wait({ tier: "global" });
const at = await alice.db.all(app.tasks, { tier: "remote" });
check("alice writes + reads task (enum 'done', renamed column)", at.length === 1 && at[0].status === "done" && at[0].details === "d", JSON.stringify(at[0]));
const bt = await bob.db.all(app.tasks, { tier: "remote" });
check("bob (member) sees task", bt.length === 1);
const ct = await carol.db.all(app.tasks, { tier: "remote" });
check("carol (non-member) sees nothing", ct.length === 0);
let refused = false; try { await carol.db.insert(app.projectMembers, { projectId: pid, user_id: bId }).wait({ tier: "global" }); } catch { refused = true; }
check("carol can't add members", refused);
done();

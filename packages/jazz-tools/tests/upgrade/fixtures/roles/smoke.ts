import { app } from "./schema.js";
import { clientAs, check, done, PORT } from "./client.js";
const { orgId } = await (await fetch(`http://localhost:${PORT}/seed`, { method: "POST" })).json();
const ed = await clientAs("ed"),
  vic = await clientAs("vic"),
  out = await clientAs("out"),
  staffer = await clientAs("staffer", { staff: true }),
  ann = await clientAs("ann"),
  friend = await clientAs("friend");
const d1: any = await ed.db
  .insert(app.docs, { orgId, title: "Draft", status: "draft", authorId: "ed" })
  .wait({ tier: "global" });
const d2: any = await ed.db
  .insert(app.docs, { orgId, title: "Pub", status: "published", authorId: "ed" })
  .wait({ tier: "global" });
const id1 = d1.value?.id ?? d1.id;
const ids = async (c: any) =>
  (await c.db.all(app.docs, { tier: "remote" }))
    .map((r: any) => r.title)
    .sort()
    .join(",");
check("editor sees both", (await ids(ed)) === "Draft,Pub");
check("viewer sees only published", (await ids(vic)) === "Pub");
check("outsider sees none", (await ids(out)) === "");
check("staff claim sees both", (await ids(staffer)) === "Draft,Pub");
await ed.db.insert(app.docShares, { docId: id1, userId: "friend" }).wait({ tier: "global" });
check("share grants friend the draft", (await ids(friend)) === "Draft");
await friend.db.insert(app.comments, { docId: id1, body: "hi" }).wait({ tier: "global" });
check(
  "friend comment visible to ann (allowedTo.read)",
  (await ann.db.all(app.comments, { tier: "remote" })).length === 1,
);
let r1 = false;
try {
  await vic.db
    .insert(app.docs, { orgId, title: "X", status: "draft", authorId: "vic" })
    .wait({ tier: "global" });
} catch {
  r1 = true;
}
check("viewer can't insert", r1);
await ed.db.update(app.docs, id1, { status: "locked" }).wait({ tier: "global" });
let r2 = false;
try {
  await ed.db.update(app.docs, id1, { title: "Y" }).wait({ tier: "global" });
} catch {
  r2 = true;
}
check("locked doc can't be edited (whereOld ne)", r2);
let r3 = false;
try {
  const m: any = (await ann.db.all(app.members, { tier: "remote" })).find(
    (x: any) => x.userId === "ann",
  );
  await ann.db.update(app.members, m.id, { role: "viewer" }).wait({ tier: "global" });
} catch {
  r3 = true;
}
check("admin can't change own role", r3);
done();

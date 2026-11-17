import { app, notesOnly } from "./src/db/index.js";
import { account, clientAs, check, done } from "./client.js";
const aId = await account("alice");
await account("bob");
const alice = await clientAs("alice"),
  bob = await clientAs("bob");
const b: any = await alice.db
  .insert(app.boards, { title: "B", owner_id: aId })
  .wait({ tier: "global" });
await alice.db
  .insert(notesOnly.notes, { text: "via slice", owner_id: aId, boardId: b.value?.id ?? b.id })
  .wait({ tier: "global" });
const rows: any[] = await alice.db.all(app.boards.include({ notes: true }), { tier: "remote" });
check(
  "alice reads board with included notes (full slice)",
  rows.length === 1 && rows[0].notes?.length === 1,
  JSON.stringify(rows[0]?.notes?.map((n: any) => n.text)),
);
check(
  "alice reads via notesOnly slice",
  (await alice.db.all(notesOnly.notes, { tier: "remote" })).length === 1,
);
check("bob sees nothing", (await bob.db.all(app.notes, { tier: "remote" })).length === 0);
done();

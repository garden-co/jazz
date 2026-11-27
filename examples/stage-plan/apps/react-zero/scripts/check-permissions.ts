// Runs the server-side mutators and queries against the seeded database as
// different users, checking "Who can do what". Run after `pnpm seed`.
import pg from "pg";
import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { mutators } from "../mutators.js";
import { queries } from "../queries.js";
import { schema } from "../schema.js";
import { DATABASE_URL } from "./db.js";

const pool = new pg.Pool({ connectionString: DATABASE_URL });
const db = zeroNodePg(schema, pool);
let failures = 0;
type Mut = { fn: (o: { tx: never; args: never; ctx: never }) => Promise<unknown> };
async function expect(label: string, ok: boolean, user: string, mutator: Mut, args: object) {
  let error: unknown;
  try {
    await db.transaction((tx) => mutator.fn({ tx: tx as never, args: args as never, ctx: { userID: user } as never }));
  } catch (e) {
    error = e;
  }
  const passed = ok ? !error : !!error;
  if (!passed) failures++;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}${error && ok ? `: ${(error as Error).message}` : ""}`);
}
async function rows(user: string, query: { fn: (o: { args: never; ctx: never }) => unknown }, args: object) {
  return db.transaction((tx) => tx.run(query.fn({ args: args as never, ctx: { userID: user } as never }) as never)) as Promise<unknown[] | unknown>;
}

const { rows: [show] } = await pool.query(`SELECT s.id, i.code FROM shows s JOIN "showInvites" i ON i."showId" = s.id WHERE s.name LIKE 'The Late Lanterns%' AND s."chiefAccount" = '00000000-0000-4000-8000-000000000001'`);
const { rows: [task] } = await pool.query(`SELECT id FROM tasks WHERE "showId" = $1 LIMIT 1`, [show.id]);
const chief = "00000000-0000-4000-8000-000000000001";
const guest = crypto.randomUUID();
const other = crypto.randomUUID();
for (const user of [guest, other]) await expect("profile", true, user, mutators.crew.ensure, { name: "Tester" });

const tasksSeen = (await rows(guest, queries.tasks.byShow, { showId: show.id })) as unknown[];
console.log(`${tasksSeen.length === 0 ? "ok  " : "FAIL"} outsider reads no tasks`), tasksSeen.length && failures++;
await expect("outsider can't add a task", false, guest, mutators.tasks.add, { id: crypto.randomUUID(), activityId: crypto.randomUUID(), showId: show.id, title: "x", status: "todo", rank: 1 });
await expect("wrong invite code is refused", false, guest, mutators.showCrew.join, { showId: show.id, code: "nope" });
await expect("invite code joins the crew", true, guest, mutators.showCrew.join, { showId: show.id, code: show.code });
const tasksAfter = (await rows(guest, queries.tasks.byShow, { showId: show.id })) as unknown[];
console.log(`${tasksAfter.length >= 100 ? "ok  " : "FAIL"} crew reads the show's tasks`), tasksAfter.length < 100 && failures++;
const invites = (await rows(guest, queries.showInvites.byShow, { showId: show.id })) as unknown[];
console.log(`${invites.length === 0 ? "ok  " : "FAIL"} crew can't read invite codes`), invites.length && failures++;
await expect("crew adds a task", true, guest, mutators.tasks.add, { id: crypto.randomUUID(), activityId: crypto.randomUUID(), showId: show.id, title: "x", status: "todo", rank: 1 });
await expect("crew moves a task", true, guest, mutators.tasks.move, { id: task.id, activityId: crypto.randomUUID(), status: "done", rank: 1 });
await expect("crew can't assign to an outsider", false, guest, mutators.tasks.assign, { id: task.id, activityId: crypto.randomUUID(), assigneeId: other });
await expect("crew can't delete the chief's task", false, guest, mutators.tasks.delete, { id: task.id, activityId: crypto.randomUUID() });
await expect("crew can't rotate the invite", false, guest, mutators.showInvites.rotate, { showId: show.id, id: crypto.randomUUID(), code: "x" });
await expect("crew can't edit the show", false, guest, mutators.shows.update, { id: show.id, name: "x", venue: "x", date: "2026-01-01", doors: "19:00" });
await expect("crew can't remove the chief", false, guest, mutators.showCrew.remove, { id: `${show.id}:${chief}` });
await expect("can't take over a show by id", false, other, mutators.shows.create, { id: show.id, inviteId: crypto.randomUUID(), code: "x", name: "x", venue: "x", date: "2026-01-01", doors: "19:00" });
await expect("chief deletes a task", true, chief, mutators.tasks.delete, { id: task.id, activityId: crypto.randomUUID() });
await expect("checklist item", true, guest, mutators.checklist.add, { id: "item-1", title: "tape" });
await expect("can't tick someone else's checklist", false, other, mutators.checklist.setDone, { id: "item-1", done: true });
const items = (await rows(other, queries.checklist.mine, { search: "", filter: "all" })) as unknown[];
console.log(`${items.length === 0 ? "ok  " : "FAIL"} checklists are private`), items.length && failures++;
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);

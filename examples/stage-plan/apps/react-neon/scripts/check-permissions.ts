// Runs the server's write handlers and live-query SQL against the seeded
// database as different users, checking "Who can do what". Run after `pnpm seed`.
import pg from "pg";
import { runMutation } from "../server/mutators.js";
import { liveQueries } from "../server/queries.js";
import { DATABASE_URL } from "./db.js";

const pool = new pg.Pool({ connectionString: DATABASE_URL });
let failures = 0;
const report = (label: string, passed: boolean, detail = "") => {
  if (!passed) failures++;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}${detail}`);
};
async function expect(label: string, ok: boolean, user: string, name: string, args: object) {
  let error: unknown;
  try {
    await runMutation(pool, user, name, args);
  } catch (e) {
    error = e;
  }
  report(label, ok ? !error : !!error, error && ok ? `: ${(error as Error).message}` : "");
}
async function rows(user: string, name: string, args: object = {}) {
  const query = liveQueries[name](user, args) as unknown as { sql: string; parameters: { value: unknown }[] };
  return (await pool.query(query.sql, query.parameters.map((p) => p.value))).rows;
}

const chief = "00000000-0000-4000-8000-000000000001";
const { rows: [show] } = await pool.query(
  `SELECT s.id, i.code FROM shows s JOIN "showInvites" i ON i."showId" = s.id WHERE s.name LIKE 'The Late Lanterns%' AND s."chiefAccount" = $1`,
  [chief],
);
const { rows: [task] } = await pool.query(`SELECT id FROM tasks WHERE "showId" = $1 LIMIT 1`, [show.id]);
const guest = crypto.randomUUID();
const other = crypto.randomUUID();
const ids = () => ({ id: crypto.randomUUID(), activityId: crypto.randomUUID() });
for (const user of [guest, other]) await expect("profile", true, user, "crew.ensure", { name: "Tester" });

report("outsider reads no tasks", (await rows(guest, "tasks.byShow", { showId: show.id })).length === 0);
report("outsider reads no show", (await rows(guest, "shows.one", { showId: show.id })).length === 0);
await expect("outsider can't add a task", false, guest, "tasks.add", { ...ids(), showId: show.id, title: "x", status: "todo", rank: 1 });
await expect("wrong invite code is refused", false, guest, "showCrew.join", { showId: show.id, code: "nope" });
await expect("invite code joins the crew", true, guest, "showCrew.join", { showId: show.id, code: show.code });
report("crew reads the show's tasks", (await rows(guest, "tasks.byShow", { showId: show.id })).length >= 100);
report("crew reads the crew list", (await rows(guest, "showCrew.byShow", { showId: show.id })).length === 2);
report("crew can't read invite codes", (await rows(guest, "showInvites.byShow", { showId: show.id })).length === 0);
report("chief reads invite codes", (await rows(chief, "showInvites.byShow", { showId: show.id })).length === 1);
await expect("crew adds a task", true, guest, "tasks.add", { ...ids(), showId: show.id, title: "x", status: "todo", rank: 1 });
await expect("crew moves a task", true, guest, "tasks.move", { ...ids(), id: task.id, status: "done", rank: 1 });
await expect("crew comments", true, guest, "comments.add", { ...ids(), taskId: task.id, body: "hi" });
report("crew reads comments", (await rows(guest, "comments.byTask", { taskId: task.id })).length >= 1);
report("outsider reads no comments", (await rows(other, "comments.byTask", { taskId: task.id })).length === 0);
report("crew reads activity", (await rows(guest, "activity.feed", { showId: show.id, limit: 20 })).length >= 1);
report("outsider reads no activity", (await rows(other, "activity.feed", { showId: show.id, taskId: task.id, limit: 20 })).length === 0);
await expect("crew can't assign to an outsider", false, guest, "tasks.assign", { ...ids(), id: task.id, assigneeId: other });
await expect("crew assigns to the chief", true, guest, "tasks.assign", { ...ids(), id: task.id, assigneeId: chief });
await expect("crew can't delete the chief's task", false, guest, "tasks.delete", { ...ids(), id: task.id });
await expect("crew can't rotate the invite", false, guest, "showInvites.rotate", { showId: show.id, id: crypto.randomUUID(), code: "x" });
await expect("crew can't edit the show", false, guest, "shows.update", { id: show.id, name: "x", venue: "x", date: "2026-01-01", doors: "19:00" });
await expect("crew can't remove the chief", false, guest, "showCrew.remove", { id: `${show.id}:${chief}` });
await expect("can't take over a show by id", false, other, "shows.create", { id: show.id, inviteId: crypto.randomUUID(), code: "x", name: "x", venue: "x", date: "2026-01-01", doors: "19:00" });
await expect("chief deletes a task", true, chief, "tasks.delete", { ...ids(), id: task.id });
await expect("new show with the demo board", true, other, "shows.create", { id: crypto.randomUUID(), inviteId: crypto.randomUUID(), code: "c", name: "x", venue: "x", date: "2026-01-01", doors: "19:00", demo: true });
report("my shows lists it with its tasks", ((await rows(other, "shows.mine"))[0]?.tasks?.length ?? 0) === 8);
await expect("checklist item", true, guest, "checklist.add", { id: "item-1", title: "tape" });
await expect("can't tick someone else's checklist", false, other, "checklist.setDone", { id: "item-1", done: true });
report("checklists are private", (await rows(other, "checklist.mine", { search: "", filter: "all" })).length === 0);
report("checklist search", (await rows(guest, "checklist.mine", { search: "ta", filter: "open" })).length === 1);
await expect("member leaves", true, guest, "showCrew.remove", { id: `${show.id}:${guest}` });
report("ex-crew reads no tasks", (await rows(guest, "tasks.byShow", { showId: show.id })).length === 0);
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);

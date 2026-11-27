// Checks the README's "Who can do what" against a running server (after
// `pnpm seed` and `pnpm start`): outsiders, crew and the chief, over HTTP.
// It writes test rows, so seed again before a benchmark run.
import { readFileSync } from "node:fs";

const origin = process.env.ORIGIN ?? "http://127.0.0.1:4320";
const { showId, code } = JSON.parse(readFileSync(".bench-ctx.json", "utf8"));
let failures = 0;
const check = (label: string, ok: boolean) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${label}`);
  if (!ok) failures++;
};

async function guest() {
  const r = await fetch(`${origin}/api/session`);
  const cookie = r.headers.get("set-cookie")!.split(";")[0];
  const { account } = await r.json();
  const get = (path: string) => fetch(`${origin}/api${path}`, { headers: { cookie } }).then((x) => x.json());
  const call = (name: string, args: object) =>
    fetch(`${origin}/api/rpc/${name}`, { method: "POST", headers: { cookie, "content-type": "application/json" }, body: JSON.stringify(args) }).then((x) => x.status);
  await call("crew.ensure", { name: `Guest ${account.slice(0, 4)}` });
  return { account, get, call };
}

const outsider = await guest();
const member = await guest();
check("outsider can't read the show", (await outsider.get(`/shows/${showId}`)) === null);
check("outsider can't read its tasks", (await outsider.get(`/shows/${showId}/tasks`)).length === 0);
check("outsider can't add a task", (await outsider.call("tasks.add", { id: crypto.randomUUID(), activityId: crypto.randomUUID(), showId, title: "x", status: "todo", rank: 1 })) === 403);
check("a wrong invite code is refused", (await outsider.call("showCrew.join", { showId, code: "nope" })) === 403);
check("the invite code admits", (await member.call("showCrew.join", { showId, code })) === 200);
const tasks = await member.get(`/shows/${showId}/tasks`);
check("crew reads the 100 tasks", tasks.length === 100);
check("crew can't read the invite code", (await member.get(`/shows/${showId}/invites`)).length === 0);
check("crew can't rotate the invite", (await member.call("showInvites.rotate", { showId, id: crypto.randomUUID(), code: "x" })) === 403);
check("crew can't edit the show", (await member.call("shows.update", { id: showId, name: "x", venue: "x", date: "x", doors: "x" })) === 403);
check("crew moves a task", (await member.call("tasks.move", { id: tasks[0].id, activityId: crypto.randomUUID(), status: "done", rank: 1 })) === 200);
check("crew can't delete the chief's task", (await member.call("tasks.delete", { id: tasks[0].id, activityId: crypto.randomUUID() })) === 403);
check("can't assign to a non-member", (await member.call("tasks.assign", { id: tasks[0].id, activityId: crypto.randomUUID(), assigneeId: outsider.account })) === 403);
const own = crypto.randomUUID();
await member.call("tasks.add", { id: own, activityId: crypto.randomUUID(), showId, title: "mine", status: "todo", rank: 1 });
check("the creator deletes their task", (await member.call("tasks.delete", { id: own, activityId: crypto.randomUUID() })) === 200);
const crew = await member.get(`/shows/${showId}/crew`);
const chiefRow = crew.find((m: { role: string }) => m.role === "chief");
check("crew can't remove the chief", (await member.call("showCrew.remove", { id: chiefRow.id })) === 403);
const item = crypto.randomUUID();
await member.call("checklist.add", { id: item, title: "private" });
check("checklists are private", (await outsider.get(`/checklist?search=&filter=all`)).length === 0);
check("others can't tick your checklist", (await outsider.call("checklist.setDone", { id: item, done: true })) === 403);
const mine = crew.find((m: { account: string }) => m.account === member.account);
check("members leave on their own", (await member.call("showCrew.remove", { id: mine.id })) === 200);
check("after leaving, the show is gone", (await member.get(`/shows/${showId}`)) === null);
process.exit(failures ? 1 : 0);

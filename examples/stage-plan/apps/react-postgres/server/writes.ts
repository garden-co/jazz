// RPC writes: POST /api/rpc/<name> with a JSON body. Each runs in one
// transaction with its permission checks, then tells the live hub which
// tables changed under which topics.
import { Hono } from "hono";
import { z } from "zod";
import { TASK_STATUSES, type ActivityKind, type Crew, type Task, type TaskStatus } from "../shared/types.js";
import { one, rows, transaction, type Tx } from "./db.js";
import { assertAssignable, assertChief, assertCrew, Forbidden } from "./permissions.js";

export const STATUS_LABELS: Record<TaskStatus, string> = { todo: "To do", doing: "In progress", done: "Done", blocked: "Blocked" };

/** The demo show every new account starts with. */
const DEMO_TASKS: { title: string; status: TaskStatus; mine?: boolean; notes?: string }[] = [
  { title: "Load-in", status: "done", mine: true, notes: "Dock opens at 14:00. Four cases and the drum riser." },
  { title: "Confirm stage plot with the venue", status: "done" },
  { title: "Set the backline", status: "doing", mine: true },
  { title: "Line check", status: "doing", notes: "16 inputs. Vocals on channels 1 to 3." },
  { title: "Soundcheck with the band", status: "todo", mine: true },
  { title: "Print setlists and tape them down", status: "todo" },
  { title: "Walk-in playlist on the house system", status: "todo" },
  { title: "Hazer approval from the venue", status: "blocked", notes: "Waiting on the fire officer." },
];
const DEMO_COMMENT = "Channel 7 crackles. Swapping the DI box before soundcheck.";

const show = (id: string) => `show:${id}`;
const me = (account: string) => `me:${account}`;

function insert(tx: Tx, table: string, row: Record<string, unknown>) {
  const cols = Object.keys(row);
  return tx.query(
    `INSERT INTO "${table}" (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")})`,
    Object.values(row),
  );
}

function logActivity(tx: Tx, actorId: string, entry: { id: string; showId: string; taskId: string; kind: ActivityKind; detail?: string }) {
  return insert(tx, "activity", { ...entry, actorId, detail: entry.detail ?? null, createdAt: Date.now() });
}

/** A task the caller may edit, locked for this transaction. */
async function crewTask(tx: Tx, account: string, id: string) {
  const task = await one<Task>(tx, `SELECT * FROM tasks WHERE id = $1 FOR UPDATE`, [id]);
  if (!task) throw new Forbidden("No such task");
  await assertCrew(tx, account, task.showId);
  return task;
}

type Handler = { input: z.ZodType; run: (tx: Tx, account: string, args: any) => Promise<unknown> };
const rpc = <S extends z.ZodType>(input: S, run: (tx: Tx, account: string, args: z.infer<S>) => Promise<unknown>): Handler => ({ input, run });

const showInput = z.object({ name: z.string().min(1), venue: z.string().min(1), date: z.string(), doors: z.string() });
const status = z.enum(TASK_STATUSES);

const handlers: Record<string, Handler> = {
  "crew.ensure": rpc(z.object({ name: z.string() }), async (tx, account, { name }) => {
    await tx.query(`INSERT INTO crew (id, account, name) VALUES ($1, $1, $2) ON CONFLICT DO NOTHING`, [account, name]);
    tx.changed(me(account), "crew");
    return one<Crew>(tx, `SELECT * FROM crew WHERE id = $1`, [account]);
  }),
  "crew.rename": rpc(z.object({ name: z.string().min(1) }), async (tx, account, { name }) => {
    await tx.query(`UPDATE crew SET name = $2 WHERE id = $1`, [account, name]);
    tx.changed(me(account), "crew");
    // Names show up in every crew list, feed and comment thread you're in.
    for (const { showId } of await rows<{ showId: string }>(tx, `SELECT "showId" FROM "showCrew" WHERE account = $1`, [account])) {
      tx.changed(show(showId), "crew");
    }
  }),

  // Creates a show with the caller as crew chief and a first invite code;
  // `demo` also fills in the demo board.
  "shows.create": rpc(
    showInput.extend({ id: z.string(), inviteId: z.string(), code: z.string(), demo: z.boolean().optional() }),
    async (tx, account, { id, inviteId, code, demo, ...input }) => {
      const now = Date.now();
      await insert(tx, "shows", { id, ...input, chiefAccount: account, createdAt: now });
      await insert(tx, "showCrew", { id: `${id}:${account}`, showId: id, crewId: account, account, role: "chief" });
      await insert(tx, "showInvites", { id: inviteId, showId: id, code });
      tx.changed(me(account), "shows");
      if (!demo) return;
      for (const [index, task] of DEMO_TASKS.entries()) {
        const taskId = crypto.randomUUID();
        await insert(tx, "tasks", {
          id: taskId, showId: id, title: task.title, status: task.status, assigneeId: task.mine ? account : null,
          notes: task.notes ?? null, rank: index + 1, createdBy: account, createdAt: now, updatedAt: now,
        });
        await logActivity(tx, account, { id: crypto.randomUUID(), showId: id, taskId, kind: "created" });
        if (task.title === "Line check") {
          await insert(tx, "comments", { id: crypto.randomUUID(), taskId, authorId: account, body: DEMO_COMMENT, createdAt: now });
          await logActivity(tx, account, { id: crypto.randomUUID(), showId: id, taskId, kind: "commented" });
        }
      }
    },
  ),
  "shows.update": rpc(showInput.extend({ id: z.string() }), async (tx, account, { id, name, venue, date, doors }) => {
    await assertChief(tx, account, id);
    await tx.query(`UPDATE shows SET name = $2, venue = $3, date = $4, doors = $5 WHERE id = $1`, [id, name, venue, date, doors]);
    tx.changed(show(id), "shows");
  }),

  // Joins a show's crew with its invite code.
  "showCrew.join": rpc(z.object({ showId: z.string(), code: z.string() }), async (tx, account, { showId, code }) => {
    const invite = await one(tx, `SELECT 1 FROM "showInvites" WHERE "showId" = $1 AND code = $2`, [showId, code]);
    if (!invite) throw new Forbidden("This invite link doesn't work any more");
    const joined = await tx.query(
      `INSERT INTO "showCrew" (id, "showId", "crewId", account, role) VALUES ($1, $2, $3, $3, 'crew') ON CONFLICT DO NOTHING`,
      [`${showId}:${account}`, showId, account],
    );
    if (joined.rowCount) tx.changed(show(showId), "showCrew");
    tx.changed(me(account), "shows");
  }),
  // Takes someone off a show's crew; their tasks go back to nobody.
  "showCrew.remove": rpc(z.object({ id: z.string() }), async (tx, account, { id }) => {
    const member = await one<{ showId: string; crewId: string; account: string }>(tx, `SELECT * FROM "showCrew" WHERE id = $1`, [id]);
    if (!member) return;
    if (member.account !== account) await assertChief(tx, account, member.showId);
    await tx.query(`UPDATE tasks SET "assigneeId" = NULL WHERE "showId" = $1 AND "assigneeId" = $2`, [member.showId, member.crewId]);
    await tx.query(`DELETE FROM "showCrew" WHERE id = $1`, [id]);
    tx.changed(show(member.showId), "showCrew", "tasks");
    tx.changed(me(member.account), "shows");
    tx.revoke(member.account, show(member.showId));
  }),

  // Replaces the show's invite code; links built from the old code stop working.
  "showInvites.rotate": rpc(z.object({ showId: z.string(), id: z.string(), code: z.string() }), async (tx, account, args) => {
    await assertChief(tx, account, args.showId);
    await tx.query(`DELETE FROM "showInvites" WHERE "showId" = $1`, [args.showId]);
    await insert(tx, "showInvites", args);
    tx.changed(show(args.showId), "showInvites");
  }),

  "tasks.add": rpc(
    z.object({ id: z.string(), activityId: z.string(), showId: z.string(), title: z.string().min(1), status, rank: z.number() }),
    async (tx, account, { activityId, ...task }) => {
      await assertCrew(tx, account, task.showId);
      const now = Date.now();
      await insert(tx, "tasks", { ...task, assigneeId: null, notes: null, createdBy: account, createdAt: now, updatedAt: now });
      await logActivity(tx, account, { id: activityId, showId: task.showId, taskId: task.id, kind: "created", detail: STATUS_LABELS[task.status] });
      tx.changed(show(task.showId), "tasks", "activity");
    },
  ),
  // Moves a task to a column (and position); logs a move when the column changes.
  "tasks.move": rpc(z.object({ id: z.string(), activityId: z.string(), status, rank: z.number() }), async (tx, account, args) => {
    const task = await crewTask(tx, account, args.id);
    await tx.query(`UPDATE tasks SET status = $2, rank = $3, "updatedAt" = $4 WHERE id = $1`, [task.id, args.status, args.rank, Date.now()]);
    if (args.status !== task.status) {
      await logActivity(tx, account, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "moved", detail: STATUS_LABELS[args.status] });
    }
    tx.changed(show(task.showId), "tasks", "activity");
  }),
  "tasks.assign": rpc(z.object({ id: z.string(), activityId: z.string(), assigneeId: z.string().nullable() }), async (tx, account, args) => {
    const task = await crewTask(tx, account, args.id);
    await assertAssignable(tx, task.showId, args.assigneeId);
    await tx.query(`UPDATE tasks SET "assigneeId" = $2, "updatedAt" = $3 WHERE id = $1`, [task.id, args.assigneeId, Date.now()]);
    const assignee = args.assigneeId ? await one<Crew>(tx, `SELECT * FROM crew WHERE id = $1`, [args.assigneeId]) : undefined;
    await logActivity(tx, account, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "assigned", detail: assignee?.name ?? "nobody" });
    tx.changed(show(task.showId), "tasks", "activity");
  }),
  "tasks.rename": rpc(z.object({ id: z.string(), activityId: z.string(), title: z.string().min(1) }), async (tx, account, args) => {
    const task = await crewTask(tx, account, args.id);
    await tx.query(`UPDATE tasks SET title = $2, "updatedAt" = $3 WHERE id = $1`, [task.id, args.title, Date.now()]);
    await logActivity(tx, account, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "renamed", detail: args.title });
    tx.changed(show(task.showId), "tasks", "activity");
  }),
  "tasks.setNotes": rpc(z.object({ id: z.string(), notes: z.string().nullable() }), async (tx, account, args) => {
    const task = await crewTask(tx, account, args.id);
    await tx.query(`UPDATE tasks SET notes = $2, "updatedAt" = $3 WHERE id = $1`, [task.id, args.notes, Date.now()]);
    tx.changed(show(task.showId), "tasks");
  }),
  "tasks.delete": rpc(z.object({ id: z.string(), activityId: z.string() }), async (tx, account, args) => {
    const task = await crewTask(tx, account, args.id);
    if (task.createdBy !== account) await assertChief(tx, account, task.showId);
    await logActivity(tx, account, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "deleted", detail: task.title });
    await tx.query(`DELETE FROM tasks WHERE id = $1`, [task.id]);
    tx.changed(show(task.showId), "tasks", "activity", "comments");
  }),

  "comments.add": rpc(
    z.object({ id: z.string(), activityId: z.string(), taskId: z.string(), body: z.string().min(1) }),
    async (tx, account, args) => {
      const task = await crewTask(tx, account, args.taskId);
      await insert(tx, "comments", { id: args.id, taskId: task.id, authorId: account, body: args.body, createdAt: Date.now() });
      await logActivity(tx, account, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "commented" });
      tx.changed(show(task.showId), "comments", "activity");
    },
  ),

  "checklist.add": rpc(z.object({ id: z.string(), title: z.string().min(1) }), async (tx, account, args) => {
    await insert(tx, "checklistItems", { ...args, done: false, ownerAccount: account });
    tx.changed(me(account), "checklist");
  }),
  "checklist.setDone": rpc(z.object({ id: z.string(), done: z.boolean() }), async (tx, account, args) => {
    const r = await tx.query(`UPDATE "checklistItems" SET done = $3 WHERE id = $1 AND "ownerAccount" = $2`, [args.id, account, args.done]);
    if (!r.rowCount) throw new Forbidden("Not your checklist");
    tx.changed(me(account), "checklist");
  }),
  "checklist.delete": rpc(z.object({ id: z.string() }), async (tx, account, args) => {
    const r = await tx.query(`DELETE FROM "checklistItems" WHERE id = $1 AND "ownerAccount" = $2`, [args.id, account]);
    if (!r.rowCount) throw new Forbidden("Not your checklist");
    tx.changed(me(account), "checklist");
  }),
};

type Env = { Variables: { me: string } };
export const writes = new Hono<Env>();

writes.post("/rpc/:name", async (c) => {
  const handler = handlers[c.req.param("name")];
  if (!handler) return c.json({ error: "No such call" }, 404);
  const parsed = handler.input.safeParse(await c.req.json());
  if (!parsed.success) return c.json({ error: parsed.error.message }, 400);
  try {
    const result = await transaction((tx) => handler.run(tx, c.var.me, parsed.data));
    return c.json({ result: result ?? null });
  } catch (error) {
    if (error instanceof Forbidden) return c.json({ error: error.message }, 403);
    throw error;
  }
});

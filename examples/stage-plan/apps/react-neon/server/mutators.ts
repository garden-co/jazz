import type pg from "pg";
import { z } from "zod";
import { DEMO_COMMENT, DEMO_TASKS, STATUS_LABELS } from "../demo.js";
import { TASK_STATUSES, type ActivityKind, type Task } from "../schema.js";

/**
 * Who can do what in StagePlan (the write half; reads are in queries.ts):
 *
 * - The crew chief (the account that created a show) manages the show, its
 *   invite link and its crew.
 * - Crew members (anyone with a showCrew row) can add, edit and move the
 *   show's tasks, comment and log activity. A task is assigned to nobody or
 *   to someone on the crew. The chief or a task's creator deletes it.
 * - Anyone holding the show's current invite code can join its crew.
 * - Members leave on their own; the chief can remove anyone.
 * - Checklist items are private to their owner.
 *
 * Every write goes through these handlers on the app's server, as Neon's
 * mutations guide does it: validate, authorize, write in one transaction and
 * return the transaction's id, which the client waits for on its live queries.
 */
export class Forbidden extends Error {}

export type Tx = {
  me: string;
  rows<T>(sql: string, params?: unknown[]): Promise<T[]>;
  /** A write; its RETURNING carries the transaction id, which the response returns. */
  write(sql: string, params?: unknown[]): Promise<number>;
};

const one = async <T>(tx: Tx, sql: string, params: unknown[]) => (await tx.rows<T>(sql, params))[0];

async function isChief(tx: Tx, showId: string) {
  return Boolean(await one(tx, `SELECT 1 FROM "shows" WHERE "id" = $1 AND "chiefAccount" = $2`, [showId, tx.me]));
}
async function assertChief(tx: Tx, showId: string) {
  if (!(await isChief(tx, showId))) throw new Forbidden("Only the crew chief can do that");
}
async function assertCrew(tx: Tx, showId: string) {
  const member = await one(tx, `SELECT 1 FROM "showCrew" WHERE "showId" = $1 AND "account" = $2`, [showId, tx.me]);
  if (!member && !(await isChief(tx, showId))) throw new Forbidden("Not on this show's crew");
}
async function assertAssignable(tx: Tx, showId: string, assigneeId: string | null) {
  if (assigneeId === null) return;
  const member = await one(tx, `SELECT 1 FROM "showCrew" WHERE "showId" = $1 AND "crewId" = $2`, [showId, assigneeId]);
  if (!member) throw new Forbidden("Tasks can only be assigned to the crew");
}
/** A task the caller may edit: it exists, and they're on its show's crew. */
async function crewTask(tx: Tx, id: string) {
  const task = await one<Task>(tx, `SELECT * FROM "tasks" WHERE "id" = $1 FOR UPDATE`, [id]);
  if (!task) throw new Forbidden("No such task");
  await assertCrew(tx, task.showId);
  return task;
}

function logActivity(
  tx: Tx,
  entry: { id: string; showId: string; taskId: string; kind: ActivityKind; detail?: string },
  createdAt = Date.now(),
) {
  return tx.write(
    `INSERT INTO "activity" ("id", "showId", "taskId", "actorId", "kind", "detail", "createdAt") VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [entry.id, entry.showId, entry.taskId, tx.me, entry.kind, entry.detail ?? null, createdAt],
  );
}

const showInput = z.object({ name: z.string().min(1), venue: z.string().min(1), date: z.string(), doors: z.string() });
const status = z.enum(TASK_STATUSES);

type Mutator = { args: z.ZodType; run: (tx: Tx, args: never) => Promise<unknown> };
const mutator = <S extends z.ZodType>(args: S, run: (tx: Tx, args: z.infer<S>) => Promise<unknown>): Mutator => ({
  args,
  run: run as Mutator["run"],
});

export const mutators: Record<string, Mutator> = {
  /** Creates the caller's crew profile (id = account) unless it exists; says whether it was new. */
  "crew.ensure": mutator(z.object({ name: z.string() }), async (tx, args) => {
    const created = await tx.write(
      `INSERT INTO "crew" ("id", "account", "name") VALUES ($1, $1, $2) ON CONFLICT ("id") DO NOTHING`,
      [tx.me, args.name],
    );
    const profile = await one(tx, `SELECT "id", "account", "name" FROM "crew" WHERE "id" = $1`, [tx.me]);
    return { profile, isNew: created > 0 };
  }),
  "crew.rename": mutator(z.object({ name: z.string().min(1) }), (tx, args) =>
    tx.write(`UPDATE "crew" SET "name" = $2 WHERE "id" = $1`, [tx.me, args.name]),
  ),

  /** A show with the caller as crew chief and a first invite code; `demo` also fills in the demo board. */
  "shows.create": mutator(
    showInput.extend({ id: z.string(), inviteId: z.string(), code: z.string(), demo: z.boolean().optional() }),
    async (tx, { id, inviteId, code, demo, ...input }) => {
      const now = Date.now();
      const created = await tx.write(
        `INSERT INTO "shows" ("id", "name", "venue", "date", "doors", "chiefAccount", "createdAt")
         VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT ("id") DO NOTHING`,
        [id, input.name, input.venue, input.date, input.doors, tx.me, now],
      );
      if (!created) throw new Forbidden("Show exists");
      await tx.write(
        `INSERT INTO "showCrew" ("id", "showId", "crewId", "account", "role") VALUES ($1, $2, $3, $3, 'chief')`,
        [`${id}:${tx.me}`, id, tx.me],
      );
      await tx.write(`INSERT INTO "showInvites" ("id", "showId", "code") VALUES ($1, $2, $3)`, [inviteId, id, code]);
      if (!demo) return;
      for (const [index, task] of DEMO_TASKS.entries()) {
        const taskId = `${id}:task:${index}`;
        await tx.write(
          `INSERT INTO "tasks" ("id", "showId", "title", "status", "assigneeId", "notes", "rank", "createdBy", "createdAt", "updatedAt")
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
          [taskId, id, task.title, task.status, task.mine ? tx.me : null, task.notes ?? null, index + 1, tx.me, now],
        );
        await logActivity(tx, { id: `${taskId}:created`, showId: id, taskId, kind: "created" }, now);
        if (task.title === "Line check") {
          await tx.write(
            `INSERT INTO "comments" ("id", "taskId", "authorId", "body", "createdAt") VALUES ($1, $2, $3, $4, $5)`,
            [`${taskId}:comment`, taskId, tx.me, DEMO_COMMENT, now],
          );
          await logActivity(tx, { id: `${taskId}:commented`, showId: id, taskId, kind: "commented" }, now);
        }
      }
    },
  ),
  "shows.update": mutator(showInput.extend({ id: z.string() }), async (tx, args) => {
    await assertChief(tx, args.id);
    await tx.write(`UPDATE "shows" SET "name" = $2, "venue" = $3, "date" = $4, "doors" = $5 WHERE "id" = $1`, [
      args.id,
      args.name,
      args.venue,
      args.date,
      args.doors,
    ]);
  }),

  /** Joins a show's crew with its current invite code. */
  "showCrew.join": mutator(z.object({ showId: z.string(), code: z.string() }), async (tx, args) => {
    const invite = await one(tx, `SELECT 1 FROM "showInvites" WHERE "showId" = $1 AND "code" = $2`, [args.showId, args.code]);
    if (!invite) throw new Forbidden("This invite link doesn't work any more");
    await tx.write(
      `INSERT INTO "showCrew" ("id", "showId", "crewId", "account", "role") VALUES ($1, $2, $3, $3, 'crew')
       ON CONFLICT ("showId", "account") DO NOTHING`,
      [`${args.showId}:${tx.me}`, args.showId, tx.me],
    );
  }),
  /** Takes someone off a show's crew; their tasks go back to nobody. */
  "showCrew.remove": mutator(z.object({ id: z.string() }), async (tx, args) => {
    const member = await one<{ showId: string; crewId: string; account: string }>(
      tx,
      `SELECT "showId", "crewId", "account" FROM "showCrew" WHERE "id" = $1`,
      [args.id],
    );
    if (!member) return;
    if (member.account !== tx.me) await assertChief(tx, member.showId);
    await tx.write(`UPDATE "tasks" SET "assigneeId" = NULL WHERE "showId" = $1 AND "assigneeId" = $2`, [
      member.showId,
      member.crewId,
    ]);
    await tx.write(`DELETE FROM "showCrew" WHERE "id" = $1`, [args.id]);
  }),

  /** Replaces the show's invite code; links built from the old code stop working. */
  "showInvites.rotate": mutator(z.object({ showId: z.string(), id: z.string(), code: z.string() }), async (tx, args) => {
    await assertChief(tx, args.showId);
    await tx.write(`DELETE FROM "showInvites" WHERE "showId" = $1`, [args.showId]);
    await tx.write(`INSERT INTO "showInvites" ("id", "showId", "code") VALUES ($1, $2, $3)`, [args.id, args.showId, args.code]);
  }),

  "tasks.add": mutator(
    z.object({ id: z.string(), activityId: z.string(), showId: z.string(), title: z.string().min(1), status, rank: z.number() }),
    async (tx, args) => {
      await assertCrew(tx, args.showId);
      const now = Date.now();
      await tx.write(
        `INSERT INTO "tasks" ("id", "showId", "title", "status", "rank", "createdBy", "createdAt", "updatedAt")
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
        [args.id, args.showId, args.title, args.status, args.rank, tx.me, now],
      );
      await logActivity(tx, { id: args.activityId, showId: args.showId, taskId: args.id, kind: "created", detail: STATUS_LABELS[args.status] }, now);
    },
  ),
  /** Moves a task to a column (and position); logs a move when the column changes. */
  "tasks.move": mutator(z.object({ id: z.string(), activityId: z.string(), status, rank: z.number() }), async (tx, args) => {
    const task = await crewTask(tx, args.id);
    await tx.write(`UPDATE "tasks" SET "status" = $2, "rank" = $3, "updatedAt" = $4 WHERE "id" = $1`, [
      task.id,
      args.status,
      args.rank,
      Date.now(),
    ]);
    if (args.status !== task.status) {
      await logActivity(tx, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "moved", detail: STATUS_LABELS[args.status] });
    }
  }),
  "tasks.assign": mutator(z.object({ id: z.string(), activityId: z.string(), assigneeId: z.string().nullable() }), async (tx, args) => {
    const task = await crewTask(tx, args.id);
    await assertAssignable(tx, task.showId, args.assigneeId);
    await tx.write(`UPDATE "tasks" SET "assigneeId" = $2, "updatedAt" = $3 WHERE "id" = $1`, [task.id, args.assigneeId, Date.now()]);
    const assignee = args.assigneeId
      ? await one<{ name: string }>(tx, `SELECT "name" FROM "crew" WHERE "id" = $1`, [args.assigneeId])
      : undefined;
    await logActivity(tx, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "assigned", detail: assignee?.name ?? "nobody" });
  }),
  "tasks.rename": mutator(z.object({ id: z.string(), activityId: z.string(), title: z.string().min(1) }), async (tx, args) => {
    const task = await crewTask(tx, args.id);
    await tx.write(`UPDATE "tasks" SET "title" = $2, "updatedAt" = $3 WHERE "id" = $1`, [task.id, args.title, Date.now()]);
    await logActivity(tx, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "renamed", detail: args.title });
  }),
  "tasks.setNotes": mutator(z.object({ id: z.string(), notes: z.string().nullable() }), async (tx, args) => {
    const task = await crewTask(tx, args.id);
    await tx.write(`UPDATE "tasks" SET "notes" = $2, "updatedAt" = $3 WHERE "id" = $1`, [task.id, args.notes, Date.now()]);
  }),
  "tasks.delete": mutator(z.object({ id: z.string(), activityId: z.string() }), async (tx, args) => {
    const task = await crewTask(tx, args.id);
    if (task.createdBy !== tx.me) await assertChief(tx, task.showId);
    await logActivity(tx, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "deleted", detail: task.title });
    await tx.write(`DELETE FROM "tasks" WHERE "id" = $1`, [task.id]);
  }),

  "comments.add": mutator(z.object({ id: z.string(), activityId: z.string(), taskId: z.string(), body: z.string().min(1) }), async (tx, args) => {
    const task = await crewTask(tx, args.taskId);
    const now = Date.now();
    await tx.write(`INSERT INTO "comments" ("id", "taskId", "authorId", "body", "createdAt") VALUES ($1, $2, $3, $4, $5)`, [
      args.id,
      task.id,
      tx.me,
      args.body,
      now,
    ]);
    await logActivity(tx, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "commented" }, now);
  }),

  "checklist.add": mutator(z.object({ id: z.string(), title: z.string().min(1) }), (tx, args) =>
    tx.write(`INSERT INTO "checklistItems" ("id", "title", "done", "ownerAccount") VALUES ($1, $2, false, $3)`, [
      args.id,
      args.title,
      tx.me,
    ]),
  ),
  "checklist.setDone": mutator(z.object({ id: z.string(), done: z.boolean() }), async (tx, args) => {
    const changed = await tx.write(`UPDATE "checklistItems" SET "done" = $2 WHERE "id" = $1 AND "ownerAccount" = $3`, [
      args.id,
      args.done,
      tx.me,
    ]);
    if (!changed) throw new Forbidden("Not your checklist");
  }),
  "checklist.delete": mutator(z.object({ id: z.string() }), async (tx, args) => {
    const changed = await tx.write(`DELETE FROM "checklistItems" WHERE "id" = $1 AND "ownerAccount" = $2`, [args.id, tx.me]);
    if (!changed) throw new Forbidden("Not your checklist");
  }),
};

export type MutationResponse = { kind: "committed"; txid: string; result?: unknown } | { kind: "unchanged"; result?: unknown };

/** Runs a mutator in one transaction and returns its txid, read from the writes' own RETURNING. */
export async function runMutation(pool: pg.Pool, me: string, name: string, rawArgs: unknown): Promise<MutationResponse> {
  const def = mutators[name];
  if (!def) throw new Forbidden(`No mutator ${name}`);
  const args = def.args.parse(rawArgs);
  const client = await pool.connect();
  let txid: string | undefined;
  const tx: Tx = {
    me,
    rows: async (sql, params) => (await client.query(sql, params)).rows,
    write: async (sql, params) => {
      const result = await client.query<{ txid: string }>(`${sql} RETURNING pg_current_xact_id()::text AS txid`, params);
      txid ??= result.rows[0]?.txid;
      return result.rowCount ?? 0;
    },
  };
  try {
    await client.query("BEGIN");
    const result = await def.run(tx, args as never);
    await client.query("COMMIT");
    return txid ? { kind: "committed", txid, result } : { kind: "unchanged", result };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

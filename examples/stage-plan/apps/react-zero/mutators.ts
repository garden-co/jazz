import { defineMutator, defineMutators, type Transaction } from "@rocicorp/zero";
import { z } from "zod";
import { TASK_STATUSES, zql, type ActivityKind, type AuthData, type TaskStatus } from "./schema.js";

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
 * Mutators run optimistically on the client and again, authoritatively, on
 * the server. The checks run on the server, where reads see all data.
 */
export const STATUS_LABELS: Record<TaskStatus, string> = {
  todo: "To do",
  doing: "In progress",
  done: "Done",
  blocked: "Blocked",
};

/** The demo show every new account starts with (see src/model/seed.ts). */
export const DEMO_SHOW = {
  name: "The Late Lanterns: album launch",
  venue: "Old Pump House",
  date: "2026-11-14",
  doors: "19:30",
};
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

function mustUser(ctx: AuthData | undefined) {
  if (!ctx?.userID) throw new Error("Not signed in");
  return ctx.userID;
}

async function isChief(tx: Transaction, me: string, showId: string) {
  return Boolean(await tx.run(zql.shows.where("id", showId).where("chiefAccount", me).one()));
}

async function assertChief(tx: Transaction, me: string, showId: string) {
  if (tx.location === "server" && !(await isChief(tx, me, showId))) {
    throw new Error("Only the crew chief can do that");
  }
}

async function assertCrew(tx: Transaction, me: string, showId: string) {
  if (tx.location !== "server") return;
  const member = await tx.run(
    zql.showCrew.where("showId", showId).where("account", me).one(),
  );
  if (!member && !(await isChief(tx, me, showId))) throw new Error("Not on this show's crew");
}

async function assertAssignable(tx: Transaction, showId: string, assigneeId: string | null) {
  if (tx.location !== "server" || assigneeId === null) return;
  const member = await tx.run(
    zql.showCrew.where("showId", showId).where("crewId", assigneeId).one(),
  );
  if (!member) throw new Error("Tasks can only be assigned to the crew");
}

/** A task the caller may edit: it exists, and they're on its show's crew. */
async function crewTask(tx: Transaction, me: string, id: string) {
  const task = await tx.run(zql.tasks.where("id", id).one());
  if (!task) throw new Error("No such task");
  await assertCrew(tx, me, task.showId);
  return task;
}

function logActivity(
  tx: Transaction,
  me: string,
  entry: { id: string; showId: string; taskId: string; kind: ActivityKind; detail?: string },
) {
  return tx.mutate.activity.insert({ ...entry, actorId: me, detail: entry.detail ?? null, createdAt: Date.now() });
}

const showInput = z.object({
  name: z.string().min(1),
  venue: z.string().min(1),
  date: z.string(),
  doors: z.string(),
});
const status = z.enum(TASK_STATUSES);

export const mutators = defineMutators({
  crew: {
    /** Creates the caller's crew profile (id = account) unless it exists. */
    ensure: defineMutator(z.object({ name: z.string() }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const existing = await tx.run(zql.crew.where("id", me).one());
      if (!existing) await tx.mutate.crew.insert({ id: me, account: me, name: args.name });
    }),
    rename: defineMutator(z.object({ name: z.string().min(1) }), async ({ tx, ctx, args }) => {
      await tx.mutate.crew.update({ id: mustUser(ctx), name: args.name });
    }),
  },

  shows: {
    /**
     * Creates a show with the caller as crew chief and a first invite code,
     * in one transaction. `demo` also fills in the demo board.
     */
    create: defineMutator(
      showInput.extend({
        id: z.string(),
        inviteId: z.string(),
        code: z.string(),
        demo: z.boolean().optional(),
      }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        const { id, inviteId, code, demo, ...input } = args;
        if (await tx.run(zql.shows.where("id", id).one())) throw new Error("Show exists");
        const now = Date.now();
        await tx.mutate.shows.insert({ id, ...input, chiefAccount: me, createdAt: now });
        await tx.mutate.showCrew.insert({ id: `${id}:${me}`, showId: id, crewId: me, account: me, role: "chief" });
        await tx.mutate.showInvites.insert({ id: inviteId, showId: id, code });
        if (!demo) return;
        // Ids derived from the show's, so client and server runs agree.
        for (const [index, task] of DEMO_TASKS.entries()) {
          const taskId = `${id}:task:${index}`;
          await tx.mutate.tasks.insert({
            id: taskId,
            showId: id,
            title: task.title,
            status: task.status,
            assigneeId: task.mine ? me : null,
            notes: task.notes ?? null,
            rank: index + 1,
            createdBy: me,
            createdAt: now,
            updatedAt: now,
          });
          await logActivity(tx, me, { id: `${taskId}:created`, showId: id, taskId, kind: "created" });
          if (task.title === "Line check") {
            await tx.mutate.comments.insert({ id: `${taskId}:comment`, taskId, authorId: me, body: DEMO_COMMENT, createdAt: now });
            await logActivity(tx, me, { id: `${taskId}:commented`, showId: id, taskId, kind: "commented" });
          }
        }
      },
    ),
    update: defineMutator(showInput.extend({ id: z.string() }), async ({ tx, ctx, args }) => {
      await assertChief(tx, mustUser(ctx), args.id);
      await tx.mutate.shows.update(args);
    }),
  },

  showCrew: {
    /** Joins a show's crew with its invite code, which the server checks. */
    join: defineMutator(z.object({ showId: z.string(), code: z.string() }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const existing = await tx.run(zql.showCrew.where("showId", args.showId).where("account", me).one());
      if (existing) return;
      if (tx.location === "server") {
        const invite = await tx.run(
          zql.showInvites.where("showId", args.showId).where("code", args.code).one(),
        );
        if (!invite) throw new Error("This invite link doesn't work any more");
      }
      await tx.mutate.showCrew.insert({ id: `${args.showId}:${me}`, showId: args.showId, crewId: me, account: me, role: "crew" });
    }),
    /** Takes someone off a show's crew; their tasks go back to nobody. */
    remove: defineMutator(z.object({ id: z.string() }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const member = await tx.run(zql.showCrew.where("id", args.id).one());
      if (!member) return;
      if (member.account !== me) await assertChief(tx, me, member.showId);
      const assigned = await tx.run(
        zql.tasks.where("showId", member.showId).where("assigneeId", member.crewId),
      );
      for (const task of assigned) await tx.mutate.tasks.update({ id: task.id, assigneeId: null });
      await tx.mutate.showCrew.delete({ id: member.id });
    }),
  },

  showInvites: {
    /** Replaces the show's invite code; links built from the old code stop working. */
    rotate: defineMutator(
      z.object({ showId: z.string(), id: z.string(), code: z.string() }),
      async ({ tx, ctx, args }) => {
        await assertChief(tx, mustUser(ctx), args.showId);
        const old = await tx.run(zql.showInvites.where("showId", args.showId));
        for (const invite of old) await tx.mutate.showInvites.delete({ id: invite.id });
        await tx.mutate.showInvites.insert(args);
      },
    ),
  },

  tasks: {
    add: defineMutator(
      z.object({
        id: z.string(),
        activityId: z.string(),
        showId: z.string(),
        title: z.string().min(1),
        status,
        rank: z.number(),
      }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        const { activityId, ...task } = args;
        await assertCrew(tx, me, task.showId);
        const now = Date.now();
        await tx.mutate.tasks.insert({ ...task, assigneeId: null, notes: null, createdBy: me, createdAt: now, updatedAt: now });
        await logActivity(tx, me, { id: activityId, showId: task.showId, taskId: task.id, kind: "created", detail: STATUS_LABELS[task.status] });
      },
    ),
    /** Moves a task to a column (and position); logs a move when the column changes. */
    move: defineMutator(
      z.object({ id: z.string(), activityId: z.string(), status, rank: z.number() }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        const task = await crewTask(tx, me, args.id);
        await tx.mutate.tasks.update({ id: task.id, status: args.status, rank: args.rank, updatedAt: Date.now() });
        if (args.status !== task.status) {
          await logActivity(tx, me, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "moved", detail: STATUS_LABELS[args.status] });
        }
      },
    ),
    assign: defineMutator(
      z.object({ id: z.string(), activityId: z.string(), assigneeId: z.string().nullable() }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        const task = await crewTask(tx, me, args.id);
        await assertAssignable(tx, task.showId, args.assigneeId);
        await tx.mutate.tasks.update({ id: task.id, assigneeId: args.assigneeId, updatedAt: Date.now() });
        const assignee = args.assigneeId ? await tx.run(zql.crew.where("id", args.assigneeId).one()) : undefined;
        await logActivity(tx, me, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "assigned", detail: assignee?.name ?? "nobody" });
      },
    ),
    rename: defineMutator(
      z.object({ id: z.string(), activityId: z.string(), title: z.string().min(1) }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        const task = await crewTask(tx, me, args.id);
        await tx.mutate.tasks.update({ id: task.id, title: args.title, updatedAt: Date.now() });
        await logActivity(tx, me, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "renamed", detail: args.title });
      },
    ),
    setNotes: defineMutator(
      z.object({ id: z.string(), notes: z.string().nullable() }),
      async ({ tx, ctx, args }) => {
        const task = await crewTask(tx, mustUser(ctx), args.id);
        await tx.mutate.tasks.update({ id: task.id, notes: args.notes, updatedAt: Date.now() });
      },
    ),
    delete: defineMutator(z.object({ id: z.string(), activityId: z.string() }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const task = await crewTask(tx, me, args.id);
      if (task.createdBy !== me) await assertChief(tx, me, task.showId);
      await logActivity(tx, me, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "deleted", detail: task.title });
      await tx.mutate.tasks.delete({ id: task.id });
    }),
  },

  comments: {
    add: defineMutator(
      z.object({ id: z.string(), activityId: z.string(), taskId: z.string(), body: z.string().min(1) }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        const task = await crewTask(tx, me, args.taskId);
        await tx.mutate.comments.insert({ id: args.id, taskId: task.id, authorId: me, body: args.body, createdAt: Date.now() });
        await logActivity(tx, me, { id: args.activityId, showId: task.showId, taskId: task.id, kind: "commented" });
      },
    ),
  },

  checklist: {
    add: defineMutator(z.object({ id: z.string(), title: z.string().min(1) }), async ({ tx, ctx, args }) => {
      await tx.mutate.checklistItems.insert({ ...args, done: false, ownerAccount: mustUser(ctx) });
    }),
    setDone: defineMutator(z.object({ id: z.string(), done: z.boolean() }), async ({ tx, ctx, args }) => {
      const item = await tx.run(zql.checklistItems.where("id", args.id).one());
      if (item?.ownerAccount !== mustUser(ctx)) throw new Error("Not your checklist");
      await tx.mutate.checklistItems.update(args);
    }),
    delete: defineMutator(z.object({ id: z.string() }), async ({ tx, ctx, args }) => {
      const item = await tx.run(zql.checklistItems.where("id", args.id).one());
      if (item?.ownerAccount !== mustUser(ctx)) throw new Error("Not your checklist");
      await tx.mutate.checklistItems.delete(args);
    }),
  },
});

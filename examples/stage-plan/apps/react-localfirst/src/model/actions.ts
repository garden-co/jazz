import type { Db, WriteResult } from "jazz-tools";
import {
  app,
  type ActivityKind,
  type Crew,
  type Show,
  type ShowCrew,
  type Task,
  type TaskStatus,
} from "../../schema.js";

export const STATUS_LABELS: Record<TaskStatus, string> = {
  todo: "To do",
  doing: "In progress",
  done: "Done",
  blocked: "Blocked",
};

/** The signed-in person: their account and their crew profile row. */
export type Me = { account: string; profile: Crew };

/** An activity row: who did what to which task, and when. */
function activityEntry(
  me: Me,
  task: Pick<Task, "id" | "showId">,
  kind: ActivityKind,
  detail?: string,
) {
  return {
    showId: task.showId,
    taskId: task.id,
    actorId: me.profile.id,
    kind,
    detail,
  };
}

/** Local rows and unsynced writes; asks the server only when there are none. */
const LOCAL_OR_SERVER = "local-first-unless-empty";

export function newInviteCode() {
  // Full UUID entropy: the link is a reusable bearer capability.
  return crypto.randomUUID();
}

export type ShowInput = Pick<Show, "name" | "venue" | "date" | "doors">;

/**
 * Creates a show with the creator as crew chief and a first invite code.
 *
 * The show is its own write, before the membership and invite: their
 * policies look up the show, and the server checks a transaction's rows
 * against data from before that transaction, so they can't share one with
 * the show (https://github.com/garden-co/jazz/issues/3755).
 *
 * Everything applies locally at once, offline too. The returned writes let a
 * caller wait for the server to accept them.
 */
export async function createShow(db: Db, me: Me, input: ShowInput) {
  const showWrite = db.insert(app.shows, { ...input, chiefAccount: me.account });
  const show = showWrite.value;
  const setup = await db.transaction((tx) => {
    tx.insert(app.showCrew, {
      showId: show.id,
      crewId: me.profile.id,
      account: me.account,
      role: "chief",
    });
    tx.insert(app.showInvites, { showId: show.id, code: newInviteCode() });
  });
  return { show, writes: [showWrite, setup] as WriteResult<unknown>[] };
}

/**
 * Re-adds the chief's membership and invite when either is missing, for
 * example because the server rejected that write. The check includes writes
 * that haven't synced yet, so it doesn't add a second membership, and it
 * works offline.
 */
export async function ensureChiefSetup(db: Db, me: Me, showId: string) {
  const [membership, invites] = await Promise.all([
    db.one(app.showCrew.where({ showId, account: me.account }), { tier: LOCAL_OR_SERVER }),
    db.all(app.showInvites.where({ showId }), { tier: LOCAL_OR_SERVER }),
  ]);
  if (membership && invites.length > 0) return [];
  const setup = await db.transaction((tx) => {
    if (!membership) {
      tx.insert(app.showCrew, {
        showId,
        crewId: me.profile.id,
        account: me.account,
        role: "chief",
      });
    }
    if (invites.length === 0) tx.insert(app.showInvites, { showId, code: newInviteCode() });
  });
  return [setup] as WriteResult<unknown>[];
}

export function updateShow(db: Db, showId: string, input: ShowInput) {
  return db.update(app.shows, showId, input);
}

export async function addTask(
  db: Db,
  me: Me,
  showId: string,
  title: string,
  status: TaskStatus,
  rank: number,
) {
  const result = await db.transaction((tx) => {
    const task = tx.insert(app.tasks, {
      showId,
      title,
      status,
      rank,
    });
    tx.insert(app.activity, activityEntry(me, task, "created", STATUS_LABELS[status]));
    return task;
  });
  return result.value;
}

/** Moves a task to a column (and position); logs a move when the column changes. */
export function moveTask(db: Db, me: Me, task: Task, status: TaskStatus, rank: number) {
  return db.transaction((tx) => {
    tx.update(app.tasks, task.id, { status, rank });
    if (status !== task.status)
      tx.insert(app.activity, activityEntry(me, task, "moved", STATUS_LABELS[status]));
  });
}

export function assignTask(db: Db, me: Me, task: Task, assignee: Crew | null) {
  return db.transaction((tx) => {
    tx.update(app.tasks, task.id, { assigneeId: assignee?.id ?? null });
    tx.insert(app.activity, activityEntry(me, task, "assigned", assignee?.name ?? "nobody"));
  });
}

export function renameTask(db: Db, me: Me, task: Task, title: string) {
  return db.transaction((tx) => {
    tx.update(app.tasks, task.id, { title });
    tx.insert(app.activity, activityEntry(me, task, "renamed", title));
  });
}

export function updateTaskNotes(db: Db, task: Task, notes: string) {
  return db.update(app.tasks, task.id, { notes: notes || null });
}

export function deleteTask(db: Db, me: Me, task: Task) {
  return db.transaction((tx) => {
    tx.insert(app.activity, activityEntry(me, task, "deleted", task.title));
    tx.delete(app.tasks, task.id);
  });
}

export function addComment(db: Db, me: Me, task: Task, body: string) {
  return db.transaction((tx) => {
    tx.insert(app.comments, {
      taskId: task.id,
      authorId: me.profile.id,
      body,
    });
    tx.insert(app.activity, activityEntry(me, task, "commented"));
  });
}

/** Replaces the show's invite code; links built from the old code stop working. */
export function rotateInvite(db: Db, showId: string, oldInviteIds: string[]) {
  return db.transaction((tx) => {
    for (const id of oldInviteIds) tx.delete(app.showInvites, id);
    tx.insert(app.showInvites, { showId, code: newInviteCode() });
  });
}

/**
 * Joins a show with an invite code. The server checks the code against the
 * private invite; once it accepts, the code is cleared from the membership so
 * the rest of the crew can't read it.
 */
export async function joinShow(db: Db, me: Me, showId: string, code: string) {
  let membership = await db.one(app.showCrew.where({ showId, account: me.account }));
  if (!membership) {
    membership = await db
      .insert(app.showCrew, {
        showId,
        crewId: me.profile.id,
        account: me.account,
        role: "crew",
        inviteCode: code,
      })
      .wait({ tier: "global" });
  }
  if (membership.inviteCode) {
    await db.update(app.showCrew, membership.id, { inviteCode: null }).wait({ tier: "global" });
  }
}

/** Rank that puts a task after every task in the column. */
export function rankAfter(tasks: Pick<Task, "rank">[]) {
  return tasks.reduce((max, task) => Math.max(max, task.rank), 0) + 1;
}

/** Rank between two neighbours; either side may be missing. */
export function rankBetween(before?: number, after?: number) {
  if (before === undefined && after === undefined) return 1;
  if (before === undefined) return after! - 1;
  if (after === undefined) return before + 1;
  return (before + after) / 2;
}

/** A show's crew membership with the person's profile included. */
export type CrewMember = ShowCrew & { crew?: Crew | null };

import type { Db, TransactionScope, WriteResult } from "jazz-tools";
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
 * Stable ids for the chief's membership and a show's first invite. Every
 * write of these rows, including a repair from another tab, lands on the same
 * row instead of adding a second one.
 */
export function chiefMembershipId(showId: string, account: string) {
  return nameBasedId(`stage-plan/chief/${showId}/${account}`);
}
export function firstInviteId(showId: string) {
  return nameBasedId(`stage-plan/first-invite/${showId}`);
}

/** A mergeable transaction that the helpers below stage their rows into. */
export type Tx = TransactionScope<"mergeable">;

/**
 * Creates a show with the creator as crew chief and a first invite code, in
 * one transaction: the membership's and invite's policies look up the show,
 * and they see the show inserted earlier in the same transaction.
 *
 * Everything applies locally at once, offline too. The returned writes let a
 * caller wait for the server to accept them.
 */
export async function createShow(db: Db, me: Me, input: ShowInput) {
  const created = await db.transaction((tx) => stageShow(tx, me, input));
  return { show: created.value, writes: [created] as WriteResult<unknown>[] };
}

/** Stages a show with its crew chief and first invite into `tx`. */
export async function stageShow(tx: Tx, me: Me, input: ShowInput) {
  const show = tx.insert(app.shows, { ...input, chiefAccount: me.account });
  const [membershipId, inviteId] = await Promise.all([
    chiefMembershipId(show.id, me.account),
    firstInviteId(show.id),
  ]);
  tx.upsert(app.showCrew, membershipId, chiefMembership(show.id, me));
  tx.upsert(app.showInvites, inviteId, { showId: show.id, code: newInviteCode() });
  return show;
}

function chiefMembership(showId: string, me: Me) {
  return { showId, crewId: me.profile.id, account: me.account, role: "chief" as const };
}

/**
 * Re-adds the chief's membership and first invite when either is missing,
 * for example because the server rejected that write. The rows have stable
 * ids, so repairs from two tabs write the same rows.
 */
export async function ensureChiefSetup(db: Db, me: Me, showId: string) {
  const [membership, invites] = await Promise.all([
    db.one(app.showCrew.where({ showId, account: me.account }), { tier: LOCAL_OR_SERVER }),
    db.all(app.showInvites.where({ showId }), { tier: LOCAL_OR_SERVER }),
  ]);
  if (membership && invites.length > 0) return [];
  // The invite code may already be out in a copied link, and an upsert would
  // replace it. So a missing invite is only written once the server confirms
  // it has none; offline, this waits for the connection instead of guessing.
  const needsInvite =
    invites.length === 0 &&
    (await db.all(app.showInvites.where({ showId }), { tier: "remote" })).length === 0;
  if (membership && !needsInvite) return [];
  const [membershipId, inviteId] = await Promise.all([
    chiefMembershipId(showId, me.account),
    firstInviteId(showId),
  ]);
  const setup = await db.transaction((tx) => {
    if (!membership) tx.upsert(app.showCrew, membershipId, chiefMembership(showId, me));
    if (needsInvite) tx.upsert(app.showInvites, inviteId, { showId, code: newInviteCode() });
  });
  return [setup] as WriteResult<unknown>[];
}

/**
 * Takes someone off a show's crew. Their tasks go back to nobody in the same
 * transaction, because tasks may only be assigned to crew.
 */
export async function removeFromCrew(db: Db, member: ShowCrew) {
  const assigned = await db.all(
    app.tasks.where({ showId: member.showId, assigneeId: member.crewId }),
  );
  return db.transaction((tx) => {
    for (const task of assigned) tx.update(app.tasks, task.id, { assigneeId: null });
    tx.delete(app.showCrew, member.id);
  });
}

/** A name-based (version 5) UUID, so the same name always gives the same id. */
async function nameBasedId(name: string) {
  const namespace = hexBytes("6f9d1c8e2b4a4e7d9c3a5b1e8f2d7a64");
  const bytes = new Uint8Array([...namespace, ...new TextEncoder().encode(name)]);
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-1", bytes)).slice(0, 16);
  hash[6] = (hash[6]! & 0x0f) | 0x50;
  hash[8] = (hash[8]! & 0x3f) | 0x80;
  const hex = [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function hexBytes(hex: string) {
  return Uint8Array.from(hex.match(/../g)!, (pair) => parseInt(pair, 16));
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
  return db.transaction((tx) => stageComment(tx, me, task, body));
}

/** Stages a comment and its activity entry into `tx`. */
export function stageComment(tx: Tx, me: Me, task: Pick<Task, "id" | "showId">, body: string) {
  tx.insert(app.comments, {
    taskId: task.id,
    authorId: me.profile.id,
    body,
  });
  tx.insert(app.activity, activityEntry(me, task, "commented"));
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

import { id } from "@instantdb/react";
import {
  db,
  transact,
  type ActivityKind,
  type Crew,
  type Show,
  type ShowCrew,
  type Task,
  type TaskStatus,
} from "./db.js";

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
  return db.tx.activity[id()]
    .create({ taskId: task.id, kind, detail, createdAt: Date.now() })
    .link({ show: task.showId, actor: me.profile.id });
}

export function newInviteCode() {
  // Full UUID entropy: the link is a reusable bearer capability.
  return crypto.randomUUID();
}

export type ShowInput = Pick<Show, "name" | "venue" | "date" | "doors">;

/**
 * Creates a show with the creator as crew chief and a first invite code, in
 * one transaction: Instant commits all of it or none of it.
 */
export function createShow(me: Me, input: ShowInput) {
  const showId = id();
  const write = transact(stageShow(me, showId, input));
  return { show: { id: showId }, write };
}

/** The chunks for a show with its crew chief and first invite. */
export function stageShow(me: Me, showId: string, input: ShowInput) {
  return [
    db.tx.shows[showId].create({ ...input, chiefAccount: me.account }),
    db.tx.showCrew[id()]
      .create({ account: me.account, role: "chief" })
      .link({ show: showId, crew: me.profile.id }),
    db.tx.showInvites[id()].create({ code: newInviteCode() }).link({ show: showId }),
  ];
}

/**
 * Takes someone off a show's crew. Their tasks go back to nobody in the same
 * transaction, because tasks may only be assigned to crew.
 */
export async function removeFromCrew(member: ShowCrew) {
  const { data } = await db.queryOnce({
    tasks: { $: { where: { show: member.showId, assigneeId: member.crewId }, fields: ["id"] } },
  });
  return transact([
    ...data.tasks.map((task) => db.tx.tasks[task.id].update({ assigneeId: null })),
    db.tx.showCrew[member.id].delete(),
  ]);
}

export function updateShow(showId: string, input: ShowInput) {
  return transact(db.tx.shows[showId].update(input));
}

export function addTask(me: Me, showId: string, title: string, status: TaskStatus, rank: number) {
  const task = { id: id(), showId };
  void transact([
    db.tx.tasks[task.id]
      .create({ title, status, rank, creatorAccount: me.account, updatedAt: Date.now() })
      .link({ show: showId }),
    activityEntry(me, task, "created", STATUS_LABELS[status]),
  ]);
  return task;
}

/** Moves a task to a column (and position); logs a move when the column changes. */
export function moveTask(me: Me, task: Task, status: TaskStatus, rank: number) {
  return transact([
    db.tx.tasks[task.id].update({ status, rank, updatedAt: Date.now() }),
    ...(status !== task.status ? [activityEntry(me, task, "moved", STATUS_LABELS[status])] : []),
  ]);
}

export function assignTask(me: Me, task: Task, assignee: Crew | null) {
  return transact([
    db.tx.tasks[task.id].update({ assigneeId: assignee?.id ?? null, updatedAt: Date.now() }),
    activityEntry(me, task, "assigned", assignee?.name ?? "nobody"),
  ]);
}

export function renameTask(me: Me, task: Task, title: string) {
  return transact([
    db.tx.tasks[task.id].update({ title, updatedAt: Date.now() }),
    activityEntry(me, task, "renamed", title),
  ]);
}

/**
 * Saves the notes typed in the task dialog. Instant has no text merge, so the
 * last write wins (the Jazz app merges concurrent edits).
 */
export function updateTaskNotes(task: Task, notes: string) {
  return transact(db.tx.tasks[task.id].update({ notes: notes || null, updatedAt: Date.now() }));
}

export function deleteTask(me: Me, task: Task) {
  return transact([activityEntry(me, task, "deleted", task.title), db.tx.tasks[task.id].delete()]);
}

export function addComment(me: Me, task: Pick<Task, "id" | "showId">, body: string) {
  return transact(stageComment(me, task, body));
}

/** The chunks for a comment and its activity entry. */
export function stageComment(me: Me, task: Pick<Task, "id" | "showId">, body: string) {
  return [
    db.tx.comments[id()]
      .create({ body, createdAt: Date.now() })
      .link({ task: task.id, author: me.profile.id }),
    activityEntry(me, task, "commented"),
  ];
}

/** Replaces the show's invite code; links built from the old code stop working. */
export function rotateInvite(showId: string, oldInviteIds: string[]) {
  return transact([
    ...oldInviteIds.map((inviteId) => db.tx.showInvites[inviteId].delete()),
    db.tx.showInvites[id()].create({ code: newInviteCode() }).link({ show: showId }),
  ]);
}

/**
 * Joins a show with an invite code. The code goes to the server as a rule
 * param, which checks it against the private invite; it is never stored.
 */
export async function joinShow(me: Me, showId: string, code: string) {
  const { data } = await db.queryOnce({
    showCrew: { $: { where: { show: showId, account: me.account }, fields: ["id"] } },
  });
  if (data.showCrew.length > 0) return;
  await transact(
    db.tx.showCrew[id()]
      .ruleParams({ inviteCode: code })
      .create({ account: me.account, role: "crew" })
      .link({ show: showId, crew: me.profile.id }),
  );
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

import { STATUS_LABELS } from "../../demo.js";
import type { Crew, Show, ShowCrew, Task, TaskStatus } from "../../schema.js";
import { mutate, onWriteError, type Overlay, type Write } from "./live.js";

export { STATUS_LABELS };

/** The signed-in person: their account and their crew profile row. */
export type Me = { account: string; profile: Crew };

/** The signed-in account; the actions take it where Zero's took its client. */
export type Db = { userID: string };

// A write the server rejects is rolled back; tell the listeners (the toasts).
export function onMutationError(listener: (kind: "app") => void) {
  return onWriteError(() => listener("app"));
}

const id = () => crypto.randomUUID();
type Rows = Parameters<Overlay[string]>[0];

/** Overlay helpers: change the task rows of the show's board at once. */
const byRank = (a: Rows[number], b: Rows[number]) => (a.rank as number) - (b.rank as number);
const patchTask = (taskId: string, patch: Partial<Task>): Overlay => ({
  "tasks.byShow": (rows) =>
    rows.some((row) => row.id === taskId)
      ? rows.map((row) => (row.id === taskId ? { ...row, ...patch } : row)).sort(byRank)
      : rows,
});

export function newInviteCode() {
  // Full UUID entropy: the link is a reusable bearer capability.
  return crypto.randomUUID();
}

export type ShowInput = Pick<Show, "name" | "venue" | "date" | "doors">;

/** Creates a show with the creator as crew chief and a first invite code, in one transaction. */
export function createShow(_db: Db, _me: Me, input: ShowInput, demo = false) {
  const showId = id();
  const write = mutate("shows.create", { ...input, id: showId, inviteId: id(), code: newInviteCode(), demo });
  return { show: { id: showId }, write };
}

/** Takes someone off a show's crew. Their tasks go back to nobody in the same transaction. */
export function removeFromCrew(_db: Db, member: ShowCrew) {
  return mutate("showCrew.remove", { id: member.id }, {
    "showCrew.byShow": (rows) => (rows.some((row) => row.id === member.id) ? rows.filter((row) => row.id !== member.id) : rows),
  });
}

export function updateShow(_db: Db, showId: string, input: ShowInput) {
  return mutate("shows.update", { id: showId, ...input }, {
    "shows.one": (rows, args) => (args.showId === showId ? rows.map((row) => ({ ...row, ...input })) : rows),
  });
}

export function addTask(_db: Db, me: Me, showId: string, title: string, status: TaskStatus, rank: number): Write {
  const taskId = id();
  const now = Date.now();
  const task: Task = { id: taskId, showId, title, status, rank, assigneeId: null, notes: null, createdBy: me.account, createdAt: now, updatedAt: now };
  return mutate("tasks.add", { id: taskId, activityId: id(), showId, title, status, rank }, {
    "tasks.byShow": (rows, args) =>
      args.showId !== showId || rows.some((row) => row.id === taskId) ? rows : [...rows, task].sort(byRank),
  });
}

/** Moves a task to a column (and position); logs a move when the column changes. */
export function moveTask(_db: Db, _me: Me, task: Task, status: TaskStatus, rank: number) {
  return mutate("tasks.move", { id: task.id, activityId: id(), status, rank }, patchTask(task.id, { status, rank }));
}

export function assignTask(_db: Db, _me: Me, task: Task, assignee: Crew | null) {
  return mutate("tasks.assign", { id: task.id, activityId: id(), assigneeId: assignee?.id ?? null }, patchTask(task.id, { assigneeId: assignee?.id ?? null }));
}

export function renameTask(_db: Db, _me: Me, task: Task, title: string) {
  return mutate("tasks.rename", { id: task.id, activityId: id(), title }, patchTask(task.id, { title }));
}

/**
 * Saves the notes typed in the task dialog. Postgres has no text merge: the
 * last write wins (Jazz merges concurrent edits; see NOTES.md).
 */
export function updateTaskNotes(_db: Db, task: Task, notes: string, _editedFrom: string | null) {
  return mutate("tasks.setNotes", { id: task.id, notes: notes || null }, patchTask(task.id, { notes: notes || null }));
}

export function deleteTask(_db: Db, _me: Me, task: Task) {
  return mutate("tasks.delete", { id: task.id, activityId: id() }, {
    "tasks.byShow": (rows) => (rows.some((row) => row.id === task.id) ? rows.filter((row) => row.id !== task.id) : rows),
  });
}

export function addComment(_db: Db, me: Me, task: Task, body: string) {
  const commentId = id();
  const comment = { id: commentId, taskId: task.id, authorId: me.account, body, createdAt: Date.now(), author: me.profile };
  return mutate("comments.add", { id: commentId, activityId: id(), taskId: task.id, body }, {
    "comments.byTask": (rows, args) =>
      args.taskId !== task.id || rows.some((row) => row.id === commentId) ? rows : [...rows, comment],
  });
}

/** Replaces the show's invite code; links built from the old code stop working. */
export function rotateInvite(_db: Db, showId: string) {
  const invite = { id: id(), showId, code: newInviteCode() };
  return mutate("showInvites.rotate", invite, {
    "showInvites.byShow": (rows, args) => (args.showId === showId ? [invite] : rows),
  });
}

/** Joins a show with an invite code; rejects if the server refuses the code. */
export async function joinShow(_db: Db, _me: Me, showId: string, code: string) {
  const result = await mutate("showCrew.join", { showId, code }).server;
  if (result.type === "error") throw new Error(result.error.message);
}

const sortByTitle = (rows: Rows) => [...rows].sort((a, b) => String(a.title).localeCompare(String(b.title)));
export function addChecklistItem(db: Db, title: string) {
  const item = { id: id(), title, done: false, ownerAccount: db.userID };
  return mutate("checklist.add", { id: item.id, title }, {
    "checklist.mine": (rows, args) =>
      rows.some((row) => row.id === item.id) ||
      (args.filter === "done") ||
      !title.includes(String(args.search ?? ""))
        ? rows
        : sortByTitle([...rows, item]),
  });
}
export function setChecklistItemDone(_db: Db, itemId: string, done: boolean) {
  return mutate("checklist.setDone", { id: itemId, done }, {
    "checklist.mine": (rows, args) =>
      !rows.some((row) => row.id === itemId) ? rows : rows
        .map((row) => (row.id === itemId ? { ...row, done } : row))
        .filter((row) => args.filter === "all" || row.id !== itemId || row.done === (args.filter === "done")),
  });
}
export function deleteChecklistItem(_db: Db, itemId: string) {
  return mutate("checklist.delete", { id: itemId }, {
    "checklist.mine": (rows) => (rows.some((row) => row.id === itemId) ? rows.filter((row) => row.id !== itemId) : rows),
  });
}
export function renameMe(_db: Db, name: string) {
  return mutate("crew.rename", { name }, {
    "crew.mine": (rows) => rows.map((row) => ({ ...row, name })),
  });
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

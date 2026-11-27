import { call } from "../data/api.js";
import type { Db } from "../data/live.js";
import type { CrewMember } from "../data/queries.js";
import type { ChecklistItem, Crew, Show, ShowCrew, Task, TaskStatus } from "../../shared/types.js";

export type { CrewMember };

export const STATUS_LABELS: Record<TaskStatus, string> = {
  todo: "To do",
  doing: "In progress",
  done: "Done",
  blocked: "Blocked",
};

/** The signed-in person: their account and their crew profile row. */
export type Me = { account: string; profile: Crew };

/** A write and the server's answer (it never rejects: errors come back as `type: "error"`). */
export type Write = { server: Promise<{ type: "success" } | { type: "error"; error: Error }> };

// Tell the listeners (the toasts) when the server refuses a write.
type ErrorKind = "app" | "network";
const errorListeners = new Set<(kind: ErrorKind) => void>();
export function onMutationError(listener: (kind: ErrorKind) => void) {
  errorListeners.add(listener);
  return () => void errorListeners.delete(listener);
}

/**
 * Sends a write. The board and the checklist pass `optimistic` to show the
 * change at once; if the server refuses, the affected queries refetch.
 * Everything else shows up when the server's change event arrives.
 */
function run(db: Db, name: string, args: object, optimistic?: { key: readonly unknown[]; patch: (data: any) => any }): Write {
  const sent = call(name, args);
  if (optimistic) {
    // As in TanStack's optimistic-update docs: stop refetches that would
    // overwrite the patch, then patch the cache.
    const { key, patch } = optimistic;
    void db.cancelQueries({ queryKey: key }).then(() =>
      db.setQueriesData({ queryKey: key }, (data) => (data === undefined ? data : patch(data))),
    );
  }
  const server = sent.then(
    () => ({ type: "success" as const }),
    (error: Error & { status?: number }) => {
      for (const listener of errorListeners) listener(error.status ? "app" : "network");
      if (optimistic) void db.invalidateQueries({ queryKey: optimistic.key });
      return { type: "error" as const, error };
    },
  );
  return { server };
}

const id = () => crypto.randomUUID();

export function newInviteCode() {
  // Full UUID entropy: the link is a reusable bearer capability.
  return crypto.randomUUID();
}

export type ShowInput = Pick<Show, "name" | "venue" | "date" | "doors">;

/** Creates a show with the creator as crew chief and a first invite code, in one call. */
export async function createShow(db: Db, _me: Me, input: ShowInput, demo = false) {
  const showId = id();
  const write = run(db, "shows.create", { ...input, id: showId, inviteId: id(), code: newInviteCode(), demo });
  await write.server;
  return { show: { id: showId }, write };
}

/** Takes someone off a show's crew. Their tasks go back to nobody in the same call. */
export function removeFromCrew(db: Db, member: ShowCrew) {
  return run(db, "showCrew.remove", { id: member.id });
}

export function updateShow(db: Db, showId: string, input: ShowInput) {
  return run(db, "shows.update", { id: showId, ...input });
}

export function addTask(db: Db, _me: Me, showId: string, title: string, status: TaskStatus, rank: number) {
  return run(db, "tasks.add", { id: id(), activityId: id(), showId, title, status, rank });
}

/** Moves a task to a column (and position), optimistically, as a drag-and-drop board does. */
export function moveTask(db: Db, _me: Me, task: Task, status: TaskStatus, rank: number) {
  return run(
    db,
    "tasks.move",
    { id: task.id, activityId: id(), status, rank },
    {
      key: ["show", task.showId, "tasks"],
      patch: (tasks: Task[]) =>
        tasks
          .map((t) => (t.id === task.id ? { ...t, status, rank, updatedAt: Date.now() } : t))
          .sort((a, b) => a.rank - b.rank),
    },
  );
}

export function assignTask(db: Db, _me: Me, task: Task, assignee: Crew | null) {
  return run(db, "tasks.assign", { id: task.id, activityId: id(), assigneeId: assignee?.id ?? null });
}

export function renameTask(db: Db, _me: Me, task: Task, title: string) {
  return run(db, "tasks.rename", { id: task.id, activityId: id(), title });
}

/** Saves the notes typed in the task dialog. The last write wins (Jazz merges; see NOTES.md). */
export function updateTaskNotes(db: Db, task: Task, notes: string, _editedFrom: string | null) {
  return run(db, "tasks.setNotes", { id: task.id, notes: notes || null });
}

export function deleteTask(db: Db, _me: Me, task: Task) {
  return run(db, "tasks.delete", { id: task.id, activityId: id() });
}

export function addComment(db: Db, _me: Me, task: Task, body: string) {
  return run(db, "comments.add", { id: id(), activityId: id(), taskId: task.id, body });
}

/** Replaces the show's invite code; links built from the old code stop working. */
export function rotateInvite(db: Db, showId: string) {
  return run(db, "showInvites.rotate", { showId, id: id(), code: newInviteCode() });
}

/** Joins a show with an invite code; rejects if the server refuses the code. */
export async function joinShow(_db: Db, _me: Me, showId: string, code: string) {
  await call("showCrew.join", { showId, code });
}

export function addChecklistItem(db: Db, title: string) {
  return run(db, "checklist.add", { id: id(), title });
}
export function setChecklistItemDone(db: Db, itemId: string, done: boolean) {
  return run(
    db,
    "checklist.setDone",
    { id: itemId, done },
    { key: ["checklist"], patch: (items: ChecklistItem[]) => items.map((i) => (i.id === itemId ? { ...i, done } : i)) },
  );
}
export function deleteChecklistItem(db: Db, itemId: string) {
  return run(db, "checklist.delete", { id: itemId });
}
export function renameMe(db: Db, name: string) {
  return run(db, "crew.rename", { name });
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

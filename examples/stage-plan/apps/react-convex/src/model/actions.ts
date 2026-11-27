import type { Doc, Id } from "../../convex/_generated/dataModel.js";

export { STATUS_LABELS } from "../../convex/lib/stage.js";
export type { ShowInput } from "../../convex/lib/stage.js";

/** The signed-in person: their account and their crew profile row. */
export type Me = { account: Id<"users">; profile: Doc<"crew"> };

/** Rank that puts a task after every task in the column. */
export function rankAfter(tasks: Pick<Doc<"tasks">, "rank">[]) {
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
export type CrewMember = Doc<"showCrew"> & { crew?: Doc<"crew"> | null };

export const TASK_STATUSES = ["todo", "doing", "done", "blocked"] as const;
export type TaskStatus = Doc<"tasks">["status"];
export type Task = Doc<"tasks">;
export type Show = Doc<"shows">;
export type Activity = Doc<"activity">;

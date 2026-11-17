// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema is schema.ts (for `generate` only); its types are in jazz.generated.ts.

/**
 * StagePlan: a crew preparing shows.
 *
 * The core tables mirror the W1 "team task board" workload one to one, so the
 * benchmark can later run against this schema:
 *
 *   W1 users     -> crew       (a person's display profile)
 *   W1 projects  -> shows      (a gig with venue, date and doors time)
 *   W1 tasks     -> tasks      (show, title, status, assignee; updated_at is $updatedAt)
 *   W1 comments  -> comments   (task, author, body; created_at is $createdAt)
 *   W1 activity  -> activity   (show, task, actor, kind; created_at is $createdAt)
 *
 * On top of that, StagePlan adds what a real app needs for permissions
 * (showCrew memberships and private showInvites) and a personal checklist.
 */
export const TASK_STATUSES = ["todo", "doing", "done", "blocked"] as const;
export type TaskStatus = (typeof TASK_STATUSES)[number];
export const ACTIVITY_KINDS = [
  "created",
  "moved",
  "assigned",
  "renamed",
  "commented",
  "deleted",
] as const;

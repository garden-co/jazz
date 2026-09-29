import { schema as s } from "jazz-tools";

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

const schema = {
  crew: s.table(
    {
      account: s.uuid(),
      name: s.string(),
    },
    {
      memberships: s.reverse("showCrew", "crew"),
      assignedTasks: s.reverse("tasks", "assignee"),
    },
  ),
  shows: s.table(
    {
      name: s.string(),
      venue: s.string(),
      /** Local date of the show, as an ISO date (YYYY-MM-DD). */
      date: s.string(),
      /** Local doors time at the venue (HH:MM). */
      doors: s.string(),
      /** The crew chief: the account that created and manages the show. */
      chiefAccount: s.uuid(),
    },
    {
      crew: s.reverse("showCrew", "show"),
      tasks: s.reverse("tasks", "show"),
      activity: s.reverse("activity", "show"),
      invites: s.reverse("showInvites", "show"),
    },
  ),
  showCrew: s.table(
    {
      showId: s.uuid(),
      crewId: s.uuid(),
      account: s.uuid(),
      role: s.enum("chief", "crew"),
      /** The invite code a crew member joined with; checked by permissions. */
      inviteCode: s.string().optional(),
    },
    {
      show: s.rel("shows", "showId"),
      crew: s.rel("crew", "crewId"),
    },
  ),
  showInvites: s.table(
    {
      showId: s.uuid(),
      code: s.string(),
    },
    { show: s.rel("shows", "showId") },
  ),
  tasks: s.table(
    {
      showId: s.uuid(),
      title: s.string(),
      status: s.enum(...TASK_STATUSES),
      assigneeId: s.uuid().optional(),
      notes: s.string().optional(),
      /** Sort key inside a column. Lower values come first. */
      rank: s.float(),
    },
    {
      show: s.rel("shows", "showId"),
      assignee: s.rel("crew", "assigneeId"),
      comments: s.reverse("comments", "task"),
      activity: s.reverse("activity", "task"),
    },
  ),
  comments: s.table(
    {
      taskId: s.uuid(),
      authorId: s.uuid(),
      body: s.string(),
    },
    {
      task: s.rel("tasks", "taskId"),
      author: s.rel("crew", "authorId"),
    },
  ),
  activity: s.table(
    {
      showId: s.uuid(),
      taskId: s.uuid(),
      actorId: s.uuid(),
      kind: s.enum(...ACTIVITY_KINDS),
      /** Short human-readable detail, such as the column a task moved to. */
      detail: s.string().optional(),
    },
    {
      show: s.rel("shows", "showId"),
      task: s.rel("tasks", "taskId"),
      actor: s.rel("crew", "actorId"),
    },
  ),
  checklistItems: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      ownerAccount: s.uuid(),
    },
    {},
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Crew = s.RowOf<typeof app.crew>;
export type Show = s.RowOf<typeof app.shows>;
export type ShowCrew = s.RowOf<typeof app.showCrew>;
export type Task = s.RowOf<typeof app.tasks>;
export type Comment = s.RowOf<typeof app.comments>;
export type Activity = s.RowOf<typeof app.activity>;
export type ActivityKind = Activity["kind"];
export type ChecklistItem = s.RowOf<typeof app.checklistItems>;

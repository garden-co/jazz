import { i } from "@instantdb/react";

/**
 * StagePlan: a crew preparing shows. The same tables as the Jazz app; rows
 * that belong to a show (or a task) are linked to it, so permissions can
 * follow the link with `data.ref(...)`. Accounts are Instant `$users` ids.
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
export type ActivityKind = (typeof ACTIVITY_KINDS)[number];

const _schema = i.schema({
  entities: {
    $users: i.entity({
      email: i.string().unique().indexed().optional(),
    }),
    crew: i.entity({
      account: i.string().unique().indexed(),
      name: i.string(),
    }),
    shows: i.entity({
      name: i.string(),
      venue: i.string(),
      /** Local date of the show, as an ISO date (YYYY-MM-DD). */
      date: i.string().indexed(),
      /** Local doors time at the venue (HH:MM). */
      doors: i.string(),
      /** The crew chief: the account that created and manages the show. */
      chiefAccount: i.string().indexed(),
    }),
    showCrew: i.entity({
      account: i.string().indexed(),
      role: i.string<"chief" | "crew">().indexed(),
    }),
    showInvites: i.entity({
      code: i.string().indexed(),
    }),
    tasks: i.entity({
      title: i.string(),
      status: i.string<TaskStatus>().indexed(),
      /** A crew profile id, or null. Permissions check it is on the show's crew. */
      assigneeId: i.string().indexed().optional(),
      notes: i.string().optional(),
      /** Sort key inside a column. Lower values come first. */
      rank: i.number().indexed(),
      creatorAccount: i.string().indexed(),
      updatedAt: i.date(),
    }),
    comments: i.entity({
      body: i.string(),
      createdAt: i.date().indexed(),
    }),
    activity: i.entity({
      /** The task's id; kept after the task is deleted, like the Jazz row. */
      taskId: i.string().indexed(),
      kind: i.string<ActivityKind>(),
      /** Short human-readable detail, such as the column a task moved to. */
      detail: i.string().optional(),
      createdAt: i.date().indexed(),
    }),
    checklistItems: i.entity({
      title: i.string().indexed(),
      done: i.boolean().indexed(),
      ownerAccount: i.string().indexed(),
    }),
  },
  links: {
    showCrewShow: {
      forward: { on: "showCrew", has: "one", label: "show", required: true, onDelete: "cascade" },
      reverse: { on: "shows", has: "many", label: "crew" },
    },
    showCrewProfile: {
      forward: { on: "showCrew", has: "one", label: "crew", required: true, onDelete: "cascade" },
      reverse: { on: "crew", has: "many", label: "memberships" },
    },
    showInvitesShow: {
      forward: {
        on: "showInvites",
        has: "one",
        label: "show",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "shows", has: "many", label: "invites" },
    },
    tasksShow: {
      forward: { on: "tasks", has: "one", label: "show", required: true, onDelete: "cascade" },
      reverse: { on: "shows", has: "many", label: "tasks" },
    },
    commentsTask: {
      forward: { on: "comments", has: "one", label: "task", required: true, onDelete: "cascade" },
      reverse: { on: "tasks", has: "many", label: "comments" },
    },
    commentsAuthor: {
      forward: { on: "comments", has: "one", label: "author" },
      reverse: { on: "crew", has: "many", label: "comments" },
    },
    activityShow: {
      forward: { on: "activity", has: "one", label: "show", required: true, onDelete: "cascade" },
      reverse: { on: "shows", has: "many", label: "activity" },
    },
    activityActor: {
      forward: { on: "activity", has: "one", label: "actor" },
      reverse: { on: "crew", has: "many", label: "activity" },
    },
  },
});

// This helps TypeScript display better IntelliSense
type _AppSchema = typeof _schema;
interface AppSchema extends _AppSchema {}
const schema: AppSchema = _schema;

export type { AppSchema };
export default schema;

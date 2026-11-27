import {
  boolean,
  createBuilder,
  createSchema,
  enumeration,
  number,
  relationships,
  string,
  table,
  type Row,
} from "@rocicorp/zero";

/**
 * StagePlan: a crew preparing shows. The Postgres tables are in
 * db/001_init.sql; this is the part of them Zero syncs.
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

const crew = table("crew")
  .columns({ id: string(), account: string(), name: string() })
  .primaryKey("id");

const shows = table("shows")
  .columns({
    id: string(),
    name: string(),
    venue: string(),
    /** Local date of the show, as an ISO date (YYYY-MM-DD). */
    date: string(),
    /** Local doors time at the venue (HH:MM). */
    doors: string(),
    /** The crew chief: the account that created and manages the show. */
    chiefAccount: string(),
    createdAt: number(),
  })
  .primaryKey("id");

const showCrew = table("showCrew")
  .columns({
    id: string(),
    showId: string(),
    crewId: string(),
    account: string(),
    role: enumeration<"chief" | "crew">(),
  })
  .primaryKey("id");

const showInvites = table("showInvites")
  .columns({ id: string(), showId: string(), code: string() })
  .primaryKey("id");

const tasks = table("tasks")
  .columns({
    id: string(),
    showId: string(),
    title: string(),
    status: enumeration<TaskStatus>(),
    assigneeId: string().optional(),
    notes: string().optional(),
    /** Sort key inside a column. Lower values come first. */
    rank: number(),
    /** The account that created the task: it may delete it. */
    createdBy: string(),
    createdAt: number(),
    updatedAt: number(),
  })
  .primaryKey("id");

const comments = table("comments")
  .columns({
    id: string(),
    taskId: string(),
    authorId: string(),
    body: string(),
    createdAt: number(),
  })
  .primaryKey("id");

const activity = table("activity")
  .columns({
    id: string(),
    showId: string(),
    taskId: string(),
    actorId: string(),
    kind: enumeration<ActivityKind>(),
    /** Short human-readable detail, such as the column a task moved to. */
    detail: string().optional(),
    createdAt: number(),
  })
  .primaryKey("id");

const checklistItems = table("checklistItems")
  .columns({ id: string(), title: string(), done: boolean(), ownerAccount: string() })
  .primaryKey("id");

const showRelationships = relationships(shows, ({ many }) => ({
  crew: many({ sourceField: ["id"], destSchema: showCrew, destField: ["showId"] }),
  tasks: many({ sourceField: ["id"], destSchema: tasks, destField: ["showId"] }),
}));

const showCrewRelationships = relationships(showCrew, ({ one }) => ({
  show: one({ sourceField: ["showId"], destSchema: shows, destField: ["id"] }),
  crew: one({ sourceField: ["crewId"], destSchema: crew, destField: ["id"] }),
}));

const showInviteRelationships = relationships(showInvites, ({ one }) => ({
  show: one({ sourceField: ["showId"], destSchema: shows, destField: ["id"] }),
}));

const taskRelationships = relationships(tasks, ({ one }) => ({
  show: one({ sourceField: ["showId"], destSchema: shows, destField: ["id"] }),
}));

const commentRelationships = relationships(comments, ({ one }) => ({
  task: one({ sourceField: ["taskId"], destSchema: tasks, destField: ["id"] }),
  author: one({ sourceField: ["authorId"], destSchema: crew, destField: ["id"] }),
}));

const activityRelationships = relationships(activity, ({ one }) => ({
  show: one({ sourceField: ["showId"], destSchema: shows, destField: ["id"] }),
  actor: one({ sourceField: ["actorId"], destSchema: crew, destField: ["id"] }),
}));

export const schema = createSchema({
  tables: [crew, shows, showCrew, showInvites, tasks, comments, activity, checklistItems],
  relationships: [
    showRelationships,
    showCrewRelationships,
    showInviteRelationships,
    taskRelationships,
    commentRelationships,
    activityRelationships,
  ],
  enableLegacyQueries: false,
  enableLegacyMutators: false,
});

export const zql = createBuilder(schema);
export type Schema = typeof schema;

/** Who is asking: the anonymous account from the session cookie. */
export type AuthData = { userID: string };

declare module "@rocicorp/zero" {
  interface DefaultTypes {
    schema: typeof schema;
    context: AuthData | undefined;
  }
}

export type Crew = Row<typeof schema.tables.crew>;
export type Show = Row<typeof schema.tables.shows>;
export type ShowCrew = Row<typeof schema.tables.showCrew>;
export type Task = Row<typeof schema.tables.tasks>;
export type Comment = Row<typeof schema.tables.comments>;
export type Activity = Row<typeof schema.tables.activity>;
export type ChecklistItem = Row<typeof schema.tables.checklistItems>;

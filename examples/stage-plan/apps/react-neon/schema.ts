/**
 * StagePlan: a crew preparing shows. The Postgres tables are in
 * db/001_init.sql; these are their row types as live queries return them.
 */
export const TASK_STATUSES = ["todo", "doing", "done", "blocked"] as const;
export type TaskStatus = (typeof TASK_STATUSES)[number];

export const ACTIVITY_KINDS = ["created", "moved", "assigned", "renamed", "commented", "deleted"] as const;
export type ActivityKind = (typeof ACTIVITY_KINDS)[number];

export type Crew = { id: string; account: string; name: string };

export type Show = {
  id: string;
  name: string;
  venue: string;
  /** Local date of the show, as an ISO date (YYYY-MM-DD). */
  date: string;
  /** Local doors time at the venue (HH:MM). */
  doors: string;
  /** The crew chief: the account that created and manages the show. */
  chiefAccount: string;
  createdAt: number;
};

export type ShowCrew = { id: string; showId: string; crewId: string; account: string; role: "chief" | "crew" };
export type ShowInvite = { id: string; showId: string; code: string };

export type Task = {
  id: string;
  showId: string;
  title: string;
  status: TaskStatus;
  assigneeId: string | null;
  notes: string | null;
  /** Sort key inside a column. Lower values come first. */
  rank: number;
  /** The account that created the task: it may delete it. */
  createdBy: string;
  createdAt: number;
  updatedAt: number;
};

export type Comment = { id: string; taskId: string; authorId: string; body: string; createdAt: number };

export type Activity = {
  id: string;
  showId: string;
  taskId: string;
  actorId: string;
  kind: ActivityKind;
  /** Short human-readable detail, such as the column a task moved to. */
  detail: string | null;
  createdAt: number;
};

export type ChecklistItem = { id: string; title: string; done: boolean; ownerAccount: string };

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output c2a41c248e2e5dd8. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ActivityEvent = {
  readonly id: string;
  project_id: string;
  task_id: string | null;
  actor_id: string;
  kind: string;
  created_at: Date;
  payload: string;
};

export type ActivityEventInsert = {
  project_id: string;
  task_id?: string | null;
  actor_id: string;
  kind: string;
  created_at: Date;
  payload: string;
};

export type Membership = {
  readonly id: string;
  organization_id: string;
  user_id: string;
  role: string;
};

export type MembershipInsert = {
  organization_id: string;
  user_id: string;
  role: string;
};

export type Organization = {
  readonly id: string;
  name: string;
  created_at: Date;
};

export type OrganizationInsert = {
  name: string;
  created_at: Date;
};

export type Project = {
  readonly id: string;
  organization_id: string;
  name: string;
  archived: boolean;
  updated_at: Date;
};

export type ProjectInsert = {
  organization_id: string;
  name: string;
  archived: boolean;
  updated_at: Date;
};

export type TaskComment = {
  readonly id: string;
  task_id: string;
  author_id: string;
  body: string;
  created_at: Date;
};

export type TaskCommentInsert = {
  task_id: string;
  author_id: string;
  body: string;
  created_at: Date;
};

export type TaskWatcher = {
  readonly id: string;
  task_id: string;
  user_id: string;
};

export type TaskWatcherInsert = {
  task_id: string;
  user_id: string;
};

export type Task = {
  readonly id: string;
  project_id: string;
  title: string;
  status: string;
  priority: number;
  assignee_id: string;
  updated_at: Date;
  due_at: Date | null;
};

export type TaskInsert = {
  project_id: string;
  title: string;
  status: string;
  priority: number;
  assignee_id: string;
  updated_at: Date;
  due_at?: Date | null;
};

export type User = {
  readonly id: string;
  display_name: string;
  email: string;
};

export type UserInsert = {
  display_name: string;
  email: string;
};

interface Types extends AppTypes {
  activity_events: {
    row: ActivityEvent;
    insert: ActivityEventInsert;
    columns: {
      project_id: { kind: "uuid"; nullable: false; value: string };
      task_id: { kind: "uuid"; nullable: true; value: string };
      actor_id: { kind: "uuid"; nullable: false; value: string };
      kind: { kind: "string"; nullable: false; value: string };
      created_at: { kind: "timestamp"; nullable: false; value: Date };
      payload: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      actor: { one: "users"; nullable: false };
      project: { one: "projects"; nullable: false };
      task: { one: "tasks"; nullable: true };
    };
  };
  memberships: {
    row: Membership;
    insert: MembershipInsert;
    columns: {
      organization_id: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      organization: { one: "organizations"; nullable: false };
      user: { one: "users"; nullable: false };
    };
  };
  organizations: {
    row: Organization;
    insert: OrganizationInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      created_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      membershipsViaOrganization: { many: "memberships" };
      projectsViaOrganization: { many: "projects" };
    };
  };
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      organization_id: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      archived: { kind: "boolean"; nullable: false; value: boolean };
      updated_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      activity_eventsViaProject: { many: "activity_events" };
      organization: { one: "organizations"; nullable: false };
      tasksViaProject: { many: "tasks" };
    };
  };
  task_comments: {
    row: TaskComment;
    insert: TaskCommentInsert;
    columns: {
      task_id: { kind: "uuid"; nullable: false; value: string };
      author_id: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
      created_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      author: { one: "users"; nullable: false };
      task: { one: "tasks"; nullable: false };
    };
  };
  task_watchers: {
    row: TaskWatcher;
    insert: TaskWatcherInsert;
    columns: {
      task_id: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      task: { one: "tasks"; nullable: false };
      user: { one: "users"; nullable: false };
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      project_id: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      status: { kind: "string"; nullable: false; value: string };
      priority: { kind: "int"; nullable: false; value: number };
      assignee_id: { kind: "uuid"; nullable: false; value: string };
      updated_at: { kind: "timestamp"; nullable: false; value: Date };
      due_at: { kind: "timestamp"; nullable: true; value: Date };
    };
    relations: {
      activity_eventsViaTask: { many: "activity_events" };
      assignee: { one: "users"; nullable: false };
      project: { one: "projects"; nullable: false };
      task_commentsViaTask: { many: "task_comments" };
      task_watchersViaTask: { many: "task_watchers" };
    };
  };
  users: {
    row: User;
    insert: UserInsert;
    columns: {
      display_name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      activity_eventsViaActor: { many: "activity_events" };
      membershipsViaUser: { many: "memberships" };
      task_commentsViaAuthor: { many: "task_comments" };
      task_watchersViaUser: { many: "task_watchers" };
      tasksViaAssignee: { many: "tasks" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:ce82d5abd64c0cba85376e3f4f3a15aab7bac199e8c39501c20f018f725bd30b",
  "schema": {
    "current": "7f4dcf0e7741f87fc43cb77f4c31d6148c51adebf309a95ff766d66e83aa27bc",
    "schemas": [
      {
        "hash": "7f4dcf0e7741f87fc43cb77f4c31d6148c51adebf309a95ff766d66e83aa27bc",
        "schema": {
          "tables": {
            "activity_events": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "project_id",
                  "nullable": false,
                  "references": "projects",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "task_id",
                  "nullable": true,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "actor_id",
                  "nullable": false,
                  "references": "users",
                },
                { "column_type": { "type": "Text" }, "name": "kind", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "created_at", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "payload", "nullable": false },
              ],
            },
            "memberships": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organization_id",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "user_id",
                  "nullable": false,
                  "references": "users",
                },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "organizations": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "created_at", "nullable": false },
              ],
            },
            "projects": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organization_id",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "archived", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updated_at", "nullable": false },
              ],
            },
            "task_comments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "task_id",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "author_id",
                  "nullable": false,
                  "references": "users",
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "created_at", "nullable": false },
              ],
            },
            "task_watchers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "task_id",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "user_id",
                  "nullable": false,
                  "references": "users",
                },
              ],
            },
            "tasks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "project_id",
                  "nullable": false,
                  "references": "projects",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "priority", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "assignee_id",
                  "nullable": false,
                  "references": "users",
                },
                { "column_type": { "type": "Timestamp" }, "name": "updated_at", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "due_at", "nullable": true },
              ],
            },
            "users": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "display_name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "activity_events": {
      "actor": { "one": "users", "column": "actor_id" },
      "project": { "one": "projects", "column": "project_id" },
      "task": { "one": "tasks", "column": "task_id" },
    },
    "memberships": {
      "organization": { "one": "organizations", "column": "organization_id" },
      "user": { "one": "users", "column": "user_id" },
    },
    "organizations": {
      "membershipsViaOrganization": { "many": "memberships", "relation": "organization" },
      "projectsViaOrganization": { "many": "projects", "relation": "organization" },
    },
    "projects": {
      "activity_eventsViaProject": { "many": "activity_events", "relation": "project" },
      "organization": { "one": "organizations", "column": "organization_id" },
      "tasksViaProject": { "many": "tasks", "relation": "project" },
    },
    "task_comments": {
      "author": { "one": "users", "column": "author_id" },
      "task": { "one": "tasks", "column": "task_id" },
    },
    "task_watchers": {
      "task": { "one": "tasks", "column": "task_id" },
      "user": { "one": "users", "column": "user_id" },
    },
    "tasks": {
      "activity_eventsViaTask": { "many": "activity_events", "relation": "task" },
      "assignee": { "one": "users", "column": "assignee_id" },
      "project": { "one": "projects", "column": "project_id" },
      "task_commentsViaTask": { "many": "task_comments", "relation": "task" },
      "task_watchersViaTask": { "many": "task_watchers", "relation": "task" },
    },
    "users": {
      "activity_eventsViaActor": { "many": "activity_events", "relation": "actor" },
      "membershipsViaUser": { "many": "memberships", "relation": "user" },
      "task_commentsViaAuthor": { "many": "task_comments", "relation": "author" },
      "task_watchersViaUser": { "many": "task_watchers", "relation": "user" },
      "tasksViaAssignee": { "many": "tasks", "relation": "assignee" },
    },
  },
});

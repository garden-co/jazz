// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ActivityKind = "created" | "moved" | "assigned" | "renamed" | "commented" | "deleted";
export type ShowCrewRole = "chief" | "crew";
export type TaskStatus = "todo" | "doing" | "done" | "blocked";

export type Activity = {
  readonly id: string;
  showId: string;
  taskId: string;
  actorId: string;
  kind: ActivityKind;
  detail: string | null;
};

export type ActivityInsert = {
  showId: string;
  taskId: string;
  actorId: string;
  kind: ActivityKind;
  detail?: string | null;
};

export type ChecklistItem = {
  readonly id: string;
  title: string;
  done: boolean;
  ownerAccount: string;
};

export type ChecklistItemInsert = {
  title: string;
  done: boolean;
  ownerAccount: string;
};

export type Comment = {
  readonly id: string;
  taskId: string;
  authorId: string;
  body: string;
};

export type CommentInsert = {
  taskId: string;
  authorId: string;
  body: string;
};

export type Crew = {
  readonly id: string;
  account: string;
  name: string;
};

export type CrewInsert = {
  account: string;
  name: string;
};

export type ShowCrew = {
  readonly id: string;
  showId: string;
  crewId: string;
  account: string;
  role: ShowCrewRole;
  inviteCode: string | null;
};

export type ShowCrewInsert = {
  showId: string;
  crewId: string;
  account: string;
  role: ShowCrewRole;
  inviteCode?: string | null;
};

export type ShowInvite = {
  readonly id: string;
  showId: string;
  code: string;
};

export type ShowInviteInsert = {
  showId: string;
  code: string;
};

export type Show = {
  readonly id: string;
  name: string;
  venue: string;
  date: string;
  doors: string;
  chiefAccount: string;
};

export type ShowInsert = {
  name: string;
  venue: string;
  date: string;
  doors: string;
  chiefAccount: string;
};

export type Task = {
  readonly id: string;
  showId: string;
  title: string;
  status: TaskStatus;
  assigneeId: string | null;
  notes: string | null;
  rank: number;
};

export type TaskInsert = {
  showId: string;
  title: string;
  status: TaskStatus;
  assigneeId?: string | null;
  notes?: string | null;
  rank: number;
};

interface Types extends AppTypes {
  activity: {
    row: Activity;
    insert: ActivityInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      taskId: { kind: "uuid"; nullable: false; value: string };
      actorId: { kind: "uuid"; nullable: false; value: string };
      kind: { kind: "enum"; nullable: false; value: ActivityKind };
      detail: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      actor: { one: "crew"; nullable: false };
      show: { one: "shows"; nullable: false };
      task: { one: "tasks"; nullable: false };
    };
  };
  checklistItems: {
    row: ChecklistItem;
    insert: ChecklistItemInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {};
  };
  comments: {
    row: Comment;
    insert: CommentInsert;
    columns: {
      taskId: { kind: "uuid"; nullable: false; value: string };
      authorId: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      author: { one: "crew"; nullable: false };
      task: { one: "tasks"; nullable: false };
    };
  };
  crew: {
    row: Crew;
    insert: CrewInsert;
    columns: {
      account: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      assignedTasks: { many: "tasks" };
      memberships: { many: "showCrew" };
    };
  };
  showCrew: {
    row: ShowCrew;
    insert: ShowCrewInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      crewId: { kind: "uuid"; nullable: false; value: string };
      account: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: ShowCrewRole };
      inviteCode: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      crew: { one: "crew"; nullable: false };
      show: { one: "shows"; nullable: false };
    };
  };
  showInvites: {
    row: ShowInvite;
    insert: ShowInviteInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      show: { one: "shows"; nullable: false };
    };
  };
  shows: {
    row: Show;
    insert: ShowInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      venue: { kind: "string"; nullable: false; value: string };
      date: { kind: "string"; nullable: false; value: string };
      doors: { kind: "string"; nullable: false; value: string };
      chiefAccount: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      activity: { many: "activity" };
      crew: { many: "showCrew" };
      invites: { many: "showInvites" };
      tasks: { many: "tasks" };
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: TaskStatus };
      assigneeId: { kind: "uuid"; nullable: true; value: string };
      notes: { kind: "string"; nullable: true; value: string };
      rank: { kind: "float"; nullable: false; value: number };
    };
    relations: {
      activity: { many: "activity" };
      assignee: { one: "crew"; nullable: true };
      comments: { many: "comments" };
      show: { one: "shows"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:bf26855a278ef390c6bb85976bfd1253a6968a1bbfbab3134607314aab6118be",
  "schema": {
    "current": "eee271fbb284b62bb48730800515e17806d793d113c3d408e646fe68db0a8c8e",
    "schemas": [
      {
        "hash": "eee271fbb284b62bb48730800515e17806d793d113c3d408e646fe68db0a8c8e",
        "schema": {
          "tables": {
            "activity": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "actorId",
                  "nullable": false,
                  "references": "crew",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["created", "moved", "assigned", "renamed", "commented", "deleted"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "detail", "nullable": true },
              ],
            },
            "checklistItems": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
              ],
            },
            "comments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "authorId",
                  "nullable": false,
                  "references": "crew",
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
              ],
            },
            "crew": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
            "showCrew": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "crewId",
                  "nullable": false,
                  "references": "crew",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["chief", "crew"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "inviteCode", "nullable": true },
              ],
            },
            "showInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
              ],
            },
            "shows": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "venue", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "date", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "doors", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "chiefAccount", "nullable": false },
              ],
            },
            "tasks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["todo", "doing", "done", "blocked"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "assigneeId",
                  "nullable": true,
                  "references": "crew",
                },
                {
                  "column_type": { "type": "Text" },
                  "merge_strategy": "Text",
                  "name": "notes",
                  "nullable": true,
                },
                { "column_type": { "type": "Double" }, "name": "rank", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "activity": {
      "actor": { "one": "crew", "column": "actorId" },
      "show": { "one": "shows", "column": "showId" },
      "task": { "one": "tasks", "column": "taskId" },
    },
    "checklistItems": {},
    "comments": {
      "author": { "one": "crew", "column": "authorId" },
      "task": { "one": "tasks", "column": "taskId" },
    },
    "crew": {
      "assignedTasks": { "many": "tasks", "relation": "assignee" },
      "memberships": { "many": "showCrew", "relation": "crew" },
    },
    "showCrew": {
      "crew": { "one": "crew", "column": "crewId" },
      "show": { "one": "shows", "column": "showId" },
    },
    "showInvites": { "show": { "one": "shows", "column": "showId" } },
    "shows": {
      "activity": { "many": "activity", "relation": "show" },
      "crew": { "many": "showCrew", "relation": "show" },
      "invites": { "many": "showInvites", "relation": "show" },
      "tasks": { "many": "tasks", "relation": "show" },
    },
    "tasks": {
      "activity": { "many": "activity", "relation": "task" },
      "assignee": { "one": "crew", "column": "assigneeId" },
      "comments": { "many": "comments", "relation": "task" },
      "show": { "one": "shows", "column": "showId" },
    },
  },
});

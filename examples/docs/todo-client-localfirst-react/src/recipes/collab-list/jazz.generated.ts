// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 1fdb9945c327bef7. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ProjectMember = {
  readonly id: string;
  projectId: string;
  user_id: string;
};

export type ProjectMemberInsert = {
  projectId: string;
  user_id: string;
};

export type Project = {
  readonly id: string;
  name: string;
};

export type ProjectInsert = {
  name: string;
};

export type Task = {
  readonly id: string;
  title: string;
  done: boolean;
  assignee_id: string | null;
  projectId: string;
};

export type TaskInsert = {
  title: string;
  done: boolean;
  assignee_id?: string | null;
  projectId: string;
};

interface Types extends AppTypes {
  projectMembers: {
    row: ProjectMember;
    insert: ProjectMemberInsert;
    columns: {
      projectId: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      project: { one: "projects"; nullable: false };
    };
  };
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      members: { many: "projectMembers" };
      tasks: { many: "tasks" };
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      assignee_id: { kind: "uuid"; nullable: true; value: string };
      projectId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      project: { one: "projects"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:01426ff44f3a92d6f83656bb9957ae88d719261836ebfd06414170d0b886358e",
  "schema": {
    "current": "642613212c0e525458ac63f6796c1a123e41b255d42d2f980bcd0888dab37c79",
    "schemas": [
      {
        "hash": "642613212c0e525458ac63f6796c1a123e41b255d42d2f980bcd0888dab37c79",
        "schema": {
          "tables": {
            "projectMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "projectId",
                  "nullable": false,
                  "references": "projects",
                },
                { "column_type": { "type": "Uuid" }, "name": "user_id", "nullable": false },
              ],
            },
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "tasks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "assignee_id", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "projectId",
                  "nullable": false,
                  "references": "projects",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "projectMembers": { "project": { "one": "projects", "column": "projectId" } },
    "projects": {
      "members": { "many": "projectMembers", "relation": "project" },
      "tasks": { "many": "tasks", "relation": "project" },
    },
    "tasks": { "project": { "one": "projects", "column": "projectId" } },
  },
});

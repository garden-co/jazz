// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Comment = {
  readonly id: string;
  body: string;
  taskId: string;
};

export type CommentInsert = {
  body: string;
  taskId: string;
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
  projectId: string;
};

export type TaskInsert = {
  title: string;
  done: boolean;
  projectId: string;
};

interface Types extends AppTypes {
  comments: {
    row: Comment;
    insert: CommentInsert;
    columns: {
      body: { kind: "string"; nullable: false; value: string };
      taskId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      task: { one: "tasks"; nullable: false };
    };
  };
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      tasks: { many: "tasks" };
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      projectId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      comments: { many: "comments" };
      project: { one: "projects"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:10b675c646eca53008bc29491839342d975bedd6bd21e7a863c67626d44d6519",
  "schema": {
    "current": "7333a91cd1c947001385ececcf551358696ec77e21058f788322e1ce0734362d",
    "schemas": [
      {
        "hash": "7333a91cd1c947001385ececcf551358696ec77e21058f788322e1ce0734362d",
        "schema": {
          "tables": {
            "comments": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
              ],
            },
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "tasks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
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
    "comments": { "task": { "one": "tasks", "column": "taskId" } },
    "projects": { "tasks": { "many": "tasks", "relation": "project" } },
    "tasks": {
      "comments": { "many": "comments", "relation": "task" },
      "project": { "one": "projects", "column": "projectId" },
    },
  },
});

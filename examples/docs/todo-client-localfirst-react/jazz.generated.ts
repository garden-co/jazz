// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 14175a30cd3289c3. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Project = {
  readonly id: string;
  name: string;
};

export type ProjectInsert = {
  name: string;
};

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  description: string | null;
  parentId: string | null;
  projectId: string | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  description?: string | null;
  parentId?: string | null;
  projectId?: string | null;
};

interface Types extends AppTypes {
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      todos: { many: "todos" };
    };
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      description: { kind: "string"; nullable: true; value: string };
      parentId: { kind: "uuid"; nullable: true; value: string };
      projectId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      children: { many: "todos" };
      parent: { one: "todos"; nullable: true };
      project: { one: "projects"; nullable: true };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:423a5f173f145a444e7aeb06821a8853ac0424b1effba6a61dd425381270c9c9",
  "schema": {
    "current": "51d3cfb859fd89fa4ff3738a1f3b6c84a387c3a954f6fc1a69b5791e66b894a3",
    "schemas": [
      {
        "hash": "51d3cfb859fd89fa4ff3738a1f3b6c84a387c3a954f6fc1a69b5791e66b894a3",
        "schema": {
          "tables": {
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "description", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentId",
                  "nullable": true,
                  "references": "todos",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "projectId",
                  "nullable": true,
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
    "projects": { "todos": { "many": "todos", "relation": "project" } },
    "todos": {
      "children": { "many": "todos", "relation": "parent" },
      "parent": { "one": "todos", "column": "parentId" },
      "project": { "one": "projects", "column": "projectId" },
    },
  },
});

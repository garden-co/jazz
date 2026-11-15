// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
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
  owner_id: string;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  description?: string | null;
  parentId?: string | null;
  projectId?: string | null;
  owner_id: string;
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
      owner_id: { kind: "uuid"; nullable: false; value: string };
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
  "source": "sha256:89e4018182c3af3e948cbc82267dbeee3d4b9c5d734932a83f4e0d5334f36026",
  "schema": {
    "current": "47de365ee426116c99016cbf3ce31e22e568a18d72dd9c0d22ba1ce3a3c6bc62",
    "schemas": [
      {
        "hash": "47de365ee426116c99016cbf3ce31e22e568a18d72dd9c0d22ba1ce3a3c6bc62",
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
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
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

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
  priority: number | null;
  description: string | null;
  owner_id: string | null;
  parentId: string | null;
  projectId: string | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  priority?: number | null;
  description?: string | null;
  owner_id?: string | null;
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
      priority: { kind: "int"; nullable: true; value: number };
      description: { kind: "string"; nullable: true; value: string };
      owner_id: { kind: "uuid"; nullable: true; value: string };
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
  "source": "sha256:18303d719d2a7f858327c2fda624e98e435a2713bd7424f4816d916ec861e151",
  "schema": {
    "current": "ce91503ae647cad6ea4a0866d256fea3f92dd38d34df7ad6e2ac8c3bfb0c09e9",
    "schemas": [
      {
        "hash": "ce91503ae647cad6ea4a0866d256fea3f92dd38d34df7ad6e2ac8c3bfb0c09e9",
        "schema": {
          "tables": {
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "priority", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "description", "nullable": true },
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": true },
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

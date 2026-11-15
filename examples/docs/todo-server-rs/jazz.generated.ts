// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export interface Project {
  readonly id: string;
  name: string;
}

export interface ProjectInsert {
  name: string;
}

export interface Todo {
  readonly id: string;
  title: string;
  done: boolean;
  priority: number | null;
  description: string | null;
  parent: string | null;
  project: string | null;
}

export interface TodoInsert {
  title: string;
  done: boolean;
  priority?: number | null;
  description?: string | null;
  parent?: string | null;
  project?: string | null;
}

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
      parent: { kind: "uuid"; nullable: true; value: string };
      project: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      children: { many: "todos" };
      parentRelation: { one: "todos"; nullable: true };
      projectRelation: { one: "projects"; nullable: true };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:216aa1e42ad99c20ff335ba1d6525b15381752a751a374ba20b7b1fa1aa5e4c0",
  "schema": {
    "current": "f01997b29114f22266ddcbd328b3aba89b2e6ff16d60255068d6774dc4bd208f",
    "schemas": [
      {
        "hash": "f01997b29114f22266ddcbd328b3aba89b2e6ff16d60255068d6774dc4bd208f",
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
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parent",
                  "nullable": true,
                  "references": "todos",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "project",
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
    "projects": { "todos": { "many": "todos", "relation": "projectRelation" } },
    "todos": {
      "children": { "many": "todos", "relation": "parentRelation" },
      "parentRelation": { "one": "todos", "column": "parent" },
      "projectRelation": { "one": "projects", "column": "project" },
    },
  },
});

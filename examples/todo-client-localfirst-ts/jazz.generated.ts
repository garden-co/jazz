// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output f7debf81f012412f. Formatters may reformat this file, except `app` below.
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
  owner_id: string;
  parentId: string | null;
  projectId: string | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  description?: string | null;
  owner_id: string;
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
      todosViaProject: { many: "todos" };
    };
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      description: { kind: "string"; nullable: true; value: string };
      owner_id: { kind: "uuid"; nullable: false; value: string };
      parentId: { kind: "uuid"; nullable: true; value: string };
      projectId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      parent: { one: "todos"; nullable: true };
      project: { one: "projects"; nullable: true };
      todosViaParent: { many: "todos" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:a333087f8b8b72a54ba3a8c934c95abcf5333e51ed6998dd570c5a6ff1a23a4b",
  "schema": {
    "current": "86cb6a677db0e9bcf52e9907941801bcbf2cb3944c7a8c5ff6b257051c1bc104",
    "schemas": [
      {
        "hash": "86cb6a677db0e9bcf52e9907941801bcbf2cb3944c7a8c5ff6b257051c1bc104",
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
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
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
    "projects": { "todosViaProject": { "many": "todos", "relation": "project" } },
    "todos": {
      "parent": { "one": "todos", "column": "parentId" },
      "project": { "one": "projects", "column": "projectId" },
      "todosViaParent": { "many": "todos", "relation": "parent" },
    },
  },
});

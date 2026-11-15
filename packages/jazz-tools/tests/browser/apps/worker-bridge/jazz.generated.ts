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
  projectId: string | null;
  tags: string[] | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  projectId?: string | null;
  tags?: string[] | null;
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
      projectId: { kind: "uuid"; nullable: true; value: string };
      tags: { kind: "array"; nullable: true; value: string[] };
    };
    relations: {
      project: { one: "projects"; nullable: true };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:a8dcf51dd770113bdd8137762717219cfc55930c7b14f0fd49651a03ec3cd486",
  "schema": {
    "current": "a1ed1642adea71a3a73f83831391e1f058ea52405ac34922ce2ec53fee2c45b6",
    "schemas": [
      {
        "hash": "a1ed1642adea71a3a73f83831391e1f058ea52405ac34922ce2ec53fee2c45b6",
        "schema": {
          "tables": {
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "projectId",
                  "nullable": true,
                  "references": "projects",
                },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "name": "tags",
                  "nullable": true,
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
    "todos": { "project": { "one": "projects", "column": "projectId" } },
  },
});

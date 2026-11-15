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
};

export type TodoInsert = {
  title: string;
  done: boolean;
};

interface Types extends AppTypes {
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:3e6344df542d8d487cd5c204c45cd9c11173acf1ebd61404c6628383ed56d1e1",
  "schema": {
    "current": "4b8ea5e4e167746ea9e1cc57cd7def1de4d57c9aeefced2f3d3eea859d1881e2",
    "schemas": [
      {
        "hash": "4b8ea5e4e167746ea9e1cc57cd7def1de4d57c9aeefced2f3d3eea859d1881e2",
        "schema": {
          "tables": {
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
              ],
              "indexed_columns": ["title", "done"],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "projects": {}, "todos": {} },
});

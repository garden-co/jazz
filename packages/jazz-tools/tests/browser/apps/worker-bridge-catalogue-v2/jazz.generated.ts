// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 5f548ab791cee139. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  completed: boolean;
  description: string | null;
};

export type TodoInsert = {
  title: string;
  completed: boolean;
  description?: string | null;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      completed: { kind: "boolean"; nullable: false; value: boolean };
      description: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:2e7280cd127395520a6b7a8d5d93a25ef6b98c5235200427bdee6282e9185d5b",
  "schema": {
    "current": "21c1c8a77148c0671feb4959f2f26d598c29f03f53e2f9b21a2cb9372a2657f6",
    "schemas": [
      {
        "hash": "4e8374087b5d6e9a5c6680a2f8f6f73a2d6caa24235c8e4aa23814a066007ee7",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "completed", "nullable": false },
              ],
            },
          },
        },
      },
      {
        "hash": "21c1c8a77148c0671feb4959f2f26d598c29f03f53e2f9b21a2cb9372a2657f6",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "completed", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "description", "nullable": true },
              ],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "description",
                "column_type": { "type": "Text" },
                "type": "introduce",
                "value": { "type": "Null" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "todos",
          },
        ],
        "fromHash": "4e8374087b5d6e9a5c6680a2f8f6f73a2d6caa24235c8e4aa23814a066007ee7",
        "toHash": "21c1c8a77148c0671feb4959f2f26d598c29f03f53e2f9b21a2cb9372a2657f6",
      },
    ],
  },
  "relations": { "todos": {} },
});

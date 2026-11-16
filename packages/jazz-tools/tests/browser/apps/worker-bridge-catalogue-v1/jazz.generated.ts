// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output ec5fd85cd43887e6. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  completed: boolean;
};

export type TodoInsert = {
  title: string;
  completed: boolean;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      completed: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:214961f2a4e0471257e62b25e093827e5afd17c235f8b07f80938d252c59effc",
  "schema": {
    "current": "4e8374087b5d6e9a5c6680a2f8f6f73a2d6caa24235c8e4aa23814a066007ee7",
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
    ],
    "migrations": [],
  },
  "relations": { "todos": {} },
});

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 9de2e73aa28b3dfc. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
};

export type TodoInsert = {
  title: string;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:144677a416379a0d3d58467c82db65916ae75209a7ff04a06bd2fd32d49b4bb8",
  "schema": {
    "current": "a3ab34b90b3103893797af159b9489f21beb6e16ce9787154e5b821956469447",
    "schemas": [
      {
        "hash": "a3ab34b90b3103893797af159b9489f21beb6e16ce9787154e5b821956469447",
        "schema": {
          "tables": {
            "todos": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "title", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "todos": {} },
});

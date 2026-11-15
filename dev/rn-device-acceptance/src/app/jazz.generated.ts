// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
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
  "source": "sha256:a146db64e29275244cb8fc2b5b56e1d823f466fcad85d3046aa29f3cde55dcd0",
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

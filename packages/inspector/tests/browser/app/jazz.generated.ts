// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output b70bd15fcfc93c40. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

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
  "source": "sha256:fdeb638ae7874e924b41a056a60233af7dd92b387fb546ae6ef9fa24cd8d8f9a",
  "schema": {
    "current": "0a458c0de89c376d9c4ba504f853340705ffe05de9233494417f11c61ad26c3f",
    "schemas": [
      {
        "hash": "0a458c0de89c376d9c4ba504f853340705ffe05de9233494417f11c61ad26c3f",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
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

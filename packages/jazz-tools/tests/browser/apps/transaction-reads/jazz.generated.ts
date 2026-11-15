// jazz.generated.ts: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export interface Todo {
  readonly id: string;
  title: string;
  done: boolean;
}

export interface TodoInsert {
  title: string;
  done: boolean;
}

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
  "source": "sha256:2bbafbd548a491b8a09893b4d6dd307d449d446ce3ec7b56404673c9511b47e5",
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

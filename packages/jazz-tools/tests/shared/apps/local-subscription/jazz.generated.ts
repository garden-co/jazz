// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output ce350159c44751ae. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  rank: number | null;
  done: boolean;
};

export type TodoInsert = {
  title: string;
  rank?: number | null;
  done: boolean;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      rank: { kind: "int"; nullable: true; value: number };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:36e9cac77a4f3c5e43cc0ba2c0f445c8d6cbf49e2ec6c47212130dadbd1313e1",
  "schema": {
    "current": "75797e8dc95ccea36e7a73e49962d0d1e868bcc4b38ffb1fb51ee4cabbd60ac6",
    "schemas": [
      {
        "hash": "75797e8dc95ccea36e7a73e49962d0d1e868bcc4b38ffb1fb51ee4cabbd60ac6",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "rank", "nullable": true },
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

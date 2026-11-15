// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  list: string;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  list: string;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      list: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:85fed1aea6de2234fe0012dbc976013ad56cd5591d39896c3919f6baa69ec3e5",
  "schema": {
    "current": "805d4959a4c089ef2227cc0d2fffccf14f55106b41b19d0a4adc27dcbb82c3ae",
    "schemas": [
      {
        "hash": "805d4959a4c089ef2227cc0d2fffccf14f55106b41b19d0a4adc27dcbb82c3ae",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "list", "nullable": false },
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

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export interface Todo {
  readonly id: string;
  title: string;
  done: boolean;
  list: string;
}

export interface TodoInsert {
  title: string;
  done: boolean;
  list: string;
}

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
  "source": "sha256:38b53dd38d0feb318e47ce5393627ab7c407f81e77612193d0be219562f6c6d3",
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

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output c8f2d7125e860b9d. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean | null;
};

export type TodoInsert = {
  title: string;
  done?: boolean | null;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: true; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:bb5f24a159e4c0066c5ab16bcb5f93d14e9754598f064827c44de09b729f180e",
  "schema": {
    "current": "76a7530e17b54fc7c72de09cd934f65fbf22b6a6e7f3fc33cb028b7b2e38fd76",
    "schemas": [
      {
        "hash": "76a7530e17b54fc7c72de09cd934f65fbf22b6a6e7f3fc33cb028b7b2e38fd76",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": true },
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

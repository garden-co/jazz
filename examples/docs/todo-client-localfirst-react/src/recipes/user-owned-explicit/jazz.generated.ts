// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 7273efa095bb44ed. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  owner_id: string;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  owner_id: string;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      owner_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:f3066bb2d50183332d1e167773995d2a56f68d5e473610f3ffaa894e28403273",
  "schema": {
    "current": "9ae581bab1cecf5f0bc5011001fac10b7ceddbe3b0b12479360b4c5a684f76bf",
    "schemas": [
      {
        "hash": "9ae581bab1cecf5f0bc5011001fac10b7ceddbe3b0b12479360b4c5a684f76bf",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
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

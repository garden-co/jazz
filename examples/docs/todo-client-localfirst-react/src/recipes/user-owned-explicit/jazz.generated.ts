// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 6df329094c5a6990. Formatters may reformat this file, except `app` below.
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
  "source": "sha256:aaac9bdddf2f0afdf24a95327c75d072978351fc386847d8e13c3dacc2e55f50",
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

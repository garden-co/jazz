// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  description: string | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  description?: string | null;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      description: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:b47c97f1e8bda9bb91d7c13a2e491aeedd6022688ba0f3c805c7cc0c8d6e4355",
  "schema": {
    "current": "e0b1f37540eddb2b08be628f42cfebfd48c82903e4079527026a8a1c907ddb20",
    "schemas": [
      {
        "hash": "e0b1f37540eddb2b08be628f42cfebfd48c82903e4079527026a8a1c907ddb20",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "description", "nullable": true },
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

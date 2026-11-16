// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 506657952f631d0d. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type TodoShare = {
  readonly id: string;
  todoId: string;
  user_id: string;
  can_edit: boolean;
};

export type TodoShareInsert = {
  todoId: string;
  user_id: string;
  can_edit: boolean;
};

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
  todoShares: {
    row: TodoShare;
    insert: TodoShareInsert;
    columns: {
      todoId: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
      can_edit: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      todo: { one: "todos"; nullable: false };
    };
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      shares: { many: "todoShares" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:71541f32c87f65c732245111339820ece0aa345072849c7436cc06eff985d583",
  "schema": {
    "current": "9bd16fcb6f608b2b01c8837c88aefb827e4b3e24c34c132b18d1a83b52293591",
    "schemas": [
      {
        "hash": "9bd16fcb6f608b2b01c8837c88aefb827e4b3e24c34c132b18d1a83b52293591",
        "schema": {
          "tables": {
            "todoShares": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "todoId",
                  "nullable": false,
                  "references": "todos",
                },
                { "column_type": { "type": "Uuid" }, "name": "user_id", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "can_edit", "nullable": false },
              ],
            },
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
  "relations": {
    "todoShares": { "todo": { "one": "todos", "column": "todoId" } },
    "todos": { "shares": { "many": "todoShares", "relation": "todo" } },
  },
});

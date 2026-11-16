// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  list: string;
  priority: number;
  tags: string[];
  payload: Uint8Array | null;
  ownerId: string | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  list: string;
  priority: number;
  tags: string[];
  payload?: Uint8Array | null;
  ownerId?: string | null;
};

export type User = {
  readonly id: string;
  name: string;
};

export type UserInsert = {
  name: string;
};

interface Types extends AppTypes {
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      list: { kind: "string"; nullable: false; value: string };
      priority: { kind: "int"; nullable: false; value: number };
      tags: { kind: "array"; nullable: false; value: string[] };
      payload: { kind: "bytes"; nullable: true; value: Uint8Array };
      ownerId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      owner: { one: "users"; nullable: true };
    };
  };
  users: {
    row: User;
    insert: UserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      todosViaOwner: { many: "todos" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:00cdfe9795f4fcce54bbca96752bc25533b5c869ee329e8b563a65ebe31239cd",
  "schema": {
    "current": "0e68b3629e8e66406243ff9aca14ea38d8211b7793fed66231aa52e203ae2271",
    "schemas": [
      {
        "hash": "0e68b3629e8e66406243ff9aca14ea38d8211b7793fed66231aa52e203ae2271",
        "schema": {
          "tables": {
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "list", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "priority", "nullable": false },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "name": "tags",
                  "nullable": false,
                },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "ownerId",
                  "nullable": true,
                  "references": "users",
                },
              ],
            },
            "users": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "todos": { "owner": { "one": "users", "column": "ownerId" } },
    "users": { "todosViaOwner": { "many": "todos", "relation": "owner" } },
  },
});

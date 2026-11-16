// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output bdd7c08bb23d8cd3. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type CheckNote = {
  readonly id: string;
  body: string;
  user_check_id: string;
};

export type CheckNoteInsert = {
  body: string;
  user_check_id: string;
};

export type Org = {
  readonly id: string;
  name: string;
};

export type OrgInsert = {
  name: string;
};

export type Todo = {
  readonly id: string;
  title: string;
  org_id: string;
};

export type TodoInsert = {
  title: string;
  org_id: string;
};

export type UserCheck = {
  readonly id: string;
  todo_id: string;
};

export type UserCheckInsert = {
  todo_id: string;
};

interface Types extends AppTypes {
  check_notes: {
    row: CheckNote;
    insert: CheckNoteInsert;
    columns: {
      body: { kind: "string"; nullable: false; value: string };
      user_check_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      user_check: { one: "user_checks"; nullable: false };
    };
  };
  orgs: {
    row: Org;
    insert: OrgInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      todosViaOrg: { many: "todos" };
    };
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      org_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      org: { one: "orgs"; nullable: false };
      user_checksViaTodo: { many: "user_checks" };
    };
  };
  user_checks: {
    row: UserCheck;
    insert: UserCheckInsert;
    columns: {
      todo_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      check_notesViaUser_check: { many: "check_notes" };
      todo: { one: "todos"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:b4519071e9092715a0f3a477a0a4c434fdf9d98366b4649eef4321f3333ad7a5",
  "schema": {
    "current": "faa58b4f0a9f2d5ab75d1be412d4d699e9222871af78bc0aa6c8188da6dea40e",
    "schemas": [
      {
        "hash": "faa58b4f0a9f2d5ab75d1be412d4d699e9222871af78bc0aa6c8188da6dea40e",
        "schema": {
          "tables": {
            "check_notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "user_check_id",
                  "nullable": false,
                  "references": "user_checks",
                },
              ],
            },
            "orgs": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "org_id",
                  "nullable": false,
                  "references": "orgs",
                },
              ],
            },
            "user_checks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "todo_id",
                  "nullable": false,
                  "references": "todos",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "check_notes": { "user_check": { "one": "user_checks", "column": "user_check_id" } },
    "orgs": { "todosViaOrg": { "many": "todos", "relation": "org" } },
    "todos": {
      "org": { "one": "orgs", "column": "org_id" },
      "user_checksViaTodo": { "many": "user_checks", "relation": "todo" },
    },
    "user_checks": {
      "check_notesViaUser_check": { "many": "check_notes", "relation": "user_check" },
      "todo": { "one": "todos", "column": "todo_id" },
    },
  },
});

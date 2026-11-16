// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output da360c13a7ee3b5d. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type CheckNote = {
  readonly id: string;
  body: string;
  org_id: string;
  user_check_id: string;
};

export type CheckNoteInsert = {
  body: string;
  org_id: string;
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
  org_id: string;
  todo_id: string;
};

export type UserCheckInsert = {
  org_id: string;
  todo_id: string;
};

interface Types extends AppTypes {
  check_notes: {
    row: CheckNote;
    insert: CheckNoteInsert;
    columns: {
      body: { kind: "string"; nullable: false; value: string };
      org_id: { kind: "uuid"; nullable: false; value: string };
      user_check_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      org: { one: "orgs"; nullable: false };
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
      check_notesViaOrg: { many: "check_notes" };
      todosViaOrg: { many: "todos" };
      user_checksViaOrg: { many: "user_checks" };
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
      org_id: { kind: "uuid"; nullable: false; value: string };
      todo_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      check_notesViaUser_check: { many: "check_notes" };
      org: { one: "orgs"; nullable: false };
      todo: { one: "todos"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:4d094ff977f5a95199395a6dc0adc3d0146d3dede8cfaf4c5f9c49e1e0dc76e2",
  "schema": {
    "current": "72f5c30f5d999cfb24b1154fdbd4bca3bb5c85c4d561dedc7ac49f2e10737021",
    "schemas": [
      {
        "hash": "72f5c30f5d999cfb24b1154fdbd4bca3bb5c85c4d561dedc7ac49f2e10737021",
        "schema": {
          "tables": {
            "check_notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "org_id",
                  "nullable": false,
                  "references": "orgs",
                },
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
                  "name": "org_id",
                  "nullable": false,
                  "references": "orgs",
                },
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
    "check_notes": {
      "org": { "one": "orgs", "column": "org_id" },
      "user_check": { "one": "user_checks", "column": "user_check_id" },
    },
    "orgs": {
      "check_notesViaOrg": { "many": "check_notes", "relation": "org" },
      "todosViaOrg": { "many": "todos", "relation": "org" },
      "user_checksViaOrg": { "many": "user_checks", "relation": "org" },
    },
    "todos": {
      "org": { "one": "orgs", "column": "org_id" },
      "user_checksViaTodo": { "many": "user_checks", "relation": "todo" },
    },
    "user_checks": {
      "check_notesViaUser_check": { "many": "check_notes", "relation": "user_check" },
      "org": { "one": "orgs", "column": "org_id" },
      "todo": { "one": "todos", "column": "todo_id" },
    },
  },
});

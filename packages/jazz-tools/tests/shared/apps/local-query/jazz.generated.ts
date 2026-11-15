// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type BundleItem = {
  readonly id: string;
  label: string;
};

export type BundleItemInsert = {
  label: string;
};

export type Bundle = {
  readonly id: string;
  name: string;
  items: string[];
};

export type BundleInsert = {
  name: string;
  items: string[];
};

export type Org = {
  readonly id: string;
  name: string;
};

export type OrgInsert = {
  name: string;
};

export type Team = {
  readonly id: string;
  name: string;
  org_id: string | null;
  parent_id: string | null;
};

export type TeamInsert = {
  name: string;
  org_id?: string | null;
  parent_id?: string | null;
};

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  priority: number | null;
  owner_id: string | null;
  tags: string[];
  payload: Uint8Array | null;
};

export type TodoInsert = {
  title: string;
  done: boolean;
  priority?: number | null;
  owner_id?: string | null;
  tags: string[];
  payload?: Uint8Array | null;
};

export type User = {
  readonly id: string;
  name: string;
  team_id: string | null;
};

export type UserInsert = {
  name: string;
  team_id?: string | null;
};

interface Types extends AppTypes {
  bundle_items: {
    row: BundleItem;
    insert: BundleItemInsert;
    columns: {
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      bundlesViaItems: { many: "bundles" };
    };
  };
  bundles: {
    row: Bundle;
    insert: BundleInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      items: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      itemsRelation: { many: "bundle_items"; nullable: false };
    };
  };
  orgs: {
    row: Org;
    insert: OrgInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      teamsViaOrg: { many: "teams" };
    };
  };
  teams: {
    row: Team;
    insert: TeamInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      org_id: { kind: "uuid"; nullable: true; value: string };
      parent_id: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      org: { one: "orgs"; nullable: true };
      parent: { one: "teams"; nullable: true };
      teamsViaParent: { many: "teams" };
      usersViaTeam: { many: "users" };
    };
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      priority: { kind: "int"; nullable: true; value: number };
      owner_id: { kind: "uuid"; nullable: true; value: string };
      tags: { kind: "array"; nullable: false; value: string[] };
      payload: { kind: "bytes"; nullable: true; value: Uint8Array };
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
      team_id: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      team: { one: "teams"; nullable: true };
      todosViaOwner: { many: "todos" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:3265d131c388ef432b44c9164ef96e936738f0f32a7fed7048237a415920b552",
  "schema": {
    "current": "0b57b98e5f4f8c498df6d00f61db5e9e8f4be027550709257e3c3f7e9827bf0f",
    "schemas": [
      {
        "hash": "0b57b98e5f4f8c498df6d00f61db5e9e8f4be027550709257e3c3f7e9827bf0f",
        "schema": {
          "tables": {
            "bundle_items": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "label", "nullable": false }],
            },
            "bundles": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "items",
                  "nullable": false,
                  "references": "bundle_items",
                },
              ],
            },
            "orgs": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "teams": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "org_id",
                  "nullable": true,
                  "references": "orgs",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parent_id",
                  "nullable": true,
                  "references": "teams",
                },
              ],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "priority", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "owner_id",
                  "nullable": true,
                  "references": "users",
                },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "name": "tags",
                  "nullable": false,
                },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": true },
              ],
            },
            "users": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "team_id",
                  "nullable": true,
                  "references": "teams",
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
    "bundle_items": { "bundlesViaItems": { "many": "bundles", "relation": "itemsRelation" } },
    "bundles": { "itemsRelation": { "one": "bundle_items", "column": "items" } },
    "orgs": { "teamsViaOrg": { "many": "teams", "relation": "org" } },
    "teams": {
      "org": { "one": "orgs", "column": "org_id" },
      "parent": { "one": "teams", "column": "parent_id" },
      "teamsViaParent": { "many": "teams", "relation": "parent" },
      "usersViaTeam": { "many": "users", "relation": "team" },
    },
    "todos": { "owner": { "one": "users", "column": "owner_id" } },
    "users": {
      "team": { "one": "teams", "column": "team_id" },
      "todosViaOwner": { "many": "todos", "relation": "owner" },
    },
  },
});

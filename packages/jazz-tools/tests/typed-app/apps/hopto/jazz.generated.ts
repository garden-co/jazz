// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 077de2627fbdc6c7. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Org = {
  readonly id: string;
  name: string;
  slug: string;
};

export type OrgInsert = {
  name: string;
  slug: string;
};

export type Todo = {
  readonly id: string;
  title: string;
  completed: boolean;
  org: string;
};

export type TodoInsert = {
  title: string;
  completed: boolean;
  org: string;
};

interface Types extends AppTypes {
  org: {
    row: Org;
    insert: OrgInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      slug: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      todoViaOrg: { many: "todo" };
    };
  };
  todo: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      completed: { kind: "boolean"; nullable: false; value: boolean };
      org: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      orgRelation: { one: "org"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:c94441235cc05d9180019737cacf6c6ecb3e5fc3aebe30a7d4aa958605987cdf",
  "schema": {
    "current": "4ddbe68c51e53bd189d1bf701c243badd3fa817814d19c1216c3e5aa638c188d",
    "schemas": [
      {
        "hash": "4ddbe68c51e53bd189d1bf701c243badd3fa817814d19c1216c3e5aa638c188d",
        "schema": {
          "tables": {
            "org": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
              ],
            },
            "todo": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "completed", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "org",
                  "nullable": false,
                  "references": "org",
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
    "org": { "todoViaOrg": { "many": "todo", "relation": "orgRelation" } },
    "todo": { "orgRelation": { "one": "org", "column": "org" } },
  },
});

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output f62c080d5d6ef049. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type JsonValue } from "jazz-tools";

export type ParentState = "draft" | "open" | "closed";

export type Child = {
  readonly id: string;
  parentId: string;
  metadata: JsonValue;
};

export type ChildInsert = {
  parentId: string;
  metadata: JsonValue;
};

export type Parent = {
  readonly id: string;
  name: string;
  state: ParentState;
  summaryId: string | null;
};

export type ParentInsert = {
  name: string;
  state: ParentState;
  summaryId?: string | null;
};

export type Summary = {
  readonly id: string;
  metadata: JsonValue;
};

export type SummaryInsert = {
  metadata: JsonValue;
};

interface Types extends AppTypes {
  children: {
    row: Child;
    insert: ChildInsert;
    columns: {
      parentId: { kind: "uuid"; nullable: false; value: string };
      metadata: { kind: "json"; nullable: false; value: JsonValue };
    };
    relations: {
      parent: { one: "parents"; nullable: false };
    };
  };
  parents: {
    row: Parent;
    insert: ParentInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      state: { kind: "enum"; nullable: false; value: ParentState };
      summaryId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      childrenViaParent: { many: "children" };
      summary: { one: "summaries"; nullable: true };
    };
  };
  summaries: {
    row: Summary;
    insert: SummaryInsert;
    columns: {
      metadata: { kind: "json"; nullable: false; value: JsonValue };
    };
    relations: {
      parentsViaSummary: { many: "parents" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:a6fbe98a5d74d12769f105a75fb82fe56325ca928a0ec6edc250b9b3df45b41f",
  "schema": {
    "current": "d1cd726fd08afc4cc2850060ce9ddb4c1be358adb7f01706bccdce5eb97e4c3b",
    "schemas": [
      {
        "hash": "d1cd726fd08afc4cc2850060ce9ddb4c1be358adb7f01706bccdce5eb97e4c3b",
        "schema": {
          "tables": {
            "children": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentId",
                  "nullable": false,
                  "references": "parents",
                },
                { "column_type": { "type": "Json" }, "name": "metadata", "nullable": false },
              ],
            },
            "parents": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["draft", "open", "closed"] },
                  "name": "state",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "summaryId",
                  "nullable": true,
                  "references": "summaries",
                },
              ],
            },
            "summaries": {
              "columns": [
                { "column_type": { "type": "Json" }, "name": "metadata", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "children": { "parent": { "one": "parents", "column": "parentId" } },
    "parents": {
      "childrenViaParent": { "many": "children", "relation": "parent" },
      "summary": { "one": "summaries", "column": "summaryId" },
    },
    "summaries": { "parentsViaSummary": { "many": "parents", "relation": "summary" } },
  },
});

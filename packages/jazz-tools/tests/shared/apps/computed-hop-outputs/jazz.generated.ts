// jazz.generated.ts: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed";

export interface Source {
  readonly id: string;
  targetId: string;
  readonly rank: number;
  readonly title: string;
}

export interface SourceInsert {
  targetId: string;
}

export interface Target {
  readonly id: string;
  rank: number;
  title: string;
  readonly label: string;
}

export interface TargetInsert {
  rank: number;
  title: string;
}

interface Types extends AppTypes {
  sources: {
    row: Source;
    insert: SourceInsert;
    columns: {
      targetId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      target: { one: "targets"; nullable: false };
    };
    computed: {
      rank: { from: {}; value: number };
      title: { from: {}; value: string };
    };
  };
  targets: {
    row: Target;
    insert: TargetInsert;
    columns: {
      rank: { kind: "int"; nullable: false; value: number };
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      sources: { many: "sources" };
    };
    computed: {
      label: { from: { title: string }; value: string };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:be74667b1e60886c76ee7f11b74c9f3fb9843c8a2dcb972a4976882e50f3e5a8",
  "schema": {
    "current": "0bf4a382d74fa6c25b2bfd2c226e0133ad488ef4e590558fdb776ef7d3be5f4a",
    "schemas": [
      {
        "hash": "0bf4a382d74fa6c25b2bfd2c226e0133ad488ef4e590558fdb776ef7d3be5f4a",
        "schema": {
          "tables": {
            "sources": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "targetId",
                  "nullable": false,
                  "references": "targets",
                },
              ],
            },
            "targets": {
              "columns": [
                { "column_type": { "type": "Integer" }, "name": "rank", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "sources": { "rank": [], "title": [] }, "targets": { "label": ["title"] } },
  },
  "relations": {
    "sources": { "target": { "one": "targets", "column": "targetId" } },
    "targets": { "sources": { "many": "sources", "relation": "target" } },
  },
  computed,
});

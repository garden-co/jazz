// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 83962c842d8da1f2. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type Source = {
  readonly id: string;
  targetId: string;
  readonly rank: number;
};

export type SourceInsert = {
  targetId: string;
};

export type Target = {
  readonly id: string;
  rank: number;
  nextId: string | null;
  readonly doubled: number;
};

export type TargetInsert = {
  rank: number;
  nextId?: string | null;
};

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
    };
  };
  targets: {
    row: Target;
    insert: TargetInsert;
    columns: {
      rank: { kind: "int"; nullable: false; value: number };
      nextId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      next: { one: "targets"; nullable: true };
    };
    computed: {
      doubled: { from: { rank: number }; value: number };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:966d66d53c1f2fca534d2a83f6db1270823760f1a616f1e3c76ba4c6b4182d73",
  "schema": {
    "current": "0106c75a2c56786c810d4208f7f7693f4b59e79c39631ba0bf85f488425f2be0",
    "schemas": [
      {
        "hash": "0106c75a2c56786c810d4208f7f7693f4b59e79c39631ba0bf85f488425f2be0",
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
                {
                  "column_type": { "type": "Uuid" },
                  "name": "nextId",
                  "nullable": true,
                  "references": "targets",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "sources": { "rank": [] }, "targets": { "doubled": ["rank"] } },
  },
  "relations": {
    "sources": { "target": { "one": "targets", "column": "targetId" } },
    "targets": { "next": { "one": "targets", "column": "nextId" } },
  },
  computed,
});

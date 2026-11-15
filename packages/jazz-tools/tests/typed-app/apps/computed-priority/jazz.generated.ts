// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl, type TransformImpl } from "jazz-tools";
import { computed, transforms } from "./jazz.computed.js";

export type ItemStoredPriority = "low" | "medium" | "high";
export type ItemPriority = "low" | "medium" | "high";

export type Item = {
  readonly id: string;
  score: number;
  storedPriority: ItemStoredPriority;
  readonly priority: ItemPriority;
};

export type ItemInsert = {
  score: number;
  storedPriority: ItemStoredPriority;
};

interface Types extends AppTypes {
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      score: { kind: "int"; nullable: false; value: number };
      storedPriority: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
    computed: {
      priority: {
        from: { score: number; storedPriority: ItemStoredPriority };
        value: ItemPriority;
      };
    };
    transforms: {
      storedPriority: "priority";
    };
  };
}

interface Transforms {
  priority: { stored: number; view: "low" | "medium" | "high" };
}

export type AppComputed = ComputedImpl<Types>;
export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:e41af79fadba03e8bc69ad395d84d224213d8e8fec98e67c1cb6c4cd560e2d45",
  "schema": {
    "current": "0d427cf9885428994013a262a7e0db7940d51ae8c2d3b2a8b5412a120947163c",
    "schemas": [
      {
        "hash": "0d427cf9885428994013a262a7e0db7940d51ae8c2d3b2a8b5412a120947163c",
        "schema": {
          "tables": {
            "items": {
              "columns": [
                { "column_type": { "type": "Integer" }, "name": "score", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "storedPriority",
                  "nullable": false,
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "items": { "priority": ["score", "storedPriority"] } },
    "transforms": { "items": { "storedPriority": "priority" } },
  },
  "relations": { "items": {} },
  computed,
  transforms,
});

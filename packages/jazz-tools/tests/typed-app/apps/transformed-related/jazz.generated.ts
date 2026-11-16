// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output ec636649585827cb. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type TransformImpl } from "jazz-tools";
import { transforms } from "./jazz.computed.js";

export type Item = {
  readonly id: string;
  score: number;
  label: string;
};

export type ItemInsert = {
  score: number;
  label: string;
};

export type Parent = {
  readonly id: string;
  item: string | null;
  itemIds: string[];
};

export type ParentInsert = {
  item?: string | null;
  itemIds: string[];
};

interface Types extends AppTypes {
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      score: { kind: "int"; nullable: false; value: number };
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      parentsViaItems: { many: "parents" };
    };
    transforms: {
      score: "tenfold";
      label: "labelPrefix";
    };
  };
  parents: {
    row: Parent;
    insert: ParentInsert;
    columns: {
      item: { kind: "uuid"; nullable: true; value: string };
      itemIds: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      itemRelation: { one: "items"; nullable: true };
      items: { many: "items"; nullable: false };
    };
    transforms: {
      item: "itemRef";
    };
  };
}

interface Transforms {
  itemRef: { stored: string; view: string };
  labelPrefix: { stored: string; view: string };
  tenfold: { stored: number; view: number };
}

export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:b46960fb1c26f56af31d2b050d74077414cd6e5bf10aad652c61cb4a960476ea",
  "schema": {
    "current": "a52995e48667028860c14b4b905a958dd5ccdf37561cbe2195791762bf7fc37c",
    "schemas": [
      {
        "hash": "a52995e48667028860c14b4b905a958dd5ccdf37561cbe2195791762bf7fc37c",
        "schema": {
          "tables": {
            "items": {
              "columns": [
                { "column_type": { "type": "Integer" }, "name": "score", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
              ],
            },
            "parents": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "item",
                  "nullable": true,
                  "references": "items",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "itemIds",
                  "nullable": false,
                  "references": "items",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "transforms": {
      "items": { "score": "tenfold", "label": "labelPrefix" },
      "parents": { "item": "itemRef" },
    },
  },
  "relations": {
    "items": { "parentsViaItems": { "many": "parents", "relation": "items" } },
    "parents": {
      "itemRelation": { "one": "items", "column": "item" },
      "items": { "one": "items", "column": "itemIds" },
    },
  },
  transforms,
});

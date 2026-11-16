// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 4266b758b7aa9e4a. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Basket = {
  readonly id: string;
  itemId: string;
};

export type BasketInsert = {
  itemId: string;
};

export type Item = {
  readonly id: string;
  score: number;
  text: string;
};

export type ItemInsert = {
  score?: number;
  text?: string;
};

interface Types extends AppTypes {
  baskets: {
    row: Basket;
    insert: BasketInsert;
    columns: {
      itemId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      item: { one: "items"; nullable: false };
    };
  };
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      score: { kind: "int"; nullable: false; value: number };
      text: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:b6cb420d60173e83c967ad54520b85a6bfc6cee401f929a551461405ef88dd0e",
  "schema": {
    "current": "19472fa02fbe21f27013eae3df37f3a437645c3d532ad09c3dcfdadb1dde931c",
    "schemas": [
      {
        "hash": "19472fa02fbe21f27013eae3df37f3a437645c3d532ad09c3dcfdadb1dde931c",
        "schema": {
          "tables": {
            "baskets": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "itemId",
                  "nullable": false,
                  "references": "items",
                },
              ],
            },
            "items": {
              "columns": [
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 5 },
                  "name": "score",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Text" },
                  "default": { "type": "Text", "value": "hello" },
                  "name": "text",
                  "nullable": false,
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "baskets": { "item": { "one": "items", "column": "itemId" } }, "items": {} },
});

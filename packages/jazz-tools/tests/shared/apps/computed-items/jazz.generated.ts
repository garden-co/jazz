// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output e837067d06ae39be. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type ItemPriority = "low" | "high";

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
  readonly priority: ItemPriority;
  readonly description: string;
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
    computed: {
      priority: { from: { score: number }; value: ItemPriority };
      description: { from: { id: string; text: string }; value: string };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:d7fdafc5e3f0193fe1ecfe37ae20043349637814c2b3c2019b113d6ed635ec1a",
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
    "computed": { "items": { "priority": ["score"], "description": ["id", "text"] } },
  },
  "relations": { "baskets": { "item": { "one": "items", "column": "itemId" } }, "items": {} },
  computed,
});

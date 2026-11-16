// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output b58e457425e6e31d. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type Row = {
  readonly id: string;
  score: number;
  text: string;
  readonly toString: string;
  readonly valueOf: string;
  readonly tag: string;
  readonly original: boolean;
};

export type RowInsert = {
  score?: number;
  text?: string;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      score: { kind: "int"; nullable: false; value: number };
      text: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
    computed: {
      toString: { from: {}; value: string };
      valueOf: { from: {}; value: string };
      tag: { from: { id: string; score: number; text: string }; value: string };
      original: { from: { id: string; score: number; text: string }; value: boolean };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:fab3499dbf43c4ac485bebc8d0f5be44de2ee5256fa92aa16656a3ab6b68098e",
  "schema": {
    "current": "ecaaf702a031bb75dd374854fcb35120ff3157b2f157f8747f54319c0089cb62",
    "schemas": [
      {
        "hash": "ecaaf702a031bb75dd374854fcb35120ff3157b2f157f8747f54319c0089cb62",
        "schema": {
          "tables": {
            "rows": {
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
    "computed": {
      "rows": {
        "toString": [],
        "valueOf": [],
        "tag": ["id", "score", "text"],
        "original": ["id", "score", "text"],
      },
    },
  },
  "relations": { "rows": {} },
  computed,
});

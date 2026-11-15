// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl, type JsonValue } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type Row = {
  readonly id: string;
  score: number;
  text: string;
  readonly wrapper: JsonValue;
  readonly mapping: JsonValue;
  readonly membership: JsonValue;
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
      wrapper: { from: { id: string; score: number; text: string }; value: JsonValue };
      mapping: { from: { id: string; score: number; text: string }; value: JsonValue };
      membership: { from: { id: string; score: number; text: string }; value: JsonValue };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:f9be6268ec3d8e39810f08ce388fbc9d55e32e0cc8a3c34131876b015223c568",
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
        "wrapper": ["id", "score", "text"],
        "mapping": ["id", "score", "text"],
        "membership": ["id", "score", "text"],
      },
    },
  },
  "relations": { "rows": {} },
  computed,
});

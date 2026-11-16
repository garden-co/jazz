// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output d3c14884109f1d5e. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Row = {
  readonly id: string;
  score: number;
  text: string;
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
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:053f0387ca32bd4ce1a40cc79627d1cfe8ce0573aacbc4500c63512cdce85300",
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
  },
  "relations": { "rows": {} },
});

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type Row = {
  readonly id: string;
  constructor: string;
  toString: string;
  valueOf: string;
  readonly computedValue: string;
};

export type RowInsert = {
  constructor: string;
  toString: string;
  valueOf: string;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      constructor: { kind: "string"; nullable: false; value: string };
      toString: { kind: "string"; nullable: false; value: string };
      valueOf: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
    computed: {
      computedValue: { from: { toString: string }; value: string };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:c5a9981c379b7dedf4dda3979d9a61398cd747dd76558a8e6cf8fca6fe8c0899",
  "schema": {
    "current": "f9edce026f5ac965b4e160c41413f76dded3c268e406ea4896dd5d0189528a70",
    "schemas": [
      {
        "hash": "f9edce026f5ac965b4e160c41413f76dded3c268e406ea4896dd5d0189528a70",
        "schema": {
          "tables": {
            "rows": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "constructor", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "toString", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "valueOf", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "rows": { "computedValue": ["toString"] } },
  },
  "relations": { "rows": {} },
  computed,
});

// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 5bca011e98560708. Formatters may reformat this file, except `app` below.
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
  "source": "sha256:09ca6df6396fd5d69a7f5ac7697febe2ea0cd55d55894eb4f4ff424ff1c85472",
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

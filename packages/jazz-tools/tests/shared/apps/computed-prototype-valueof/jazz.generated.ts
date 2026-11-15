// jazz.generated.ts: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed";

export interface Row {
  readonly id: string;
  constructor: string;
  toString: string;
  valueOf: string;
  readonly computedValue: string;
}

export interface RowInsert {
  constructor: string;
  toString: string;
  valueOf: string;
}

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
      computedValue: { from: { valueOf: string }; value: string };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:ae95a808610d9e9721e6c55b1af2e08d16a054999cbf04e4e1cfe234c0fc9e39",
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
    "computed": { "rows": { "computedValue": ["valueOf"] } },
  },
  "relations": { "rows": {} },
  computed,
});

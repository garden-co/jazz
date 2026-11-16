// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 0a441ed6d3377cbe. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type Row = {
  readonly id: string;
  useLeft: boolean;
  left: number;
  right: number;
  readonly value: number;
};

export type RowInsert = {
  useLeft: boolean;
  left: number;
  right: number;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      useLeft: { kind: "boolean"; nullable: false; value: boolean };
      left: { kind: "int"; nullable: false; value: number };
      right: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
    computed: {
      value: { from: { useLeft: boolean; left: number; right: number }; value: number };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:da007cc52586351ca5bc021a266a15f171ef522ce29388888da774f35cd1fe69",
  "schema": {
    "current": "f503a38f7e8a1c10b006e158e225b32000303667e4cfe407b5bd290c356e89a0",
    "schemas": [
      {
        "hash": "f503a38f7e8a1c10b006e158e225b32000303667e4cfe407b5bd290c356e89a0",
        "schema": {
          "tables": {
            "rows": {
              "columns": [
                { "column_type": { "type": "Boolean" }, "name": "useLeft", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "left", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "right", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "rows": { "value": ["useLeft", "left", "right"] } },
  },
  "relations": { "rows": {} },
  computed,
});

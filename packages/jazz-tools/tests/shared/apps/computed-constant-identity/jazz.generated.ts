// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 6eb775d253137b9d. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type Row = {
  readonly id: string;
  text: string;
  readonly constant: number;
  readonly identity: string;
};

export type RowInsert = {
  text: string;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      text: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
    computed: {
      constant: { from: {}; value: number };
      identity: { from: { id: string }; value: string };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:fffe6f3f9d28d33da11a231871e072c5a394224e4b676aea2ed3b976a937a611",
  "schema": {
    "current": "31c160318a9bda8d3979295142bbd2a488ac13256630062a937da084831e9198",
    "schemas": [
      {
        "hash": "31c160318a9bda8d3979295142bbd2a488ac13256630062a937da084831e9198",
        "schema": {
          "tables": {
            "rows": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "text", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "rows": { "constant": [], "identity": ["id"] } },
  },
  "relations": { "rows": {} },
  computed,
});

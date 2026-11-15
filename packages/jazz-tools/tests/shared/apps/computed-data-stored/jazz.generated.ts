// jazz.generated.ts: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export interface Row {
  readonly id: string;
  data: string;
}

export interface RowInsert {
  data: string;
}

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      data: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:a9759e0a72374cc613459770ac76e1c7312142726ead0416f77eef7735836840",
  "schema": {
    "current": "97730bad8c4c1a6e1114aee523d76efc7bd3537c4f8d552a89aa5dc59a290c35",
    "schemas": [
      {
        "hash": "97730bad8c4c1a6e1114aee523d76efc7bd3537c4f8d552a89aa5dc59a290c35",
        "schema": {
          "tables": {
            "rows": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "data", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "rows": {} },
});

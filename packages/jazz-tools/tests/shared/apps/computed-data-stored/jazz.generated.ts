// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Row = {
  readonly id: string;
  data: string;
};

export type RowInsert = {
  data: string;
};

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
  "source": "sha256:d0386675acd26335504b4245cf4b639bc526811f5119a6e857b5c3963ddc2143",
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
